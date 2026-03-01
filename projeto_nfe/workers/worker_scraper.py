"""
worker_scraper.py
=================
Worker de scraping + parse + inserção Silver.

Loop infinito:
  1. BLPOP queue:scrape → image_id
  2. Atualiza scrape_status = 'processing'
  3. Notifica usuário que o scraping iniciou
  4. Chama fetch_nfe_html(qr_url) — Firefox headless via Playwright
  5. Salva HTML em scraped_html/{image_id}_{timestamp}.html
  6. Chama parse_nfe_html(html) → insere em silver.nfe_headers + silver.nfe_items
  7. Notifica usuário com resumo formatado
  8. Erros em qualquer etapa → atualiza status + notifica usuário
"""

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime
from pathlib import Path

import db
import httpx
import redis.asyncio as aioredis
from dotenv import load_dotenv
from nfe_parser import parse_nfe_html
from nfe_scraper import fetch_nfe_html

# ---------------------------------------------------------------------------
# Ambiente e logging
# ---------------------------------------------------------------------------
load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
SCRAPED_HTML_DIR = Path(os.getenv("SCRAPED_HTML_DIR", "/app/scraped_html"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("worker_scraper")

# ---------------------------------------------------------------------------
# Telegram HTTP helper
# ---------------------------------------------------------------------------


async def telegram_reply(
    chat_id: int,
    reply_to_message_id: int,
    text: str,
) -> None:
    """Envia mensagem via API HTTP do Telegram (sem event loop do bot)."""
    if not BOT_TOKEN:
        log.warning("BOT_TOKEN não definido — notificação não enviada.")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "reply_to_message_id": reply_to_message_id,
        "text": text,
        "parse_mode": "MarkdownV2",
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code != 200:
                log.warning("Telegram retornou %d: %s", resp.status_code, resp.text)
    except Exception as exc:
        log.error("Erro ao enviar notificação Telegram: %s", exc)


def _escape(text: str) -> str:
    special = r"\_*[]()~`>#+-=|{}.!"
    for ch in special:
        text = text.replace(ch, f"\\{ch}")
    return text


# ---------------------------------------------------------------------------
# Formatação do resumo da nota
# ---------------------------------------------------------------------------


def _format_summary(header: dict, total_itens: int, valor_total: float | None) -> str:
    """Formata o resumo da NF-e em MarkdownV2 para enviar ao usuário."""
    estab = _escape(header.get("estabelecimento") or "Não identificado")
    cnpj = _escape(header.get("cnpj") or "")
    endereco = _escape(header.get("endereco") or "")
    valor_str = (
        _escape(
            f"R$ {valor_total:,.2f}".replace(",", "X")
            .replace(".", ",")
            .replace("X", ".")
        )
        if valor_total
        else "N/A"
    )

    lines = [
        "✅ *Nota Fiscal processada com sucesso\\!*\n",
        f"🏪 *Estabelecimento:* {estab}",
    ]
    if cnpj:
        lines.append(f"🔢 *CNPJ:* `{cnpj}`")
    if endereco:
        lines.append(f"📍 *Endereço:* {endereco}")
    lines += [
        f"📦 *Total de itens:* {total_itens}",
        f"💰 *Valor total:* {valor_str}",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Processamento de uma imagem
# ---------------------------------------------------------------------------


async def process_image(image_id: int) -> None:
    log.info("Processando image_id=%d", image_id)

    record = await db.get_image_by_id(image_id)
    if not record:
        log.error("image_id=%d não encontrado no banco.", image_id)
        return

    chat_id = record["chat_id"]
    message_id = record["message_id"]
    qr_url = record.get("qr_url")
    user_id = record["telegram_user_id"]

    if not qr_url:
        log.error("image_id=%d sem qr_url no banco.", image_id)
        return

    # ── scrape_status = processing ──────────────────────────────────────
    await db.set_scrape_processing(image_id)

    # ── Notifica início do scraping ──────────────────────────────────────
    await telegram_reply(
        chat_id=chat_id,
        reply_to_message_id=message_id,
        text="🔍 *Consultando NF\\-e na Fazenda\\.\\.\\.*\n\n⏳ _Isso pode levar até 1 minuto\\._",
    )

    # ── Scraping com Firefox headless ────────────────────────────────────
    # fetch_nfe_html é síncrono (usa Playwright sync API); roda numa thread
    loop = asyncio.get_event_loop()
    try:
        html = await loop.run_in_executor(None, fetch_nfe_html, qr_url)
    except Exception as exc:
        log.error(
            "Exceção no fetch_nfe_html para image_id=%d: %s",
            image_id,
            exc,
            exc_info=True,
        )
        html = None

    if not html:
        log.warning("image_id=%d — fetch_nfe_html retornou None.", image_id)
        await db.set_scrape_error(image_id)
        await telegram_reply(
            chat_id=chat_id,
            reply_to_message_id=message_id,
            text=(
                "❌ *Não foi possível consultar a NF\\-e\\.*\n\n"
                "O site da Fazenda pode estar indisponível\\. "
                "Tente novamente em alguns minutos\\."
            ),
        )
        return

    # ── Persiste HTML ────────────────────────────────────────────────────
    SCRAPED_HTML_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_name = f"{image_id}_{ts}.html"
    html_path = SCRAPED_HTML_DIR / html_name

    try:
        html_path.write_text(html, encoding="utf-8")
        log.info("HTML salvo: %s (%d bytes)", html_path, len(html))
    except Exception as exc:
        log.error("Erro ao salvar HTML: %s", exc, exc_info=True)
        # Continua — o parse ainda pode funcionar sem salvar

    # ── Parse NF-e ───────────────────────────────────────────────────────
    try:
        parsed = parse_nfe_html(html)
    except Exception as exc:
        log.error("Erro no parse NF-e image_id=%d: %s", image_id, exc, exc_info=True)
        await db.set_scrape_error(image_id)
        await telegram_reply(
            chat_id=chat_id,
            reply_to_message_id=message_id,
            text=(
                "❌ *Erro ao extrair os dados da NF\\-e\\.*\n\n"
                "O HTML foi obtido mas não foi possível interpretar os dados\\."
            ),
        )
        return

    if not parsed["ok"]:
        log.warning("image_id=%d — parse retornou 0 itens.", image_id)
        await db.set_scrape_error(image_id)
        await telegram_reply(
            chat_id=chat_id,
            reply_to_message_id=message_id,
            text=(
                "⚠️ *HTML obtido, mas nenhum item de produto encontrado\\.*\n\n"
                "Verifique se a nota foi emitida corretamente\\."
            ),
        )
        return

    # ── Inserção Silver ──────────────────────────────────────────────────
    try:
        await db.insert_nfe_silver(
            image_id=image_id,
            header=parsed["header"],
            items=parsed["items"],
            raw_html_path=str(html_path),
        )
    except Exception as exc:
        log.error(
            "Erro ao inserir Silver image_id=%d: %s", image_id, exc, exc_info=True
        )
        # Não interrompe — ainda atualiza status e notifica

    # ── Atualiza status sucesso ──────────────────────────────────────────
    await db.set_scrape_success(image_id, str(html_path))

    # ── Notifica usuário com resumo ──────────────────────────────────────
    summary = _format_summary(
        header=parsed["header"],
        total_itens=parsed["total_itens"],
        valor_total=parsed["valor_total"],
    )
    await telegram_reply(
        chat_id=chat_id,
        reply_to_message_id=message_id,
        text=summary,
    )

    log.info(
        "image_id=%d user_id=%d scrape_status=success itens=%d valor=%.2f",
        image_id,
        user_id,
        parsed["total_itens"],
        parsed["valor_total"] or 0,
    )


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------

_running = True


def _handle_sigterm(signum, frame):
    global _running
    log.info("SIGTERM recebido — encerrando worker_scraper...")
    _running = False


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


async def main() -> None:
    log.info("=" * 50)
    log.info("worker_scraper iniciando...")
    log.info("SCRAPED_HTML_DIR: %s", SCRAPED_HTML_DIR)
    log.info("=" * 50)

    await db.get_pool()

    redis_client = aioredis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
    )

    try:
        while _running:
            try:
                result = await redis_client.blpop("queue:scrape", timeout=5)
                if result is None:
                    continue

                _, raw_id = result
                image_id = int(raw_id)
                log.info("Recebido da fila: image_id=%d", image_id)

                try:
                    await process_image(image_id)
                except Exception as exc:
                    log.error(
                        "Erro não tratado ao processar image_id=%d: %s",
                        image_id,
                        exc,
                        exc_info=True,
                    )

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error("Erro no loop worker_scraper: %s", exc, exc_info=True)
                await asyncio.sleep(5)

    finally:
        await redis_client.aclose()
        await db.close_pool()
        log.info("worker_scraper encerrado.")


if __name__ == "__main__":
    asyncio.run(main())
