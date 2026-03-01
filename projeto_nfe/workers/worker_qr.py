"""
worker_qr.py
============
Worker de leitura de QR Code.

Loop infinito:
  1. BLPOP queue:qr_parse → image_id
  2. Atualiza qr_status = 'processing'
  3. Decodifica QR Code com QReader + cv2
  4. Valida URL (consultadfe.fazenda.rj.gov.br)
  5. Sucesso  → grava qr_text/qr_url, publica em queue:scrape, notifica usuário
  6. Falha    → grava qr_status='error', notifica usuário
"""

import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

import cv2
import db
import httpx
import redis.asyncio as aioredis
from dotenv import load_dotenv
from qreader import QReader

# ---------------------------------------------------------------------------
# Ambiente e logging
# ---------------------------------------------------------------------------
load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
IMAGES_DIR = Path(os.getenv("IMAGES_DIR", "/app/received_images"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("worker_qr")

# URL válida da NF-e do RJ
_VALID_URL_PREFIX = "https://consultadfe.fazenda.rj.gov.br/"

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
                log.warning("Telegram API retornou %d: %s", resp.status_code, resp.text)
    except Exception as exc:
        log.error("Erro ao enviar notificação Telegram: %s", exc)


def _escape(text: str) -> str:
    special = r"\_*[]()~`>#+-=|{}.!"
    for ch in special:
        text = text.replace(ch, f"\\{ch}")
    return text


# ---------------------------------------------------------------------------
# Leitura do QR Code
# ---------------------------------------------------------------------------


def decode_qr(image_path: Path) -> str | None:
    """
    Tenta decodificar o QR Code da imagem usando QReader + cv2.
    Retorna o texto do QR ou None.
    """
    try:
        image = cv2.imread(str(image_path))
        if image is None:
            log.error("cv2 não conseguiu abrir: %s", image_path)
            return None
        qr = QReader()
        decoded = qr.detect_and_decode(image=image)
        texts = [t for t in decoded if t]
        if texts:
            log.info("QR decodificado: %r", texts[0])
            return texts[0]
        log.warning("Nenhum QR Code encontrado em: %s", image_path)
        return None
    except Exception as exc:
        log.error("Erro ao decodificar QR: %s", exc, exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Processamento de uma imagem
# ---------------------------------------------------------------------------


async def process_image(image_id: int, redis_client: aioredis.Redis) -> None:
    log.info("Processando image_id=%d", image_id)

    # Carrega registro do banco
    record = await db.get_image_by_id(image_id)
    if not record:
        log.error("image_id=%d não encontrado no banco.", image_id)
        return

    chat_id = record["chat_id"]
    message_id = record["message_id"]
    file_path = Path(record["file_path"])
    user_id = record["telegram_user_id"]

    # ── qr_status = processing ──────────────────────────────────────────
    await db.set_qr_processing(image_id)

    # ── Decodifica QR ────────────────────────────────────────────────────
    qr_text = decode_qr(file_path)

    if not qr_text:
        log.warning("image_id=%d — QR Code não decodificado.", image_id)
        await db.set_qr_error(image_id)
        await telegram_reply(
            chat_id=chat_id,
            reply_to_message_id=message_id,
            text=(
                "❌ *QR Code não encontrado ou ilegível\\.*\n\n"
                "Verifique se o QR Code está visível na foto e tente novamente\\."
            ),
        )
        return

    # ── Valida URL ───────────────────────────────────────────────────────
    if not qr_text.startswith(_VALID_URL_PREFIX):
        log.warning(
            "image_id=%d — URL inválida: %r (não começa com %s)",
            image_id,
            qr_text,
            _VALID_URL_PREFIX,
        )
        await db.set_qr_error(image_id)
        await telegram_reply(
            chat_id=chat_id,
            reply_to_message_id=message_id,
            text=(
                "❌ *QR Code lido, mas URL inválida\\.*\n\n"
                f"O QR Code não aponta para a Fazenda do RJ\\.\n"
                f"Texto encontrado: `{_escape(qr_text[:120])}`"
            ),
        )
        return

    # ── Sucesso ──────────────────────────────────────────────────────────
    await db.set_qr_success(image_id, qr_text=qr_text, qr_url=qr_text)

    # Publica em queue:scrape
    await redis_client.rpush("queue:scrape", str(image_id))
    log.info("image_id=%d publicado em queue:scrape", image_id)

    # Notifica usuário
    url_escaped = _escape(qr_text)
    await telegram_reply(
        chat_id=chat_id,
        reply_to_message_id=message_id,
        text=(
            "✅ *QR Code lido com sucesso\\!*\n\n"
            f"🔗 URL da nota:\n`{url_escaped}`\n\n"
            "⏳ _Consultando dados na Fazenda\\.\\.\\._"
        ),
    )

    log.info(
        "image_id=%d user_id=%d qr_status=success url=%s",
        image_id,
        user_id,
        qr_text,
    )


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------

_running = True


def _handle_sigterm(signum, frame):
    global _running
    log.info("SIGTERM recebido — encerrando worker_qr...")
    _running = False


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


async def main() -> None:
    log.info("=" * 50)
    log.info("worker_qr iniciando...")
    log.info("IMAGES_DIR: %s", IMAGES_DIR)
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
                # BLPOP bloqueia até 5 s para permitir checagem de _running
                result = await redis_client.blpop("queue:qr_parse", timeout=5)
                if result is None:
                    continue

                _, raw_id = result
                image_id = int(raw_id)
                log.info("Recebido da fila: image_id=%d", image_id)

                try:
                    await process_image(image_id, redis_client)
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
                log.error("Erro no loop do worker_qr: %s", exc, exc_info=True)
                await asyncio.sleep(3)  # Evita loop de erro acelerado

    finally:
        await redis_client.aclose()
        await db.close_pool()
        log.info("worker_qr encerrado.")


if __name__ == "__main__":
    asyncio.run(main())
