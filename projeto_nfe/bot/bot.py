"""
bot.py  (bot)
=============
Entry point do bot Telegram para leitura de NF-e via QR Code.

Fluxo:
  1. Carrega variáveis de ambiente do .env
  2. Configura logging estruturado
  3. Inicializa pool Postgres
  4. Registra handlers
  5. Inicia polling (bloqueante)
  6. Fecha pool ao encerrar

Uso:
    python bot.py
"""

import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

import db
import handlers
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters

# ---------------------------------------------------------------------------
# Ambiente
# ---------------------------------------------------------------------------
load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
IMAGES_DIR = os.getenv("IMAGES_DIR", "/app/received_images")

if not TOKEN:
    print(
        "ERRO: Variável TELEGRAM_BOT_TOKEN não definida.\n"
        "Copie .env.example para .env e preencha o token do BotFather."
    )
    sys.exit(1)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/bot.log", encoding="utf-8"),
    ],
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("asyncpg").setLevel(logging.WARNING)

log = logging.getLogger("bot")


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------


def build_application() -> Application:
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", handlers.handle_start))

    app.add_handler(MessageHandler(filters.PHOTO, handlers.handle_photo))
    app.add_handler(MessageHandler(filters.Document.IMAGE, handlers.handle_photo))

    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handlers.handle_text_message)
    )

    app.add_handler(MessageHandler(filters.COMMAND, handlers.handle_unknown))

    return app


# ---------------------------------------------------------------------------
# Startup / Shutdown hooks
# ---------------------------------------------------------------------------


async def on_startup(app: Application) -> None:
    log.info("Inicializando pool Postgres...")
    await db.get_pool()
    log.info("Pool Postgres pronto.")


async def on_shutdown(app: Application) -> None:
    log.info("Fechando pool Postgres...")
    await db.close_pool()
    log.info("Pool Postgres fechado.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    log.info("=" * 55)
    log.info("Bot NFe iniciando...")
    log.info("Imagens salvas em: %s/", IMAGES_DIR)
    log.info("Log level: %s", LOG_LEVEL)
    log.info("=" * 55)

    Path(IMAGES_DIR).mkdir(parents=True, exist_ok=True)

    app = build_application()
    app.post_init = on_startup
    app.post_stop = on_shutdown

    log.info("Polling iniciado. Pressione Ctrl+C para encerrar.")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )

    log.info("Bot encerrado.")


if __name__ == "__main__":
    main()
