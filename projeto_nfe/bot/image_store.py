"""
image_store.py  (bot)
=====================
Baixa e persiste as imagens recebidas pelo bot.
Além de salvar o arquivo em disco, insere o registro em
bronze.received_images e publica o image_id na fila queue:qr_parse.

Nome do arquivo gerado:
    uid{user_id}_mid{message_id}_{YYYYMMDD_HHMMSS}_{file_unique_id}.jpg
"""

import logging
import os
from datetime import datetime
from pathlib import Path

import redis.asyncio as aioredis
from telegram import PhotoSize, Message

import db

log = logging.getLogger("bot.image_store")


def _get_images_dir() -> Path:
    images_dir = Path(os.getenv("IMAGES_DIR", "/app/received_images"))
    images_dir.mkdir(parents=True, exist_ok=True)
    return images_dir


def build_filename(user_id: int, message_id: int, file_unique_id: str) -> str:
    """Gera o nome do arquivo com informações de rastreabilidade."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"uid{user_id}_mid{message_id}_{ts}_{file_unique_id}.jpg"


async def _get_redis() -> aioredis.Redis:
    return aioredis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
    )


async def save_photo(message: Message) -> dict | None:
    """
    Baixa a melhor resolução da foto, salva em disco,
    insere em bronze.received_images e enfileira em queue:qr_parse.

    Retorna dict com metadados, ou None em caso de erro.
    """
    if not message.photo:
        log.warning("save_photo chamado sem foto na mensagem %d", message.message_id)
        return None

    best_photo: PhotoSize = message.photo[-1]

    user           = message.from_user
    user_id        = user.id
    username       = user.username or ""
    full_name      = user.full_name or ""
    message_id     = message.message_id
    file_unique_id = best_photo.file_unique_id
    chat_id        = message.chat_id

    filename   = build_filename(user_id, message_id, file_unique_id)
    images_dir = _get_images_dir()
    image_path = images_dir / filename

    # ── Download da imagem ───────────────────────────────────────────────
    try:
        tg_file = await best_photo.get_file()
        await tg_file.download_to_drive(str(image_path))
        log.info(
            "Foto salva: %s | user=%d (%s) | msg=%d | size=%dx%d",
            image_path, user_id, username, message_id,
            best_photo.width, best_photo.height,
        )
    except Exception as exc:
        log.error("Erro ao baixar foto msg=%d: %s", message_id, exc, exc_info=True)
        return None

    # ── Inserção em bronze.received_images ───────────────────────────────
    try:
        image_id = await db.insert_received_image(
            filename=filename,
            file_path=str(image_path),
            telegram_user_id=user_id,
            telegram_username=username,
            telegram_full_name=full_name,
            chat_id=chat_id,
            message_id=message_id,
            file_unique_id=file_unique_id,
            file_size=best_photo.file_size,
            width=best_photo.width,
            height=best_photo.height,
            caption=message.caption or None,
        )
    except Exception as exc:
        log.error("Erro ao inserir imagem no banco: %s", exc, exc_info=True)
        return None

    # ── Publicação na fila queue:qr_parse ────────────────────────────────
    try:
        redis_client = await _get_redis()
        async with redis_client as r:
            await r.rpush("queue:qr_parse", str(image_id))
        log.info("image_id=%d enfileirado em queue:qr_parse", image_id)
    except Exception as exc:
        log.error(
            "Erro ao publicar image_id=%d em queue:qr_parse: %s",
            image_id, exc, exc_info=True,
        )

    return {
        "image_id":   image_id,
        "filename":   filename,
        "file_path":  str(image_path),
        "user_id":    user_id,
        "chat_id":    chat_id,
        "message_id": message_id,
    }
