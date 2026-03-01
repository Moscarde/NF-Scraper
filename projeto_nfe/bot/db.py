"""
db.py  (bot)
============
Pool de conexões Postgres + helpers CRUD para bronze.
Usa asyncpg para operações assíncronas.
"""

import logging
import os
from typing import Any

import asyncpg

log = logging.getLogger("bot.db")

_pool: asyncpg.Pool | None = None


# ---------------------------------------------------------------------------
# Pool
# ---------------------------------------------------------------------------

async def get_pool() -> asyncpg.Pool:
    """Retorna o pool, criando-o na primeira chamada."""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "nfe"),
            user=os.getenv("POSTGRES_USER", "nfe_user"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
            min_size=2,
            max_size=10,
            command_timeout=30,
        )
        log.info("Pool Postgres criado.")
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        log.info("Pool Postgres fechado.")


# ---------------------------------------------------------------------------
# bronze.telegram_users
# ---------------------------------------------------------------------------

async def upsert_telegram_user(
    user_id: int,
    username: str | None,
    full_name: str | None,
) -> None:
    """Insere ou atualiza usuário. Atualiza last_seen."""
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO bronze.telegram_users (telegram_user_id, username, full_name)
        VALUES ($1, $2, $3)
        ON CONFLICT (telegram_user_id)
        DO UPDATE SET
            username  = EXCLUDED.username,
            full_name = EXCLUDED.full_name,
            last_seen = NOW()
        """,
        user_id, username, full_name,
    )


async def was_greeted(user_id: int) -> bool:
    """Retorna True se o usuário já recebeu a saudação."""
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT greeted FROM bronze.telegram_users WHERE telegram_user_id = $1",
        user_id,
    )
    return bool(row["greeted"]) if row else False


async def mark_greeted(user_id: int) -> None:
    """Marca o usuário como saudado."""
    pool = await get_pool()
    await pool.execute(
        """
        UPDATE bronze.telegram_users
        SET greeted = TRUE
        WHERE telegram_user_id = $1
        """,
        user_id,
    )


# ---------------------------------------------------------------------------
# bronze.received_images
# ---------------------------------------------------------------------------

async def insert_received_image(
    filename: str,
    file_path: str,
    telegram_user_id: int,
    telegram_username: str | None,
    telegram_full_name: str | None,
    chat_id: int,
    message_id: int,
    file_unique_id: str | None,
    file_size: int | None,
    width: int | None,
    height: int | None,
    caption: str | None,
) -> int:
    """Insere registro de imagem recebida e retorna o id gerado."""
    pool = await get_pool()
    row = await pool.fetchrow(
        """
        INSERT INTO bronze.received_images (
            filename, file_path,
            telegram_user_id, telegram_username, telegram_full_name,
            chat_id, message_id,
            file_unique_id, file_size, width, height, caption
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
        )
        RETURNING id
        """,
        filename, file_path,
        telegram_user_id, telegram_username, telegram_full_name,
        chat_id, message_id,
        file_unique_id, file_size, width, height, caption,
    )
    image_id: int = row["id"]
    log.info("Imagem inserida em bronze.received_images: id=%d filename=%s", image_id, filename)
    return image_id


async def get_image_by_id(image_id: int) -> dict[str, Any] | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM bronze.received_images WHERE id = $1",
        image_id,
    )
    return dict(row) if row else None
