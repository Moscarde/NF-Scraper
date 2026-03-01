"""
db.py  (workers)
================
Pool de conexões Postgres + helpers CRUD compartilhados pelos workers.
Idêntico em interface ao bot/db.py, mas com helpers adicionais para
atualizações do pipeline e inserção na camada Silver.
"""

import logging
import os
from typing import Any

import asyncpg

log = logging.getLogger("workers.db")

_pool: asyncpg.Pool | None = None


# ---------------------------------------------------------------------------
# Pool
# ---------------------------------------------------------------------------


async def get_pool() -> asyncpg.Pool:
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
            command_timeout=60,
        )
        log.info("Pool Postgres criado (workers).")
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        log.info("Pool Postgres fechado (workers).")


# ---------------------------------------------------------------------------
# bronze.received_images — leitura
# ---------------------------------------------------------------------------


async def get_image_by_id(image_id: int) -> dict[str, Any] | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM bronze.received_images WHERE id = $1",
        image_id,
    )
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# bronze.received_images — atualização de status QR
# ---------------------------------------------------------------------------


async def set_qr_processing(image_id: int) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE bronze.received_images SET qr_status = 'processing' WHERE id = $1",
        image_id,
    )


async def set_qr_success(image_id: int, qr_text: str, qr_url: str) -> None:
    pool = await get_pool()
    await pool.execute(
        """
        UPDATE bronze.received_images
        SET qr_status = 'success',
            qr_text = $2,
            qr_url  = $3,
            qr_processed_at = NOW(),
            notified_qr = TRUE
        WHERE id = $1
        """,
        image_id,
        qr_text,
        qr_url,
    )


async def set_qr_error(image_id: int) -> None:
    pool = await get_pool()
    await pool.execute(
        """
        UPDATE bronze.received_images
        SET qr_status = 'error',
            qr_processed_at = NOW(),
            notified_qr = TRUE
        WHERE id = $1
        """,
        image_id,
    )


# ---------------------------------------------------------------------------
# bronze.received_images — atualização de status scraping
# ---------------------------------------------------------------------------


async def set_scrape_processing(image_id: int) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE bronze.received_images SET scrape_status = 'processing' WHERE id = $1",
        image_id,
    )


async def set_scrape_success(image_id: int, html_path: str) -> None:
    pool = await get_pool()
    await pool.execute(
        """
        UPDATE bronze.received_images
        SET scrape_status = 'success',
            scrape_html_path = $2,
            scrape_processed_at = NOW(),
            notified_scrape = TRUE,
            notified_silver = TRUE
        WHERE id = $1
        """,
        image_id,
        html_path,
    )


async def set_scrape_error(image_id: int) -> None:
    pool = await get_pool()
    await pool.execute(
        """
        UPDATE bronze.received_images
        SET scrape_status = 'error',
            scrape_processed_at = NOW(),
            notified_scrape = TRUE
        WHERE id = $1
        """,
        image_id,
    )


# ---------------------------------------------------------------------------
# silver.nfe_headers + silver.nfe_items
# ---------------------------------------------------------------------------


async def insert_nfe_silver(
    image_id: int,
    header: dict[str, Any],
    items: list[dict[str, Any]],
    raw_html_path: str,
) -> int:
    """
    Insere cabeçalho e itens da NF-e nas tabelas Silver.
    Retorna o id do nfe_header criado.
    """
    pool = await get_pool()

    async with pool.acquire() as conn:
        async with conn.transaction():
            header_id: int = await conn.fetchval(
                """
                INSERT INTO silver.nfe_headers
                    (image_id, estabelecimento, cnpj, endereco, chave_acesso, raw_html_path)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id
                """,
                image_id,
                header.get("estabelecimento"),
                header.get("cnpj"),
                header.get("endereco"),
                header.get("chave_acesso"),
                raw_html_path,
            )

            for idx, item in enumerate(items, start=1):
                await conn.execute(
                    """
                    INSERT INTO silver.nfe_items
                        (nfe_header_id, image_id, codigo, descricao,
                         quantidade, unidade, valor_unitario, valor_total, item_order)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    header_id,
                    image_id,
                    item.get("codigo"),
                    item.get("descricao"),
                    item.get("quantidade"),
                    item.get("unidade"),
                    item.get("valor_unitario"),
                    item.get("valor_total"),
                    idx,
                )

    log.info(
        "Silver inserido: image_id=%d header_id=%d itens=%d",
        image_id,
        header_id,
        len(items),
    )
    return header_id
