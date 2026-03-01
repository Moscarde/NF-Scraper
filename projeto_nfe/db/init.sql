-- =============================================================
-- init.sql
-- Esquema inicial do banco de dados NF-e
-- Executado automaticamente pelo Postgres na primeira inicialização
-- =============================================================

-- Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;

-- =============================================================
-- Bronze: tabela de usuários Telegram
-- =============================================================
CREATE TABLE IF NOT EXISTS bronze.telegram_users (
    id                  SERIAL PRIMARY KEY,
    telegram_user_id    BIGINT NOT NULL UNIQUE,
    username            TEXT,
    full_name           TEXT,
    greeted             BOOLEAN DEFAULT FALSE,
    first_seen          TIMESTAMPTZ DEFAULT NOW(),
    last_seen           TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================
-- Bronze: imagens recebidas (dados brutos, imutáveis)
-- =============================================================
CREATE TABLE IF NOT EXISTS bronze.received_images (
    id                  SERIAL PRIMARY KEY,
    filename            TEXT NOT NULL UNIQUE,
    file_path           TEXT NOT NULL,
    received_at         TIMESTAMPTZ DEFAULT NOW(),

    -- Dados do usuário Telegram
    telegram_user_id    BIGINT NOT NULL,
    telegram_username   TEXT,
    telegram_full_name  TEXT,
    chat_id             BIGINT NOT NULL,
    message_id          INT NOT NULL,

    -- Metadados do arquivo
    file_unique_id      TEXT,
    file_size           INT,
    width               INT,
    height              INT,
    caption             TEXT,

    -- Status do pipeline: QR Code
    qr_status           TEXT DEFAULT 'pending',   -- pending | processing | success | error
    qr_text             TEXT,
    qr_url              TEXT,
    qr_processed_at     TIMESTAMPTZ,

    -- Status do pipeline: Scraping
    scrape_status       TEXT DEFAULT 'pending',   -- pending | processing | success | error
    scrape_html_path    TEXT,
    scrape_processed_at TIMESTAMPTZ,

    -- Notificações enviadas ao usuário
    notified_qr         BOOLEAN DEFAULT FALSE,
    notified_scrape     BOOLEAN DEFAULT FALSE,
    notified_silver     BOOLEAN DEFAULT FALSE
);

-- =============================================================
-- Silver: cabeçalho da NF-e
-- =============================================================
CREATE TABLE IF NOT EXISTS silver.nfe_headers (
    id              SERIAL PRIMARY KEY,
    image_id        INT REFERENCES bronze.received_images(id) ON DELETE CASCADE,
    estabelecimento TEXT,
    cnpj            TEXT,
    endereco        TEXT,
    chave_acesso    TEXT,
    scraped_at      TIMESTAMPTZ DEFAULT NOW(),
    raw_html_path   TEXT
);

-- =============================================================
-- Silver: itens da NF-e
-- =============================================================
CREATE TABLE IF NOT EXISTS silver.nfe_items (
    id              SERIAL PRIMARY KEY,
    nfe_header_id   INT REFERENCES silver.nfe_headers(id) ON DELETE CASCADE,
    image_id        INT REFERENCES bronze.received_images(id) ON DELETE CASCADE,
    codigo          TEXT,
    descricao       TEXT,
    quantidade      NUMERIC(10,3),
    unidade         TEXT,
    valor_unitario  NUMERIC(10,2),
    valor_total     NUMERIC(10,2),
    item_order      INT
);

-- =============================================================
-- Indexes
-- =============================================================
CREATE INDEX IF NOT EXISTS idx_received_images_user
    ON bronze.received_images (telegram_user_id);

CREATE INDEX IF NOT EXISTS idx_received_images_qr_status
    ON bronze.received_images (qr_status);

CREATE INDEX IF NOT EXISTS idx_received_images_scrape_status
    ON bronze.received_images (scrape_status);

CREATE INDEX IF NOT EXISTS idx_nfe_items_header
    ON silver.nfe_items (nfe_header_id);

CREATE INDEX IF NOT EXISTS idx_nfe_items_image
    ON silver.nfe_items (image_id);

CREATE INDEX IF NOT EXISTS idx_telegram_users_uid
    ON bronze.telegram_users (telegram_user_id);
