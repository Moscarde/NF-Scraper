# =============================================================
# Dockerfile — workers (worker_qr + worker_scraper)
# =============================================================
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Dependências do sistema para cv2, QReader e Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    libglib2.0-0 \
    libgl1 \
    libsm6 \
    libxrender1 \
    libxext6 \
    # Playwright Firefox
    wget \
    libnss3 \
    libatk-bridge2.0-0 \
    libdrm2 \
    libxkbcommon0 \
    libgtk-3-0 \
    libgbm1 \
    libasound2 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libpangocairo-1.0-0 \
    libcairo-gobject2 \
    libcairo2 \
    xvfb \
 && rm -rf /var/lib/apt/lists/*

# Instala dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instala navegador Firefox para Playwright
RUN playwright install firefox && playwright install-deps firefox

# Copia código-fonte
COPY . .

# Diretórios montados via volume no compose
RUN mkdir -p /app/received_images /app/scraped_html

# Comando padrão — sobrescrito pelo compose para cada worker
CMD ["python", "worker_qr.py"]
