# =============================================================
# Dockerfile — bot
# =============================================================
FROM python:3.12-slim

# Evita buffers de stdout/stderr que atrapalham logs em containers
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Dependências do sistema mínimas
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*

# Instala dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia código-fonte
COPY . .

# Diretório de imagens (montado via volume no compose)
RUN mkdir -p /app/received_images /app/logs

CMD ["python", "bot.py"]
