# ==============================
# 🏗️ Stage 1: Builder
# ==============================
FROM python:3.11-alpine AS builder
WORKDIR /app

# Hanya instal library dasar yang diperlukan untuk kompilasi psycopg2-binary
RUN apk add --no-cache \
    gcc musl-dev \
    bash coreutils curl ca-certificates \
 && update-ca-certificates

COPY requirements.txt .
# --prefix=/install untuk menginstal dependensi ke folder terpisah
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ==============================
# 🚀 Stage 2: Runtime (Kecil)
# ==============================
FROM python:3.11-alpine
WORKDIR /app

# Instal runtime dependencies minimal (musl-libc, dll.)
RUN apk add --no-cache \
    ca-certificates coreutils bash \
 && update-ca-certificates

# Salin dependensi yang sudah terinstal dari stage builder
COPY --from=builder /install /usr/local

# Salin script worker utama Anda
COPY worker_rainfall.py .

# Pastikan Python tidak melakukan buffering output (penting untuk log)
ENV PYTHONUNBUFFERED=1

# Perintah default untuk menjalankan worker
# Ini menjalankan fungsi looping utama Anda: run_worker_app()
CMD ["python", "worker_rainfall.py"]
