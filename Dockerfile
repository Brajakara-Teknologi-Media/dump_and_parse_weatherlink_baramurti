# File: Dockerfile

# ===============================
# 🏗️ Stage 1: Builder (Untuk Kompilasi)
# ===============================
FROM python:3.11-alpine AS builder
WORKDIR /app

# Instal tools yang diperlukan untuk kompilasi psycopg2-binary
# Termasuk gcc dan musl-dev
RUN apk add --no-cache \
    gcc musl-dev \
    bash coreutils curl ca-certificates \
 && update-ca-certificates

# Diasumsikan ada file requirements.txt
COPY requirements.txt .
# Instal dependensi ke folder /install (untuk disalin di stage berikutnya)
RUN pip install --no-cache-dir --prefix=/usr/local -r requirements.txt


# ===============================
# 🚀 Stage 2: Runtime (Image Final yang Kecil)
# ===============================
FROM python:3.11-alpine
WORKDIR /app

# Instal runtime dependencies minimal
RUN apk add --no-cache \
    ca-certificates coreutils bash 

# Salin dependensi Python yang sudah terinstal dari stage builder
COPY --from=builder /usr/local /usr/local

# Salin script worker utama Anda
COPY worker_rainfall.py .
COPY .env .

# Pastikan Python tidak melakukan buffering output (penting untuk log real-time)
ENV PYTHONUNBUFFERED=1

# CMD tidak diperlukan jika sudah ada 'command' di docker-compose, 
# tetapi bisa ditinggalkan sebagai fallback.
CMD ["python", "worker_rainfall.py"]
