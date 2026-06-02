# syntax=docker/dockerfile:1.7
#
# streamedtom3u — HLS proxy for streamed.pk
#
# Multi-arch image (linux/amd64, linux/arm64). Uses python:3.12-slim and lets
# Playwright fetch the matching Chromium headless-shell for the build arch.
#
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    PORT=8765

# Runtime dependencies for headless Chromium (covers both amd64 and arm64).
# `playwright install --with-deps` would handle this too, but doing it explicitly
# keeps the layer cacheable and the image self-contained.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        fonts-liberation \
        fonts-noto-color-emoji \
        libasound2 \
        libatk1.0-0 \
        libatk-bridge2.0-0 \
        libatspi2.0-0 \
        libcairo2 \
        libcups2 \
        libdbus-1-3 \
        libdrm2 \
        libgbm1 \
        libglib2.0-0 \
        libnspr4 \
        libnss3 \
        libpango-1.0-0 \
        libx11-6 \
        libx11-xcb1 \
        libxcb1 \
        libxcomposite1 \
        libxdamage1 \
        libxext6 \
        libxfixes3 \
        libxkbcommon0 \
        libxrandr2 \
        libxshmfence1 \
        wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps first so Docker can cache the heavy Playwright install layer
COPY requirements.txt ./
RUN pip install -r requirements.txt && \
    python -m playwright install chromium && \
    # Remove the bundled ffmpeg from playwright (we don't use it) to shave the image
    find /ms-playwright -type d -name "ffmpeg-*" -prune -exec rm -rf {} + || true

COPY server.py ./

# Persistent state lives here; mount a volume to survive container recreation
RUN mkdir -p /data
VOLUME ["/data"]
ENV DATA_DIR=/data

EXPOSE 8765 8768

# Tini-less; uvicorn handles SIGTERM cleanly via lifespan
CMD ["python", "server.py"]
