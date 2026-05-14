FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    xvfb \
    xauth \
    procps \
    wget \
    curl \
    ca-certificates \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpangocairo-1.0-0 \
    libpango-1.0-0 \
    libcairo2 \
    libx11-6 \
    libxcb1 \
    libxext6 \
    libxrender1 \
    libxi6 \
    libgtk-3-0 \
    libglib2.0-0 \
    libdbus-1-3 \
    libfontconfig1 \
    libfreetype6 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-tiktok-monitor.txt /app/requirements-tiktok-monitor.txt

RUN pip install --no-cache-dir -r /app/requirements-tiktok-monitor.txt
RUN python -m playwright install --with-deps chromium

COPY . /app

CMD ["bash", "-lc", "Xvfb :99 -screen 0 1280x1024x24 -nolisten tcp > /tmp/xvfb.log 2>&1 & export DISPLAY=:99; exec python -u -m scripts.tiktok_creator_monitor"]
