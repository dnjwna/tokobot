# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Jakarta

RUN apt-get update && apt-get install -y --no-install-recommends tzdata ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 tokobot

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=tokobot:tokobot . .
RUN mkdir -p /app/state /app/logs && chown -R tokobot:tokobot /app/state /app/logs
USER tokobot

# Sehat bila heartbeat (ditulis tiap 60 dtk saat feed harga segar) < 180 dtk
HEALTHCHECK --interval=60s --timeout=5s --start-period=180s --retries=3 \
  CMD python -c "import os,sys,time; p='/app/state/heartbeat'; sys.exit(0 if os.path.exists(p) and time.time()-os.path.getmtime(p)<180 else 1)"

ENTRYPOINT ["python", "main.py"]
