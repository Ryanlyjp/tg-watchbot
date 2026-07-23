FROM python:3.12-slim

ARG APP_UID=999
ARG APP_GID=994

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TG_WATCHBOT_DATA_DIR=/data \
    WEB_PANEL_BIND_HOST=0.0.0.0

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r /app/requirements.txt \
    && groupadd --gid "${APP_GID}" tg-watchbot \
    && useradd --uid "${APP_UID}" --gid "${APP_GID}" --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin tg-watchbot \
    && mkdir -p /data \
    && chown tg-watchbot:tg-watchbot /data

COPY --chown=root:root . /app

EXPOSE 8765

USER tg-watchbot:tg-watchbot

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["curl", "--fail", "--silent", "--show-error", "http://127.0.0.1:8765/health"]

CMD ["python", "app.py"]
