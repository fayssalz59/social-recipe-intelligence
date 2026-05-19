# Tastagram

Tastagram is a small FastAPI + Jinja2 web frontend for the TikTok recipe intelligence pipeline.

## Run locally

From the repository root:

```bash
cd docker
docker compose up -d tastagram
```

Then open:

```bash
http://127.0.0.1:18090
```

## Health check

```bash
curl http://127.0.0.1:18090/health
```

## Notes

- The service uses `TASTAGRAM_API_BASE_URL` and defaults to `http://recipe-api:8000`.
- It depends on the `recipe-api` service being available in Docker.
- The app is mounted from the repository root, so code changes are live on container restart.

## TikTok thumbnails

Download thumbnails from CSV files already in `data/raw`:

```bash
docker compose --profile recovery run --rm recipe-content-recovery \
  python -u -m scripts.download_tiktok_thumbnails --source csv --input-dir data/raw
```

Download thumbnails for videos already loaded in Snowflake Bronze:

```bash
docker compose --profile recovery run --rm recipe-content-recovery \
  python -u -m scripts.download_tiktok_thumbnails --source snowflake
```

Files are written to `tastagram/static/thumbnails/{video_id}.jpg` and Tastagram displays them automatically when present.
