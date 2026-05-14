# TikTok Recipe Discovery Scraper

Script:

- `scripts/tiktok_recipe_discovery.py`

Purpose:

- discover public TikTok recipe videos from known creators
- discover additional food creators through TikTok user search
- collect videos from recipe-related hashtags
- filter captions likely to contain recipe content
- export a Bronze-compatible CSV

Output:

- `data/raw/tiktok_recipe_discovery.csv`

Required runtime:

- `TikTokApi`
- `playwright`
- a usable `TIKTOK_MS_TOKEN` in `.env`
- preferably the existing `tiktok-monitor` Docker image because it already
  contains the browser/runtime dependencies

Recommended server command:

```bash
cd ~/recipe/social-recipe-intelligence/docker
docker compose run --no-deps --rm tiktok-monitor bash -lc '
Xvfb :99 -screen 0 1280x1024x24 -nolisten tcp > /tmp/xvfb.log 2>&1 &
export DISPLAY=:99
python -u -m scripts.tiktok_recipe_discovery \
  --session-timeout 180 \
  --max-rows 300 \
  --per-creator 10 \
  --per-hashtag 20 \
  --per-user-search 5 \
  --per-searched-user 5 \
  --debug-rejects \
  --sleep-min 10 \
  --sleep-max 20
'
```

Windows PowerShell one-line version:

```powershell
docker compose run --no-deps --rm tiktok-monitor bash -lc "Xvfb :99 -screen 0 1280x1024x24 -nolisten tcp > /tmp/xvfb.log 2>&1 & export DISPLAY=:99; python -u -m scripts.tiktok_recipe_discovery --session-timeout 180 --max-rows 300 --per-creator 10 --per-hashtag 20 --per-user-search 5 --per-searched-user 5 --debug-rejects --sleep-min 10 --sleep-max 20"
```

Then ingest the discovered CSV:

```bash
docker compose exec airflow-scheduler bash -lc '
cd /opt/airflow
python -m scripts.load_bronze --input-dir data/raw --pattern "tiktok_recipe_discovery.csv"
python -m scripts.enrich_silver --limit 500
cd dbt_project
export DBT_TARGET_PATH=/tmp/dbt_target
export DBT_LOG_PATH=/tmp/dbt_logs
dbt run --profiles-dir . --no-partial-parse
'
```

Notes:

- TikTokApi is unofficial and TikTok can block or throttle requests.
- The wrapper documentation says direct search is limited mostly to users, so
  this scraper combines user search, known creators, and hashtag videos.
- Captions are marked `DESCRIPTION_IS_PARTIAL=true` because a TikTok caption may
  not contain the full recipe shown in the video.
- Use small batches first. If the container starts returning empty responses,
  refresh `TIKTOK_MS_TOKEN`, slow down the run, or use a cleaner IP/proxy.
- Use `--no-deps` for one-off discovery runs so Kafka/Postgres/Zookeeper do not
  start unnecessarily.
- `xvfb-run` blocked in Docker Desktop during testing. The working setup is
  manual Xvfb plus `DISPLAY=:99`, with Chromium in headed mode.
