# Verified Social Recipe Video Dataset

Main import file:

- `data/raw/social_recipe_verified_real_videos.csv`

This CSV is intended for Bronze ingestion and contains only rows linked to an
identified public TikTok, Instagram, or YouTube Shorts URL.

Current generated volume:

- 60 rows total
- 24 TikTok URLs
- 32 YouTube Shorts URLs
- 4 Instagram URLs
- 0 search-result URLs
- 0 duplicate video URLs

The first three columns remain compatible with the legacy loader contract:

1. `TITLE`
2. `DESCRIPTION`
3. `URL_TIKTOK`

Additional columns preserve multi-platform metadata:

- `PLATFORM`
- `CONTENT_ID`
- `CREATOR_USERNAME`
- `SOURCE_PLATFORM_URL`
- `RECIPE_LANGUAGE_HINT`
- `CUISINE_HINT`
- `MAIN_INGREDIENT_HINT`
- `DESCRIPTION_IS_PARTIAL`
- `DATA_ORIGIN`
- `VERIFICATION_SOURCE_URL`

Important distinction:

- The previous `social_recipe_seed_*_120.csv` files were synthetic portfolio seed
  data and used discovery/search URLs. They were removed from `data/raw` to avoid
  accidental ingestion as real scraped content.
- `social_recipe_verified_real_videos.csv` uses real social URLs. Some recipe
  descriptions are paraphrased from public recipe pages that embed or link the
  video, so the dataset remains practical for LLM enrichment without pretending
  that search URLs are video permalinks.

Regenerate the verified CSV with:

```bash
python scripts/generate_verified_real_video_csv.py
```

The generator intentionally keeps the dataset smaller than a synthetic seed file:
the priority is to preserve provenance with `VERIFICATION_SOURCE_URL` for each
row, so portfolio reviewers can understand where the video URL and recipe context
came from.
