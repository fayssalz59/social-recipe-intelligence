from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from TikTokApi import TikTokApi

from scripts.content_source_types import SourceContentItem

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env", override=True)


class TikTokClient:
    def __init__(self) -> None:
        self._api: TikTokApi | None = None

    async def __aenter__(self) -> "TikTokClient":
        self._api = TikTokApi()
        await self._api.__aenter__()

        ms_token = os.getenv("TIKTOK_MS_TOKEN")
        ms_tokens = [ms_token] if ms_token else None

        await self._api.create_sessions(
            num_sessions=1,
            headless=False,
            ms_tokens=ms_tokens,
            sleep_after=8,
            browser="chromium",
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._api is not None:
            await self._api.__aexit__(exc_type, exc, tb)
            self._api = None

    async def fetch_recent_videos_for_creator(
        self,
        creator_username: str,
        count: int = 5,
    ) -> list[SourceContentItem]:
        if self._api is None:
            raise RuntimeError("TikTokClient must be used inside an async context manager.")

        items: list[SourceContentItem] = []
        user = self._api.user(username=creator_username)

        async for video in user.videos(count=count):
            data = getattr(video, "as_dict", {}) or {}

            video_id = str(data.get("id") or "").strip()
            if not video_id:
                continue

            description = str(data.get("desc") or "").strip()
            create_time = data.get("createTime")

            published_at = None
            if create_time:
                try:
                    published_at = datetime.fromtimestamp(
                        int(create_time),
                        tz=timezone.utc,
                    ).isoformat()
                except Exception:
                    published_at = None

            items.append(
                SourceContentItem(
                    platform="tiktok",
                    creator_username=creator_username,
                    content_id=video_id,
                    published_at=published_at,
                    title=description[:120] if description else "",
                    description=description,
                    url=f"https://www.tiktok.com/@{creator_username}/video/{video_id}",
                    description_is_partial=True,
                    raw_payload={
                        "id": data.get("id"),
                        "desc": data.get("desc"),
                        "createTime": data.get("createTime"),
                    },
                )
            )

        return items


async def demo() -> None:
    async with TikTokClient() as client:
        items = await client.fetch_recent_videos_for_creator("thegoldenbalance", count=3)
        for item in items:
            print(item)


if __name__ == "__main__":
    asyncio.run(demo())