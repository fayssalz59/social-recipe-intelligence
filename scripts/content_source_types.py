from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SourceContentItem:
    platform: str
    creator_username: str
    content_id: str
    published_at: str | None
    title: str
    description: str
    url: str
    raw_payload: dict[str, Any] = field(default_factory=dict)
    language_hint: str | None = None
    description_is_partial: bool = False

    def to_event_payload(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "creator_username": self.creator_username,
            "content_id": self.content_id,
            "published_at": self.published_at,
            "title": self.title,
            "description": self.description,
            "url": self.url,
            "language_hint": self.language_hint,
            "description_is_partial": self.description_is_partial,
            "raw_payload": self.raw_payload,
        }