import logging
import os
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone
from typing import Optional

try:  # Optional dependency
    import redis  # type: ignore
except Exception:  # noqa: BLE001
    redis = None


DEFAULT_TTL_HOURS = 72


class SentStore:
    def __init__(self, ttl_hours: int = DEFAULT_TTL_HOURS, db_path: str = "news_sent.db") -> None:
        self.ttl_hours = ttl_hours
        self.redis_url = os.getenv("REDIS_URL")
        self.db_path = db_path

        self._redis_client = None
        if self.redis_url and redis:
            try:
                self._redis_client = redis.from_url(self.redis_url, decode_responses=True)
                self._redis_client.ping()
                logging.info("Using Redis sent store")
            except Exception as exc:  # noqa: BLE001
                logging.warning("Redis unavailable, falling back to SQLite: %s", exc)
                self._redis_client = None

        if not self._redis_client:
            self._ensure_sqlite()

    def _ensure_sqlite(self) -> None:
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sent_news (
                    story_id TEXT PRIMARY KEY,
                    expires_at INTEGER
                )
                """
            )
            conn.commit()
        logging.info("Using SQLite sent store at %s", self.db_path)

    def _purge_sqlite(self) -> None:
        now_ts = int(time.time())
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("DELETE FROM sent_news WHERE expires_at <= ?", (now_ts,))
            conn.commit()

    def is_sent(self, story_id: str) -> bool:
        if self._redis_client:
            try:
                return bool(self._redis_client.exists(story_id))
            except Exception as exc:  # noqa: BLE001
                logging.warning("Redis check failed: %s", exc)

        self._purge_sqlite()
        with closing(sqlite3.connect(self.db_path)) as conn:
            row = conn.execute("SELECT 1 FROM sent_news WHERE story_id=?", (story_id,)).fetchone()
            return row is not None

    def mark_sent(self, story_id: str) -> None:
        expires_at = datetime.now(timezone.utc) + timedelta(hours=self.ttl_hours)
        expires_ts = int(expires_at.timestamp())

        if self._redis_client:
            try:
                self._redis_client.setex(story_id, self.ttl_hours * 3600, "1")
                return
            except Exception as exc:  # noqa: BLE001
                logging.warning("Redis write failed: %s", exc)

        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO sent_news (story_id, expires_at) VALUES (?, ?)",
                (story_id, expires_ts),
            )
            conn.commit()

    def mark_many(self, story_ids) -> None:
        for story_id in story_ids:
            self.mark_sent(story_id)

