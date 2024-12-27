import asyncio
import aiohttp
import sqlite3
import hashlib
from datetime import datetime
from pathlib import Path
import logging
import time
import aiosqlite
import os


class ContentStore:
    def __init__(self, name: str, base_dir="/output"):
        self.name = name
        self.base_dir = Path(base_dir) / name
        self.db_path = self.base_dir / "content.db"
        self.storage_path = self.base_dir / "content"

        # Ensure directories exist
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.setup_db()

    def setup_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS content (
                    content_hash TEXT PRIMARY KEY,
                    first_seen TIMESTAMP,
                    last_seen TIMESTAMP,
                    size_bytes INTEGER,
                    content_type TEXT,
                    source_url TEXT,
                    file_path TEXT
                )
            """
            )

    def get_file_path(self, dt: datetime, content_hash: str) -> Path:
        return (
            self.storage_path / dt.strftime("%Y-%m-%d") / f'{dt.strftime("%H-%M-%S")}_{content_hash[:6]}'
        )

    async def store_content(
        self, content: bytes, url: str, content_type: str
    ) -> tuple[str, bool]:
        """Store content and return (content_hash, is_new)"""
        now = datetime.now()
        now_iso = now.isoformat()
        content_hash = hashlib.sha256(content).hexdigest()
        file_path = self.get_file_path(now, content_hash)
        is_new = False

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT 1 FROM content WHERE content_hash = ?", (content_hash,)
            )
            existing = await cursor.fetchone()

            if existing:
                await db.execute(
                    """
                    UPDATE content 
                    SET last_seen = ?
                    WHERE content_hash = ?
                """,
                    (now_iso, content_hash),
                )
            else:
                is_new = True
                file_path.parent.mkdir(parents=True, exist_ok=True)
                with open(file_path, "wb") as f:
                    f.write(content)

                await db.execute(
                    """
                    INSERT INTO content 
                    (content_hash, first_seen, last_seen, size_bytes, content_type, source_url, file_path)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        content_hash,
                        now_iso,
                        now_iso,
                        len(content),
                        content_type,
                        url,
                        str(file_path),
                    ),
                )

            await db.commit()

        return content_hash, is_new


class Fetcher:
    def __init__(
        self,
        name: str,
        urls: list[str],
        interval: int = 15,
        output_dir: str = "/output",
    ):
        self.name = name
        self.urls = urls
        self.interval = interval
        self.store = ContentStore(name=name, base_dir=output_dir)
        self.setup_logging()

    def setup_logging(self):
        log_path = self.store.base_dir / "fetcher.log"
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_path), logging.StreamHandler()],
        )
        self.logger = logging.getLogger(self.name)

    async def fetch_url(self, session: aiohttp.ClientSession, url: str):
        start = time.time()
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    self.logger.error(f"Error fetching {url}: HTTP {response.status}")
                    return

                content = await response.read()
                content_type = response.headers.get(
                    "content-type", "application/octet-stream"
                )
                content_hash, is_new = await self.store.store_content(
                    content, url, content_type
                )

                elapsed = time.time() - start
                status = "new" if is_new else "duplicate"
                self.logger.info(
                    f"{url}: {status} content {content_hash[:8]}... ({elapsed:.2f}s)"
                )

        except Exception as e:
            self.logger.error(f"Error fetching {url}: {e}")

    async def fetch_all(self):
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_url(session, url) for url in self.urls]
            await asyncio.gather(*tasks)

    async def run_forever(self):
        self.logger.info(f"Starting fetcher '{self.name}' for {len(self.urls)} URLs")
        try:
            while True:
                start = time.time()
                try:
                    await self.fetch_all()
                except Exception as e:
                    self.logger.error(f"Error in fetch cycle: {e}")

                elapsed = time.time() - start
                if elapsed < self.interval:
                    await asyncio.sleep(self.interval - elapsed)
        except Exception as e:
            self.logger.error(f"Fatal error: {e}")
            raise


async def run_fetchers(fetchers):
    await asyncio.gather(*(f.run_forever() for f in fetchers))


def main():
    output_dir = os.getenv("OUTPUT_DIR", "/output")
    interval = int(os.getenv("FETCH_INTERVAL", "15"))

    # Example of using multiple named fetchers
    cc_urls = [
        "https://cwwp2.dot.ca.gov/data/d3/cc/ccStatusD03.json",
    ]
    weather_urls = [
        "https://cwwp2.dot.ca.gov/data/d3/rwis/rwisStatusD03.json",
    ]

    fetchers = [
        Fetcher("cc", cc_urls, interval=interval, output_dir=output_dir),  # chains
        Fetcher(
            "rwis", weather_urls, interval=interval, output_dir=output_dir
        ),  # roadside weather
    ]

    try:
        # Run all fetchers concurrently
        asyncio.run(run_fetchers(fetchers))
    except KeyboardInterrupt:
        logging.info("Shutting down fetchers")


if __name__ == "__main__":
    main()
