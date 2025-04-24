import feedparser, hashlib, bs4, pandas as pd
from zoneinfo import ZoneInfo
from urllib.parse import urlparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# ──────────────────────────────────────────────────────────────────────────────
# Constants & helpers
NY_TZ = ZoneInfo("America/New_York")     # preferred zone for timestamps

# Get the site name from a URL to populate the "source" column in the DataFrame.
def site_name(url: str) -> str:
    """Return a clean publisher name from a URL."""
    host = (urlparse(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host.split(".")[-2].upper() if "." in host else host.upper()


# Creates a datapoint for each article pulled from the RSS feed
def canonical_row(feed_entry) -> dict:
    """Coerce one feed entry into the canonical schema."""
    ts_utc   = pd.to_datetime(feed_entry.published, utc=True, errors="coerce")
    ts_local = ts_utc.tz_convert(NY_TZ)

    url = feed_entry.link
    aid = hashlib.sha1(url.encode()).hexdigest()   # 40-char stable key

    return {
        "article_id": aid,
        "timestamp":  ts_local.isoformat(timespec="seconds"),
        "title":      feed_entry.title.strip(),
        "summary":    bs4.BeautifulSoup(
                          feed_entry.summary, "html.parser"
                      ).get_text(" ", strip=True),
        "tags":    ", ".join(t["term"] for t in feed_entry.get("tags", [])),
        "authors": ", ".join(a.get("name", "").strip()
                             for a in feed_entry.get("authors", [])),
        "url":     url,
        "source":  site_name(url),
    }


# RSS sources — add / remove as needed
SOURCES = [
    "http://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "https://feeds.content.dowjones.io/public/rss/WSJcomUSBusiness",
    "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain",
    "https://feeds.content.dowjones.io/public/rss/RSSWSJD",
    "https://feeds.content.dowjones.io/public/rss/RSSPersonalFinance",
    "http://feeds.harvardbusiness.org/harvardbusiness?format=xml",
]


if __name__ == "__main__":
    # start timer
    t0 = time.perf_counter()

    # 1️⃣  download every RSS feed in parallel
    max_feed_workers = min(16, len(SOURCES))
    with ThreadPoolExecutor(max_workers=max_feed_workers) as pool:
        feeds = list(pool.map(feedparser.parse, SOURCES))

    all_entries = [entry for feed in feeds for entry in feed.entries]

    # 2️⃣  canonicalize each entry concurrently
    rows = []
    max_row_workers = 8
    with ThreadPoolExecutor(max_workers=max_row_workers) as pool:
        futures = {pool.submit(canonical_row, e): e for e in all_entries}
        for fut in as_completed(futures):
            try:
                rows.append(fut.result())
            except Exception as exc:
                bad = futures[fut].link
                print(f"⚠️  skipped {bad}: {exc}")

    # 3️⃣  build DataFrame → Parquet
    (pd.DataFrame(rows)
        .drop_duplicates(subset="article_id")
        .sort_values("timestamp", ascending=False)
        .reset_index(drop=True)
        .to_parquet("news.parquet",
                    engine="pyarrow",
                    compression="zstd",
                    index=False))

    # stop timer
    elapsed = time.perf_counter() - t0

    # print results
    print("✅ Parquet written:",
          f"{Path('news.parquet').stat().st_size/1_048_576:.2f} MB",
          f"({len(rows)} articles)")
    print(f"⏱️  Total run time: {elapsed:.2f} s")
    print("⏱️  Per-article time:",
          f"{elapsed/len(rows):.2f} s" if rows else "N/A")
