import feedparser, hashlib, bs4, pandas as pd
from zoneinfo import ZoneInfo
from urllib.parse import urlparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

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


# ── NEW “main” SECTION ────────────────────────────────────────────────────────
if __name__ == "__main__":
    t0 = time.perf_counter()

    # 1️⃣  pull every feed in parallel
    with ThreadPoolExecutor(max_workers=min(16, len(SOURCES))) as pool:
        feeds = list(pool.map(feedparser.parse, SOURCES))

    all_entries = [e for f in feeds for e in f.entries]

    # 2️⃣  canonicalize concurrently
    rows, seen_this_run = [], set()
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(canonical_row, e): e for e in all_entries}
        for fut in as_completed(futures):
            try:
                row = fut.result()
                if row["article_id"] not in seen_this_run:
                    rows.append(row)
                    seen_this_run.add(row["article_id"])
            except Exception as exc:          # bad feed item → skip
                print(f"⚠️  skipped {futures[fut].link}: {exc}")

    fresh_df = (pd.DataFrame(rows)
                  .sort_values("timestamp", ascending=False)
                  .reset_index(drop=True))

    # 3️⃣  append-if-new instead of overwrite-and-drop
    OUT = Path("news.parquet")
    if OUT.exists():
        old_df     = pd.read_parquet(OUT, engine="pyarrow")
        before_set = set(old_df["article_id"])
        fresh_df   = fresh_df[~fresh_df["article_id"].isin(before_set)]

        combined = pd.concat([old_df, fresh_df], ignore_index=True)
    else:
        combined = fresh_df

    # write back
    combined.to_parquet(OUT, engine="pyarrow",
                        compression="zstd", index=False)

    # 4️⃣  stats
    elapsed = time.perf_counter() - t0
    print(f"🆕  Added {len(fresh_df)} new articles "
          f"(now {len(combined)} total)")
    print("✅ Parquet size:",
          f"{OUT.stat().st_size/1_048_576:.2f} MB")
    print(f"⏱️  Run time: {elapsed:.2f}s "
          f"({elapsed/max(len(rows),1):.2f}s/article)")