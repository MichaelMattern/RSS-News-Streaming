# Finance News RSS Scraper

Collect the latest business and markets headlines from a handful of hand‑picked RSS feeds and store them in a single compressed [Parquet](https://parquet.apache.org/) file for fast, column‑oriented analytics.

---

## ✨ Key Features

| Feature                  | Details                                                                                                                                     |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------- |
| **Parallel downloads**   | Each RSS feed is fetched concurrently with `ThreadPoolExecutor`, making a full refresh finish in only a few seconds.                        |
| **Canonical schema**     | Every entry is normalized into the same column set — `article_id`, `timestamp`, `title`, `summary`, `tags`, `authors`, `url`, and `source`. |
| **Stable IDs**           | A SHA‑1 hash of the article URL yields a 40‑character key that survives feed reorderings and duplicates.                                    |
| **Timezone handling**    | Timestamps are converted to America/New\_York (`NY_TZ`) and saved in ISO‑8601 format.                                                       |
| **Zero‑copy storage**    | Output is written to `news.parquet` using the ultra‑fast **pyarrow** engine with **zstd** compression.                                      |
| **Simple extensibility** | Add or remove feeds by editing the `SOURCES` list at the bottom of the script.                                                              |

---

## 🔧 Requirements

- **Python ≥ 3.9** (for the `zoneinfo` module)
- pip packages:
  ```bash
  pip install feedparser beautifulsoup4 pandas pyarrow
  ```

> **Tip 💡** Create an isolated environment first:
>
> ```bash
> python -m venv .venv && source .venv/bin/activate  # PowerShell: .venv\Scripts\Activate.ps1
> ```

---

## 🚀 Running the Scraper

```bash
python rss_scraper.py
```

Sample output:

```
✅ Parquet written: 1.83 MB (162 articles)
⏱️  Total run time: 4.27 s
⏱️  Per-article time: 0.03 s
```

The Parquet file is created in the working directory and can be inspected with `pandas`:

```python
import pandas as pd
df = pd.read_parquet("news.parquet")
print(df.head())
```

---

## 📝 Customizing the Feed List

Open `rss_scraper.py` and edit the `SOURCES` list:

```python
SOURCES = [
    "http://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    # add more …
]
```

> **Good practice:** keep the list short and relevant — many general‑news feeds publish hundreds of unrelated stories per hour.

---

## ⚙️ How It Works (Under the Hood)

1. **Fetch & parse** All feeds are downloaded in parallel (max 16 workers) and processed by `feedparser`.
2. **Canonicalise** Each entry is fed to `canonical_row()` where:
   - The publication date is parsed to UTC, converted to `NY_TZ`, and ISO‑formatted.
   - The article URL is hashed (`sha1`) to produce a unique `article_id`.
   - HTML summaries are stripped to plain text with **BeautifulSoup**.
3. **Frame & write** Rows are collected into a `pandas.DataFrame`, duplicates are dropped, and the table is sorted newest‑first before being persisted as Parquet with **Zstandard** compression.

---

## 🔍 Querying the Output (Examples)

```python
# Top publishers in the last 24 h
(df[df.timestamp > (pd.Timestamp.utcnow() - pd.Timedelta('1D')).isoformat()]
   .groupby('source').size().sort_values(ascending=False).head())
```

```python
# Search headlines for a ticker symbol
mask = df.title.str.contains(r"\bMSFT\b", case=False, regex=True)
print(df.loc[mask, ["timestamp", "title", "url"]])
```

---

## 🏎️ Performance Tweaks

| Parameter           | Where                                 | Why you might change it                                                                  |
| ------------------- | ------------------------------------- | ---------------------------------------------------------------------------------------- |
| `max_feed_workers`  | top‑level `__main__` block            | Increase for *many* feeds; decrease on low‑core machines or to be gentle on the sources. |
| `max_row_workers`   | same                                  | Controls how many entries are canonicalised in parallel.                                 |
| Parquet compression | `to_parquet(..., compression="zstd")` | Switch to `snappy`, `gzip`, or leave `None` for speed over size.                         |

---

## 🛠️ Extending the Schema

Need more columns? Inside `canonical_row()` you already have access to the raw `feed_entry` object. Add new keys to the returned dict and they will appear automatically in the DataFrame.


