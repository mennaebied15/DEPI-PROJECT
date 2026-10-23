"""
github_extractor.py
Download the dated Olist files for ONE run date from a GitHub repo and save them
in ./gathered_data/.

Repo file names carry the date, e.g. olist_customers_dataset_2026-10-24.csv
Saved as:                            gathered_customers_data_2026-10-24.csv
Running again for the same date overwrites the same files.

Usage:
    python github_extractor.py                    # today (this computer's date)
    python github_extractor.py --date 2026-10-24  # a specific date

Which date is used (first match wins):
    1. the run_date argument of run() / the --date flag
    2. the global RUN_DATE below (for quick local testing)
    3. today
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import time
from datetime import date, datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OWNER = os.getenv("GITHUB_OWNER", "mennaebied15-maker")
REPO = os.getenv("GITHUB_REPO", "olist-batched-data")
BRANCH = os.getenv("GITHUB_BRANCH", "main")
API = "https://api.github.com"
TIMEOUT = (5, 60)  # (connect, read) seconds

OUTPUT_DIR = Path("gathered_data")
DATE_FORMAT = "%Y-%m-%d"  # how the date appears in the repo file names

# Set e.g. RUN_DATE = "2026-10-24" to test. None = today.
# A date passed to run() always wins over this.
RUN_DATE="2026-10-24"

# table name -> file-name prefix in the repo
TABLES = {
    "customers": "olist_customers_dataset",
    "orders": "olist_orders_dataset",
    "order_items": "olist_order_items_dataset",
    "products": "olist_products_dataset",
}

log = logging.getLogger("extract.github")


class ExtractError(Exception):
    """Extraction failed."""


def resolve_run_date(run_date: date | str | None = None) -> date:
    if run_date is None:
        return date.today()
    if isinstance(run_date, datetime):
        return run_date.date()
    if isinstance(run_date, date):
        return run_date
    try:
        return datetime.strptime(str(run_date), DATE_FORMAT).date()
    except ValueError as exc:
        raise ExtractError(f"Bad date {run_date!r}; expected YYYY-MM-DD") from exc


def match_table(file_name: str, run_date: date) -> str | None:
    """Which table a repo file is for this run date (None = not one of ours)."""
    date_str = run_date.strftime(DATE_FORMAT)
    for table, prefix in TABLES.items():
        if re.match(rf"^{re.escape(prefix)}[\W_]*{re.escape(date_str)}\.csv$",
                    file_name, flags=re.IGNORECASE):
            return table
    return None


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/vnd.github+json",
                      "User-Agent": "olist-etl-extractor"})
    token = os.getenv("GITHUB_TOKEN")
    if token:
        s.headers["Authorization"] = f"Bearer {token}"
    retry = Retry(total=4, backoff_factor=1.5,
                  status_forcelist=(500, 502, 503, 504), allowed_methods=("GET",))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def get(session: requests.Session, url: str, **kwargs) -> requests.Response:
    """GET with a wait-and-retry when GitHub rate-limits us."""
    for attempt in range(1, 4):
        try:
            resp = session.get(url, timeout=TIMEOUT, **kwargs)
        except (requests.Timeout, requests.ConnectionError) as exc:
            raise ExtractError(f"Network failure for {url}: {exc}") from exc
        remaining = resp.headers.get("X-RateLimit-Remaining")
        if resp.status_code == 429 or (resp.status_code == 403 and remaining == "0"):
            reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait = int(resp.headers.get("Retry-After", max(reset - time.time(), 1)))
            log.warning("Rate limited. Sleeping %ss (attempt %s/3)", wait, attempt)
            time.sleep(min(wait, 120))
            continue
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            raise ExtractError(f"HTTP {resp.status_code} for {url}") from exc
        return resp
    raise ExtractError(f"Gave up on {url} after repeated rate limiting")


def list_files(session: requests.Session, path: str = "") -> list[dict]:
    url = f"{API}/repos/{OWNER}/{REPO}/contents/{path}".rstrip("/")
    items = get(session, url, params={"ref": BRANCH}).json()
    if isinstance(items, dict):
        items = [items]
    return [i for i in items if i.get("type") == "file"]


def run(
    run_date: date | str | None = None,
    path: str = "",
    out_dir: Path | str = OUTPUT_DIR,
    session: requests.Session | None = None,
) -> list[str]:
    """Download the files dated `run_date`; return the list of saved paths."""
    d = resolve_run_date(run_date if run_date is not None else RUN_DATE)
    date_str = d.strftime(DATE_FORMAT)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    session = session or build_session()
    log.info("Extracting %s/%s for %s", OWNER, REPO, date_str)

    found: dict[str, dict] = {}
    for item in list_files(session, path):
        table = match_table(item["name"], d)
        if table:
            if table in found:
                raise ExtractError(f"Two files match {table} for {date_str}")
            found[table] = item

    if not found:
        raise ExtractError(f"No files dated {date_str} found in the repo")
    missing = [t for t in TABLES if t not in found]
    if missing:
        log.warning("No file dated %s for: %s", date_str, missing)

    saved = []
    for table, item in found.items():
        data = get(session, item["download_url"]).content
        if not data.strip():  # nothing came back: keep the old copy, don't overwrite
            raise ExtractError(f"{item['name']} came back empty from GitHub")
        final = out_dir / f"gathered_{table}_data_{date_str}.csv"
        tmp = final.with_name(f".{final.name}.part")
        tmp.write_bytes(data)
        os.replace(tmp, final)  # overwrites the same date's file
        log.info("Saved %s (%d bytes) -> %s", item["name"], len(data), final)
        saved.append(str(final))
    return saved


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (default: today)")
    ap.add_argument("--path", default="", help="repo folder (default: repo root)")
    args = ap.parse_args()
    for p in run(args.date, args.path):
        print(p)