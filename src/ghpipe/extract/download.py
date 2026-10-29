import argparse
import gzip
import hashlib
import logging
import shutil
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

from ghpipe.manifest import connect, day_summary, get_row, upsert

log = logging.getLogger(__name__)
BASE_URL = "https://data.gharchive.org"
HEADERS = {"User-Agent": "octopulse-lakehouse/1.0"}
CHUNK = 1024 * 1024


def hour_keys(start: date, end: date):
    d = start
    while d <= end:
        for h in range(24):
            yield f"{d.isoformat()}-{h}"      # hour is NOT zero-padded
        d += timedelta(days=1)


def head_size(url: str) -> int | None:
    """Content-Length from a HEAD request; None if the file does not exist (404)."""
    req = urllib.request.Request(url, method="HEAD", headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return int(r.headers["Content-Length"])
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def gzip_ok(path: Path) -> bool:
    """Read the whole file; truncated or corrupt gzip raises."""
    try:
        with gzip.open(path, "rb") as f:
            while f.read(CHUNK):
                pass
        return True
    except (OSError, EOFError):
        return False


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch_one(key: str, dest_dir: Path, retries: int = 5) -> dict:
    """Runs in a worker thread. Returns a result dict; the main thread writes the manifest."""
    url = f"{BASE_URL}/{key}.json.gz"
    dest = dest_dir / f"{key}.json.gz"
    part = dest.with_name(dest.name + ".part")
    result = dict(hour_key=key, url=url, expected_bytes=None, actual_bytes=None, sha256=None)

    for attempt in range(1, retries + 1):
        try:
            expected = head_size(url)
            if expected is None:
                return {**result, "status": "MISSING", "attempts": attempt}

            # Adopt an existing complete file instead of re-downloading it
            if not (dest.exists() and dest.stat().st_size == expected and gzip_ok(dest)):
                req = urllib.request.Request(url, headers=HEADERS)
                with urllib.request.urlopen(req, timeout=60) as r, open(part, "wb") as f:
                    shutil.copyfileobj(r, f, CHUNK)
                if part.stat().st_size != expected:
                    raise IOError(f"size mismatch {part.stat().st_size} != {expected}")
                if not gzip_ok(part):
                    raise IOError("gzip integrity check failed")
                part.replace(dest)  # atomic rename

            return {**result, "status": "DOWNLOADED", "expected_bytes": expected,
                    "actual_bytes": dest.stat().st_size, "sha256": sha256_of(dest),
                    "attempts": attempt}
        except Exception as e:
            log.warning("%s attempt %d/%d failed: %s", key, attempt, retries, e)
            time.sleep(2 ** attempt)

    part.unlink(missing_ok=True)
    return {**result, "status": "FAILED", "attempts": retries}


def run(start: date, end: date, dest_dir: Path, db_path: Path, workers: int = 3) -> Counter:
    dest_dir.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)

    todo = []
    for key in hour_keys(start, end):
        row = get_row(conn, key)
        f = dest_dir / f"{key}.json.gz"
        done = (row and row["status"] == "DOWNLOADED"
                and f.exists() and f.stat().st_size == row["actual_bytes"])
        if not done:
            todo.append(key)
    log.info("%d hours requested, %d to fetch", len(list(hour_keys(start, end))), len(todo))

    counts: Counter = Counter()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(fetch_one, k, dest_dir) for k in todo]
        for fut in as_completed(futures):
            res = fut.result()
            upsert(conn, **res)               # only the main thread touches SQLite
            counts[res["status"]] += 1

    for r in day_summary(conn):
        log.info("%s downloaded=%d missing=%d failed=%d",
                 r["day"], r["downloaded"], r["missing"], r["failed"])
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    p = argparse.ArgumentParser(description="Download GH Archive hourly files")
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    p.add_argument("--dest", type=Path, default=Path("data/raw/gharchive"))
    p.add_argument("--db", type=Path, default=Path("data/state/manifest.db"))
    p.add_argument("--workers", type=int, default=3)
    a = p.parse_args()
    run(a.start, a.end, a.dest, a.db, a.workers)


if __name__ == "__main__":
    main()
