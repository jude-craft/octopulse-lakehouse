import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "src"))
from ghpipe.gold.common import VELOCITY_WINDOW_DAYS
from ghpipe.gold.pipeline import process_gold
from ghpipe.spark import get_spark

SILVER = "data/silver"
GOLD = "data/gold"


def silver_days() -> list[str]:
    return sorted(p.name.split("=", 1)[1] for p in Path(f"{SILVER}/events").glob("event_date=*"))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--window-days", type=int, default=VELOCITY_WINDOW_DAYS)
    a = ap.parse_args()
    days = silver_days()
    start, end = a.start or days[0], a.end or days[-1]

    spark = get_spark("octopulse-gold")
    try:
        t0 = time.time()
        rows = process_gold(spark, SILVER, GOLD, start, end, a.window_days)
        for table, n in rows.items():
            logging.info("%-28s rows=%s", table, f"{n:,}")
        logging.info("Gold done for %s .. %s in %.0fs", start, end, time.time() - t0)
    finally:
        spark.stop()