import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "src"))
from ghpipe.silver.pipeline import process_silver_day
from ghpipe.spark import get_spark

BRONZE = "data/bronze/events"
SILVER = "data/silver"
KNOWN_BOTS = "config/known_bots.csv"


def bronze_days() -> list[str]:
    return sorted(p.name.split("=", 1)[1] for p in Path(BRONZE).glob("event_date=*"))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", nargs="*", help="YYYY-MM-DD dates (default: every date in Bronze)")
    days = ap.parse_args().days or bronze_days()

    spark = get_spark("octopulse-silver")
    try:
        t_all = time.time()
        for day in days:
            t0 = time.time()
            s = process_silver_day(spark, day, BRONZE, SILVER, KNOWN_BOTS)
            logging.info(
                "%s bronze=%s events=%s dup_removed=%s bots=%s prs=%s pushes=%s time=%.0fs",
                day, f"{s['bronze_rows']:,}", f"{s['silver_events']:,}",
                f"{s['duplicates_removed']:,}", f"{s['bot_events']:,}",
                f"{s['pull_requests']:,}", f"{s['pushes']:,}", time.time() - t0)
        logging.info("All days done in %.0fs", time.time() - t_all)
    finally:
        spark.stop()