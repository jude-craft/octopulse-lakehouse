import logging
import re
import sys
import time
from collections import Counter
from pathlib import Path

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

sys.path.append(str(Path(__file__).parent.parent / "src"))
from ghpipe.bronze import process_bronze


# ---- Settings ----
RAW_DIR = "data/raw/gharchive"
BRONZE_OUT = "data/bronze/events"
QUARANTINE_OUT = "data/quarantine"
DEV_SLICE = False 

def files_per_day(raw_dir:str) -> Counter:
    """Count hourly files per UTC date, based on the filename."""
    pat = re.compile(r"(\d{4}-\d{2}-\d{2})-\d{1,2}\.json\.gz$")
    return Counter(
        m.group(1)
        for p in Path(raw_dir).glob("*.json.gz")
        if (m:= pat.search(p.name))
    )

def guard_partial_days(raw_dir:str, dev_slice:bool) -> None:
    """"Dynamically overwrites replaces a whole  date partition, so every date we touch
        must have all 24 hours loaded, or we would silently drop data.
    """
    counts = files_per_day(raw_dir)
    if not counts:
        raise SystemExit(f"No. .json.gz files found in {raw_dir}")
    for day, n in sorted(counts.items()):
        if n==24:
            continue
        if dev_slice:
            logging.warning("%s: only %d/24 hours present (dev slice)", day, n)
        else:
            raise SystemExit(
                f"{day}: {n}/24 hourly files present. Refusing to overwrite that date's partition."
            )
            
if __name__ == "__main__":
    guard_partial_days(RAW_DIR, DEV_SLICE)
    days = sorted(files_per_day(RAW_DIR))

    spark = (
        SparkSession.builder
        .appName("octopulse-bronze")
        .master("local[4]")                      # cap concurrent file parsing; raise if RAM allows
        .config("spark.driver.memory", "6g")     # check `free -h` first
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        t_all = time.time()
        for day in days:
            t0 = time.time()
            r = process_bronze(spark, f"{RAW_DIR}/{day}-*.json.gz", BRONZE_OUT, QUARANTINE_OUT)
            total = r["in_scope"] + r["out_of_scope"] + r["corrupt"]
            logging.info("%s raw=%s in_scope=%s out_of_scope=%s corrupt=%s time=%.0fs",
                         day, f"{total:,}", f"{r['in_scope']:,}",
                         f"{r['out_of_scope']:,}", f"{r['corrupt']:,}", time.time() - t0)
        logging.info("All days done in %.0fs", time.time() - t_all)
    finally:
        spark.stop()