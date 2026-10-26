import logging
import re
import sys
from collections import Counter
from pathlib import Path

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

sys.path.append(str(Path(__file__).parent.parent / "src"))
from ghpipe.bronze import process_bronze


# ---- Settings ----
RAW_DIR = "data/raw/m0_sample"
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
            
if __name__ == "__main__": # Only run the code below if I am executing this file directly from the terminal.
    guard_partial_days(RAW_DIR, DEV_SLICE)
    
    spark = (
        SparkSession.builder
        .appName("octopulse-bronze")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        logging.info("Starting Bronze Processing...")
        results = process_bronze(spark, RAW_DIR, BRONZE_OUT, QUARANTINE_OUT)
        
        total = results["in_scope"] + results["out_of_scope"] + results["corrupt"]
        logging.info("--- Final Results ---")
        logging.info(f"Dates processed:     {results["dates"]}")
        logging.info(f"Total raw rows:       {total:,}")
        logging.info(f"In scope written:     {results['in_scope']:,}")
        logging.info(f"Out of scope skipped: {results['out_of_scope']:,}")
        logging.info(f"Corrupt quarantined:  {results['corrupt']:,}")
    finally:
        spark.stop()