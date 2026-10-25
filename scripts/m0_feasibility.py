import json
import urllib.request
import shutil
import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType

# Idempotent Directories

RAW_DIR = Path("data/raw/m0_sample")
RAW_DIR.mkdir(parents=True, exist_ok=True)

SAMPLES = {
    "window_a": "2025-08-15-15", # Pre-change: Expect full payloads
    "window_b": "2026-09-15-15", # Post-change: Expect trimmed payloads
}

def check_size_and_download(label: str, hour_str: str) -> Path:
    url = f"https://data.gharchive.org/{hour_str}.json.gz"
    dest = RAW_DIR / f"{hour_str}.json.gz"
    part = dest.with_name(dest.name + ".part")
    headers = {"User-Agent": "octopulse-m0/1.0"}

    head = urllib.request.Request(url, method="HEAD", headers=headers)
    with urllib.request.urlopen(head, timeout=30) as resp:
        expected = int(resp.headers["Content-Length"])
    print(f"[{label}] {hour_str}: {expected / 1024**2:.2f} MB compressed")

    if dest.exists() and dest.stat().st_size == expected:
        print("  already complete, skipping")
        return dest

    for attempt in range(1, 6):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=60) as r, open(part, "wb") as f:
                shutil.copyfileobj(r, f, length=1024 * 1024)
            if part.stat().st_size != expected:
                raise IOError(f"size mismatch: {part.stat().st_size} != {expected}")
            part.replace(dest)  # atomic rename
            print("  downloaded OK")
            return dest
        except Exception as e:
            print(f"  [!] attempt {attempt} failed: {e}")
            time.sleep(2 ** attempt)  # backoff: 2, 4, 8, 16, 32 s
    part.unlink(missing_ok=True)
    raise RuntimeError(f"giving up on {url}")

def verify_with_spark(files: dict[str, Path]):
    spark = (SparkSession.builder.appName("octopulse-m0-feasibility")
             .master("local[*]")
             .config("spark.driver.memory", "4g")
             .config("spark.sql.session.timezone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("actor", StructType([
            StructField("id", LongType(), True),
            StructField("login", StringType(), True)]), True),
        StructField("repo", StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True)]), True),
        StructField("payload", StringType(), True),
        StructField("public", BooleanType(), True),
        StructField("created_at", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ])  

    for label, path in files.items():
        print(f"\n=== {label.upper()} ({path.name}) ===")
        df = (spark.read.schema(schema)
              .option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", "_corrupt_record")
              .json(str(path)))

        # ONE pass: totals, corrupt, bots, duplicate check
        p = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(F.col("_corrupt_record").isNotNull(), 1).otherwise(0)).alias("corrupt"),
            F.sum(F.when(F.col("actor.login").endswith("[bot]"), 1).otherwise(0)).alias("bots"),
            F.countDistinct("id").alias("distinct_ids"),
        ).first()
        print(f"rows={p.total:,} corrupt={p.corrupt} bots={p.bots:,} "
              f"({p.bots / p.total:.2%}) duplicate_ids={p.total - p.distinct_ids}")

        df.groupBy("type").count().orderBy(F.desc("count")).show(8, truncate=False)

        # A6: did Spark stringify the nested payload object?
        sample = df.filter(F.col("type") == "PullRequestEvent").select("payload").first()
        print("payload sample:", (sample.payload[:150] if sample else None))

        # A1-A3: field presence RATES by PR action, not one row
        (df.filter(F.col("type") == "PullRequestEvent")
           .select(F.get_json_object("payload", "$.action").alias("action"),
                   F.get_json_object("payload", "$.pull_request.merged").alias("merged"),
                   F.get_json_object("payload", "$.pull_request.base.repo.language").alias("lang"))
           .groupBy("action")
           .agg(F.count("*").alias("n"),
                F.sum(F.col("merged").isNotNull().cast("int")).alias("has_merged"),
                F.sum(F.col("lang").isNotNull().cast("int")).alias("has_language"))
           .show(truncate=False))

        # A4: push payload fields
        (df.filter(F.col("type") == "PushEvent")
           .select(F.get_json_object("payload", "$.commits[0].sha").alias("commit0"),
                   F.get_json_object("payload", "$.size").alias("size"),
                   F.get_json_object("payload", "$.distinct_size").alias("dsize"))
           .agg(F.count("*").alias("pushes"),
                F.sum(F.col("commit0").isNotNull().cast("int")).alias("has_commits"),
                F.sum(F.col("size").isNotNull().cast("int")).alias("has_size"),
                F.sum(F.col("dsize").isNotNull().cast("int")).alias("has_distinct_size"))
           .show(truncate=False))
    spark.stop()
    
if __name__ == "__main__":
    # Step 1: Check size and download
    downloaded = {k: check_size_and_download(k,v) for k,v in SAMPLES.items()}
    # Step 2: Run Spark analysis
    verify_with_spark(downloaded)