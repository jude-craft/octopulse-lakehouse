import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType,BooleanType)

log = logging.getLogger(__name__)

IN_SCOPE_TYPES = ["PushEvent","PullRequestEvent","WatchEvent","ForkEvent"]
FILE_RE = r"(\d{4}-\d{2}-\d{2})-(\d{1,2})\.json\.gz$"

ENVELOPE_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("id", LongType(), True),
        StructField("login", StringType(), True)
    ]),True),
     StructField("repo", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True)]), True),
    StructField("payload", StringType(), True),
    StructField("public", BooleanType(), True),
    StructField("created_at", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

def process_bronze(spark: SparkSession, raw_path:str, bronze_out: str, 
                   quarantine_out: str, run_ts: str | None=None) -> dict:
    run_ts = run_ts or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    
    raw = (spark.read.schema(ENVELOPE_SCHEMA)
           .option("mode", "PERMISSIVE")
           .option("columnNameOfCorruptRecord","_corrupt_record")
           .json(raw_path))
    
   # Lineage + partition keys come from the FILE NAME, not created_at
    df = (raw.select("*", F.col("_metadata.file_path").alias("source_file"))
             .select("*",
                     F.to_date(F.regexp_extract("source_file", FILE_RE, 1)).alias("event_date"),
                     F.regexp_extract("source_file", FILE_RE, 2).cast("int").alias("event_hour"),
                     F.lit(run_ts).cast("timestamp").alias("ingest_ts")))
    
    # Pass 1: Count everything in ONE aggregation
    rows = (df.select("event_date",
                      F.col("_corrupt_record").isNotNull().alias("is_corrupt"),
                      F.col("type").isin(IN_SCOPE_TYPES).alias("in_scope"))
            .groupBy("event_date", "is_corrupt","in_scope").count().collect())
    n_corrupt = sum(r["count"] for r in rows if r["is_corrupt"])
    n_in = sum(r["count"] for r in rows if not r["is_corrupt"] and r["in_scope"])
    n_out = sum(r["count"] for r in rows if not r["is_corrupt"] and not r["in_scope"])
    dates = sorted({r["event_date"] for r in rows})
    log.info("raw=%d in_scope=%d out_of_scope=%d corrupt=%d",
             n_in + n_out + n_corrupt, n_in, n_out, n_corrupt)    
    
    # Pass 2: write in-scope rows (overwrite touches only these dates)
    good = (df.filter(F.col("_corrupt_record").isNull()
                      & F.col("type").isin(IN_SCOPE_TYPES))
              .drop("_corrupt_record"))
    (good.write.option("partitionOverwriteMode","dynamic")
                .mode("overwrite")
                .partitionBy("event_date")
                .parquet(bronze_out))
    
    # Quarantine: idempotent overwrite by date
    if n_corrupt:
        (df.filter(F.col("_corrupt_record").isNotNull())
           .select("id","_corrupt_record","source_file","event_date","ingest_ts")
           .write.option("partitionOverwriteMode", "dynamic")
           .mode("overwrite")
           .partitionBy("event_date")
           .parquet(quarantine_out))
    # Data Contract Reconciliation: what landed must equal what we counted
    written = (spark.read.parquet(bronze_out)
                    .filter(F.col("event_date").isin(dates)).count())
    if written != n_in:
        raise RuntimeError(f"Bronze reconciliation failed: wrote {written}, expected{n_in}")
    return {"in_scope": n_in, "out_of_scope": n_out, "corrupt": n_corrupt, "dates": dates}