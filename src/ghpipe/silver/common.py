from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# GitHub announced on 2025-08-08 that Events API payloads would be trimmed on 2025-10-07.
SCHEMA_CUTOVER = "2025-10-07"


def load_day(spark: SparkSession, bronze_path: str, day: str,
             types: list[str] | None = None, with_payload: bool = False) -> DataFrame:
    """Read one Bronze day. Filters and column pruning happen BEFORE any shuffle."""
    df = (spark.read.parquet(bronze_path)
          .filter(F.col("event_date") == F.lit(day).cast("date")))   # partition pruning
    if types:
        df = df.filter(F.col("type").isin(types))
    cols = [
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.to_timestamp("created_at").alias("event_ts"),
        F.col("event_date"),
        F.col("event_hour"),
        F.col("actor.id").alias("actor_id"),
        F.col("actor.login").alias("actor_login"),
        F.col("repo.id").alias("repo_id"),
        F.col("repo.name").alias("repo_name"),
        F.col("source_file"),
        F.col("ingest_ts"),
    ]
    if with_payload:
        cols.append(F.col("payload"))      # the heavy column is read only when asked for (lazy evaluation)
    return df.select(*cols)


def dedup_by_event_id(df: DataFrame) -> DataFrame:
    """Keep exactly one row per event_id, chosen deterministically."""
    w = Window.partitionBy("event_id").orderBy("source_file", "ingest_ts")
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))


def add_bot_flag(spark: SparkSession, df: DataFrame,
                 known_bots_path: str | None = None) -> DataFrame:
    """is_bot = login ends with [bot] OR login is in known_bots.csv. A lower bound."""
    by_suffix = F.coalesce(F.col("actor_login").endswith("[bot]"), F.lit(False))
    if known_bots_path and Path(known_bots_path).exists():
        bots = (spark.read.option("header", True).csv(known_bots_path)
                .select(F.lower(F.trim(F.col("login"))).alias("_bot_login"))
                .filter(F.col("_bot_login") != "")
                .distinct())
        df = df.join(F.broadcast(bots),
                     F.lower(F.col("actor_login")) == F.col("_bot_login"), "left")
        return df.withColumn("is_bot", by_suffix | F.col("_bot_login").isNotNull()) \
                 .drop("_bot_login")
    return df.withColumn("is_bot", by_suffix)


def add_era(df: DataFrame) -> DataFrame:
    """Date-based era label. Cross-checked against field presence in verify_silver.py."""
    return df.withColumn(
        "payload_era",
        F.when(F.col("event_ts") >= F.lit(SCHEMA_CUTOVER).cast("timestamp"), "B_trimmed")
         .otherwise("A_full"))


def write_partitioned(df: DataFrame, path: str) -> None:
    (df.write.option("partitionOverwriteMode", "dynamic")
       .mode("overwrite").partitionBy("event_date").parquet(path))