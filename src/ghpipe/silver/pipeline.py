import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ghpipe.silver.common import (add_bot_flag, add_era, dedup_by_event_id,
                                  load_day, write_partitioned)
from ghpipe.silver.pull_requests import transform_pull_requests
from ghpipe.silver.pushes import transform_pushes

log = logging.getLogger(__name__)


def _events(spark, bronze_path, day, bots_path):
    """All event types, no payload column: cheap to shuffle, so dedup first."""
    df = dedup_by_event_id(load_day(spark, bronze_path, day))
    return add_era(add_bot_flag(spark, df, bots_path))


def _typed(spark, bronze_path, day, bots_path, types, transform):
    """Typed tables: parse the heavy payload into small columns BEFORE the dedup shuffle."""
    df = load_day(spark, bronze_path, day, types=types, with_payload=True)
    df = add_era(add_bot_flag(spark, df, bots_path))     # broadcast join: no shuffle
    df = transform(df)                                   # from_json; payload is dropped here
    return dedup_by_event_id(df).drop("source_file", "ingest_ts")


def process_silver_day(spark: SparkSession, day: str, bronze_path: str,
                       silver_root: str, known_bots_path: str | None = None) -> dict:
    day_col = F.lit(day).cast("date")
    out_events = f"{silver_root}/events"
    out_pr = f"{silver_root}/pull_requests"
    out_push = f"{silver_root}/pushes"

    bronze_rows = (spark.read.parquet(bronze_path)
                   .filter(F.col("event_date") == day_col).count())
    if bronze_rows == 0:
        raise RuntimeError(f"No Bronze rows for {day}")

    write_partitioned(_events(spark, bronze_path, day, known_bots_path), out_events)
    write_partitioned(_typed(spark, bronze_path, day, known_bots_path,
                             ["PullRequestEvent"], transform_pull_requests), out_pr)
    write_partitioned(_typed(spark, bronze_path, day, known_bots_path,
                             ["PushEvent"], transform_pushes), out_push)

    events = spark.read.parquet(out_events).filter(F.col("event_date") == day_col)
    n_events = events.count()
    by_type = {r["event_type"]: r["count"]
               for r in events.groupBy("event_type").count().collect()}
    n_bots = events.filter("is_bot").count()
    n_pr = spark.read.parquet(out_pr).filter(F.col("event_date") == day_col).count()
    n_push = spark.read.parquet(out_push).filter(F.col("event_date") == day_col).count()

    if n_events > bronze_rows:
        raise RuntimeError(f"{day}: Silver events {n_events} > Bronze rows {bronze_rows}")
    if n_pr != by_type.get("PullRequestEvent", 0):
        raise RuntimeError(f"{day}: PR rows {n_pr} != events {by_type.get('PullRequestEvent', 0)}")
    if n_push != by_type.get("PushEvent", 0):
        raise RuntimeError(f"{day}: push rows {n_push} != events {by_type.get('PushEvent', 0)}")

    return {"day": day, "bronze_rows": bronze_rows, "silver_events": n_events,
            "duplicates_removed": bronze_rows - n_events, "bot_events": n_bots,
            "pull_requests": n_pr, "pushes": n_push}