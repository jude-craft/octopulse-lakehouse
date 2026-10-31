from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ghpipe.silver.common import SCHEMA_CUTOVER

# Rolling-metric settings. With 3 consecutive days per era only a 1-day window fits;
# a window of N days needs 2N consecutive loaded days.
VELOCITY_WINDOW_DAYS = 1
MIN_PRIOR_STARS = 5


def day_num():
    """Days since 1970-01-01 as an integer: a numeric ordering key for range windows."""
    return F.datediff(F.col("event_date"), F.lit("1970-01-01").cast("date"))


def era_of_date():
    return (F.when(F.col("event_date") >= F.lit(SCHEMA_CUTOVER).cast("date"), F.lit("B_trimmed"))
             .otherwise(F.lit("A_full")))


def read_silver(spark: SparkSession, silver_root: str, table: str, start: str, end: str) -> DataFrame:
    return (spark.read.parquet(f"{silver_root}/{table}")
            .filter((F.col("event_date") >= F.lit(start).cast("date")) &
                    (F.col("event_date") <= F.lit(end).cast("date"))))


def coverage_calendar(events: DataFrame, span_days: int) -> DataFrame:
    """For each loaded date: are ALL of the last `span_days` calendar days (itself included) loaded?"""
    loaded = events.select("event_date").distinct().withColumn("dn", day_num())
    w = Window.orderBy("dn").rangeBetween(-(span_days - 1), 0)
    return (loaded.withColumn("days_loaded", F.count("*").over(w))
                  .select("event_date", (F.col("days_loaded") == span_days).alias("history_complete")))


def write_partitioned(df: DataFrame, path: str) -> None:
    (df.write.option("partitionOverwriteMode", "dynamic")
       .mode("overwrite").partitionBy("event_date").parquet(path))