from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ghpipe.gold.common import era_of_date


def build_pr_merge_time_daily(pr: DataFrame) -> DataFrame:
    """Grain: one row per (event_date, is_bot). PR-level percentiles, not per-repo medians.
    is_bot refers to the actor of the closing event."""
    hours = (F.unix_timestamp("merged_at") - F.unix_timestamp("pr_created_at")) / 3600.0
    merged = pr.filter((F.col("pr_outcome") == "merged")
                       & F.col("merged_at").isNotNull() & F.col("pr_created_at").isNotNull())
    return (
        merged.groupBy("event_date", "is_bot")
        .agg(F.count("*").alias("merged_prs_with_times"),
             F.percentile_approx(hours, [0.25, 0.5, 0.75, 0.9]).alias("_q"))
        .select("event_date", "is_bot", "merged_prs_with_times",
                F.col("_q")[0].alias("p25_hours"), F.col("_q")[1].alias("p50_hours"),
                F.col("_q")[2].alias("p75_hours"), F.col("_q")[3].alias("p90_hours"))
        .withColumn("payload_era", era_of_date())
    )