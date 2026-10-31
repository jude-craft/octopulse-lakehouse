from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ghpipe.gold.common import era_of_date


def build_language_activity_daily(pr: DataFrame) -> DataFrame:
    """Grain: one row per (event_date, language), including an explicit 'UNKNOWN' bucket."""
    g = (
        pr.withColumn("language", F.coalesce(F.col("base_language"), F.lit("UNKNOWN")))
          .groupBy("event_date", "language")
          .agg(F.countDistinct("repo_id").alias("active_repos"),
               F.count("*").alias("pr_events"),
               F.sum(F.when(F.col("pr_action") == "opened", 1).otherwise(0)).alias("pr_opened_events"),
               F.sum(F.when((F.col("pr_action") == "opened") & ~F.col("is_bot"), 1)
                      .otherwise(0)).alias("pr_opened_human_events"),
               F.countDistinct("actor_id").alias("distinct_contributors"))
          .withColumn("is_known_language", F.col("language") != "UNKNOWN")
    )
    w = Window.partitionBy("event_date")
    known = F.when(F.col("is_known_language"), F.col("pr_events")).otherwise(0)
    return (
        g.withColumn("share_of_pr_events", F.col("pr_events") / F.sum("pr_events").over(w))
         .withColumn("language_coverage", F.sum(known).over(w) / F.sum("pr_events").over(w))
         .withColumn("payload_era", era_of_date())
    )