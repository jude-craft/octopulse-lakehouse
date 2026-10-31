from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ghpipe.gold.common import era_of_date


def build_contributor_activity_daily(events: DataFrame) -> DataFrame:
    """Grain: one row per (event_date, actor_id)."""
    t = F.col("event_type")
    era = era_of_date()

    daily = (
        events.groupBy("event_date", "actor_id")
        .agg(F.max_by("actor_login", "event_ts").alias("actor_login"),
             F.max("is_bot").alias("is_bot"),
             F.count("*").alias("events_total"),
             F.sum(F.when(t == "PushEvent", 1).otherwise(0)).alias("push_events"),
             F.sum(F.when(t == "PullRequestEvent", 1).otherwise(0)).alias("pr_events"),
             F.countDistinct("repo_id").alias("repos_touched"))
        .withColumn("payload_era", era)
    )

    # "First seen" is measured within each era's loaded days, never across the 13-month gap.
    w = Window.partitionBy("payload_era", "actor_id")
    segments = (events.select("event_date").distinct().withColumn("payload_era", era)
                .groupBy("payload_era").agg(F.min("event_date").alias("segment_start")))

    return (
        daily.withColumn("first_seen_date", F.min("event_date").over(w))
             .join(F.broadcast(segments), "payload_era")
             .withColumn("is_new_contributor",
                         F.when(F.col("event_date") > F.col("segment_start"),
                                F.col("event_date") == F.col("first_seen_date")))
             .drop("segment_start")
    )