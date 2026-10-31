from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ghpipe.gold.common import (MIN_PRIOR_STARS, VELOCITY_WINDOW_DAYS,
                                coverage_calendar, day_num, era_of_date)


def build_repo_activity_daily(events: DataFrame, window_days: int = VELOCITY_WINDOW_DAYS,
                              min_prior_stars: int = MIN_PRIOR_STARS) -> DataFrame:
    """Grain: one row per (event_date, repo_id) with at least one in-scope event."""
    t = F.col("event_type")

    def count_if(cond):
        return F.sum(F.when(cond, 1).otherwise(0))     # conditional aggregation

    daily = (
        events.groupBy("event_date", "repo_id")
        .agg(F.max_by("repo_name", "event_ts").alias("repo_name"),   # latest name that day
             F.count("*").alias("total_events"),
             count_if(t == "PushEvent").alias("push_events"),
             count_if(t == "ForkEvent").alias("fork_events"),
             count_if(t == "WatchEvent").alias("star_events"),       # star ACTIONS, not total stars
             count_if(t == "PullRequestEvent").alias("pr_events"),
             count_if(F.col("is_bot")).alias("bot_events"),
             F.countDistinct("actor_id").alias("distinct_actors"),
             F.countDistinct(F.when(t == "PushEvent", F.col("actor_id"))).alias("distinct_pushers"))
        .withColumn("human_events", F.col("total_events") - F.col("bot_events"))
    )

    n = window_days
    order = day_num()
    cur = Window.partitionBy("repo_id").orderBy(order).rangeBetween(-(n - 1), 0)
    prev = Window.partitionBy("repo_id").orderBy(order).rangeBetween(-(2 * n - 1), -n)

    out = (
        daily
        .withColumn("star_events_window", F.sum("star_events").over(cur))
        .withColumn("star_events_prior_window",
                    F.coalesce(F.sum("star_events").over(prev), F.lit(0)))
        .join(F.broadcast(coverage_calendar(events, 2 * n)), "event_date", "left")
        .withColumnRenamed("history_complete", "velocity_history_complete")
    )

    min_prior = max(min_prior_stars, 1)                 # never divide by zero
    ok = F.col("velocity_history_complete") & (F.col("star_events_prior_window") >= min_prior)
    rank_w = Window.partitionBy("event_date").orderBy(F.desc("star_velocity"),
                                                      F.desc("star_events_window"))
    return (
        out
        .withColumn("star_delta", F.when(F.col("velocity_history_complete"),
                                         F.col("star_events_window") - F.col("star_events_prior_window")))
        .withColumn("star_velocity", F.when(ok, F.col("star_events_window") / F.col("star_events_prior_window")))
        .withColumn("trending_rank", F.when(F.col("star_velocity").isNotNull(),
                                            F.row_number().over(rank_w)))
        .withColumn("velocity_window_days", F.lit(n))
        .withColumn("payload_era", era_of_date())
        .select("event_date", "payload_era", "repo_id", "repo_name", "total_events",
                "push_events", "fork_events", "star_events", "pr_events",
                "bot_events", "human_events", "distinct_actors", "distinct_pushers",
                "star_events_window", "star_events_prior_window", "star_delta",
                "star_velocity", "velocity_window_days", "velocity_history_complete",
                "trending_rank")
    )