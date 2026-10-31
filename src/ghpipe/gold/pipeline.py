import logging
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ghpipe.gold.common import VELOCITY_WINDOW_DAYS, read_silver, write_partitioned
from ghpipe.gold.contributors import build_contributor_activity_daily
from ghpipe.gold.language_activity import build_language_activity_daily
from ghpipe.gold.pr_health import build_pr_health_daily
from ghpipe.gold.pr_merge_time import build_pr_merge_time_daily
from ghpipe.gold.repo_activity import build_repo_activity_daily

log = logging.getLogger(__name__)
TABLES = ("repo_activity_daily", "language_activity_daily", "pr_health_daily",
          "contributor_activity_daily", "pr_merge_time_daily")


def _per_day(df, value_col=None) -> dict:
    agg = F.count("*") if value_col is None else F.sum(value_col)
    return {r["event_date"]: r["v"] for r in df.groupBy("event_date").agg(agg.alias("v")).collect()}


def process_gold(spark: SparkSession, silver_root: str, gold_root: str,
                 start: str, end: str, window_days: int = VELOCITY_WINDOW_DAYS) -> dict:
    lookback = 2 * window_days - 1
    read_start = (date.fromisoformat(start) - timedelta(days=lookback)).isoformat()
    in_range = ((F.col("event_date") >= F.lit(start).cast("date")) &
                (F.col("event_date") <= F.lit(end).cast("date")))
    paths = {t: f"{gold_root}/{t}" for t in TABLES}

    events = read_silver(spark, silver_root, "events", read_start, end)   # includes lookback days
    pr = read_silver(spark, silver_root, "pull_requests", start, end)

    write_partitioned(build_repo_activity_daily(events, window_days).filter(in_range),
                      paths["repo_activity_daily"])
    write_partitioned(build_language_activity_daily(pr), paths["language_activity_daily"])
    write_partitioned(build_pr_health_daily(pr), paths["pr_health_daily"])
    write_partitioned(build_contributor_activity_daily(events).filter(in_range),
                      paths["contributor_activity_daily"])
    write_partitioned(build_pr_merge_time_daily(pr), paths["pr_merge_time_daily"])      

    # Reconcile every table back to Silver, per day
    read = lambda name: spark.read.parquet(paths[name]).filter(in_range)
    silver_events = _per_day(events.filter(in_range))
    silver_prs = _per_day(pr)
    repo = read("repo_activity_daily")
    typed = repo.withColumn("typed", F.col("push_events") + F.col("fork_events")
                            + F.col("star_events") + F.col("pr_events"))
    timed = pr.filter((F.col("pr_outcome") == "merged")                                 
                      & F.col("merged_at").isNotNull() & F.col("pr_created_at").isNotNull())
    checks = {
        "repo_activity total_events": (_per_day(repo, "total_events"), silver_events),
        "repo_activity typed sum": (_per_day(typed, "typed"), silver_events),
        "language pr_events": (_per_day(read("language_activity_daily"), "pr_events"), silver_prs),
        "pr_health pr_events": (_per_day(read("pr_health_daily"), "pr_events"), silver_prs),
        "contributors events_total": (_per_day(read("contributor_activity_daily"), "events_total"),
                                      silver_events),
        "merge_time merged_prs_with_times": (
            _per_day(read("pr_merge_time_daily"), "merged_prs_with_times"), _per_day(timed)),
    }
    for name, (gold, silver) in checks.items():
        if gold != silver:
            raise RuntimeError(f"Gold reconciliation failed: {name}\n gold={gold}\n silver={silver}")

    return {t: read(t).count() for t in TABLES}