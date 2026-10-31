import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "src"))
from pyspark.sql import Window
from pyspark.sql import functions as F

from ghpipe.spark import get_spark

spark = get_spark("octopulse-verify-gold")
G = "data/gold"
repo = spark.read.parquet(f"{G}/repo_activity_daily")
lang = spark.read.parquet(f"{G}/language_activity_daily")
health = spark.read.parquet(f"{G}/pr_health_daily")
contrib = spark.read.parquet(f"{G}/contributor_activity_daily")

print("\n== Grain uniqueness (rows must equal distinct keys) ==")
for name, df, keys in [("repo_activity", repo, ["event_date", "repo_id"]),
                       ("language", lang, ["event_date", "language"]),
                       ("pr_health", health, ["event_date", "repo_id"]),
                       ("contributors", contrib, ["event_date", "actor_id"])]:
    n, d = df.count(), df.select(*keys).distinct().count()
    print(f"{name:14s} rows={n:,} distinct_keys={d:,} -> {'OK' if n == d else 'DUPLICATES'}")

print("\n== Velocity history coverage per day ==")
repo.groupBy("event_date", "velocity_history_complete").count().orderBy("event_date").show()

print("== Top 5 trending repos per day (where velocity is defined) ==")
(repo.filter("trending_rank <= 5")
     .select("event_date", "trending_rank", "repo_name", "star_events_window",
             "star_events_prior_window", "star_velocity")
     .orderBy("event_date", "trending_rank").show(40, False))

print("== Top 5 repos by total events per day (who dominates?) ==")
w = Window.partitionBy("event_date").orderBy(F.desc("total_events"))
(repo.withColumn("rk", F.row_number().over(w)).filter("rk <= 5")
     .select("event_date", "rk", "repo_name", "total_events", "push_events", "bot_events")
     .orderBy("event_date", "rk").show(40, False))

print("== Language: top 5 per day, plus coverage ==")
lw = Window.partitionBy("event_date").orderBy(F.desc("pr_events"))
(lang.withColumn("rk", F.row_number().over(lw)).filter("rk <= 5")
     .select("event_date", "language", "pr_events", F.round("share_of_pr_events", 4).alias("share"),
             F.round("language_coverage", 4).alias("coverage"))
     .orderBy("event_date", "rk").show(40, False))

print("== PR health per day ==")
(health.groupBy("event_date", "payload_era")
       .agg(F.sum("prs_opened").alias("opened"), F.sum("prs_merged").alias("merged"),
            F.sum("prs_closed_unmerged").alias("closed_unmerged"),
            F.sum("prs_closed_unknown").alias("closed_unknown"),
            F.round(F.sum("prs_merged") / F.sum("prs_closed"), 4).alias("merge_rate_overall"))
       .orderBy("event_date").show(truncate=False))

print("== Time to merge (PR level, hours) ==")
spark.read.parquet(f"{G}/pr_merge_time_daily").orderBy("event_date", "is_bot").show(truncate=False)

print("== Contributors per day ==")
(contrib.groupBy("event_date")
        .agg(F.count("*").alias("actors"),
             F.round(F.avg(F.col("is_bot").cast("int")), 4).alias("bot_share_of_actors"),
             F.round(F.avg(F.col("is_new_contributor").cast("int")), 4).alias("new_share"))
        .orderBy("event_date").show())

spark.stop()