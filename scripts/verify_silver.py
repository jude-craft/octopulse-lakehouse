import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "src"))
from pyspark.sql import Window
from pyspark.sql import functions as F

from ghpipe.spark import get_spark

spark = get_spark("octopulse-verify-silver")
ev = spark.read.parquet("data/silver/events")
pr = spark.read.parquet("data/silver/pull_requests")
pu = spark.read.parquet("data/silver/pushes")

print("\n== Rows per day ==")
ev.groupBy("event_date").count().orderBy("event_date").show(truncate=False)

print("== Duplicates across ALL days ==")
total, distinct = ev.count(), ev.select("event_id").distinct().count()
print(f"rows={total:,} distinct_event_ids={distinct:,} duplicates={total - distinct:,}")

print("== Null rate of key columns ==")
keys = ["event_id", "event_type", "event_ts", "actor_id", "actor_login", "repo_id", "repo_name"]
ev.select([F.round(F.avg(F.col(c).isNull().cast("int")), 6).alias(c) for c in keys]).show()

print("== File date vs event time mismatch rate ==")
(ev.groupBy("event_date")
   .agg(F.round(F.avg((F.to_date("event_ts") != F.col("event_date")).cast("int")), 6)
         .alias("mismatch_rate"))
   .orderBy("event_date").show())

print("== Bot share per day ==")
(ev.groupBy("event_date").agg(F.round(F.avg(F.col("is_bot").cast("int")), 4).alias("bot_share"))
   .orderBy("event_date").show())

print("== Concentration: share of events from the top 10 repos ==")
per_repo = ev.groupBy("event_date", "repo_id").agg(F.count("*").alias("n"))
w = Window.partitionBy("event_date").orderBy(F.desc("n"))
top10 = (per_repo.withColumn("rk", F.row_number().over(w)).filter("rk <= 10")
         .groupBy("event_date").agg(F.sum("n").alias("top10")))
tot = per_repo.groupBy("event_date").agg(F.sum("n").alias("total"))
(top10.join(tot, "event_date")
      .withColumn("top10_share", F.round(F.col("top10") / F.col("total"), 4))
      .orderBy("event_date").show())

print("== Event mix by era ==")
ev.groupBy("payload_era", "event_type").count().orderBy("payload_era", F.desc("count")).show(20, False)

print("== PR: era vs field presence (must agree) ==")
pr.groupBy("payload_era", "merged_known").count().show()

print("== PR: actions and outcomes ==")
pr.groupBy("payload_era", "pr_action", "pr_outcome").count() \
  .orderBy("payload_era", F.desc("count")).show(30, False)

print("== PR: language presence and parse failures ==")
(pr.groupBy("payload_era")
   .agg(F.round(F.avg(F.col("base_language").isNotNull().cast("int")), 4).alias("language_present"),
        F.sum(F.col("pr_action").isNull().cast("int")).alias("parse_failures")).show())

print("== Era B: does a 'merged' action also have a 'closed' event? ==")
b = pr.filter("payload_era = 'B_trimmed'")
keys2 = ["event_date", "repo_id", "pr_number"]
m = b.filter("pr_action = 'merged'").select(*keys2).distinct()
c = b.filter("pr_action = 'closed'").select(*keys2).distinct()
print(f"merged PRs={m.count():,} closed PRs={c.count():,} in_both={m.join(c, keys2).count():,}")

print("== Pushes: field presence by era ==")
(pu.groupBy("payload_era")
   .agg(F.count("*").alias("pushes"),
        F.round(F.avg(F.col("push_id").isNotNull().cast("int")), 4).alias("has_push_id"),
        F.round(F.avg(F.col("has_commit_counts").cast("int")), 4).alias("has_distinct_size")).show())

spark.stop()