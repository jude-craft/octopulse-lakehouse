from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ghpipe.gold.common import era_of_date


def build_pr_health_daily(pr: DataFrame) -> DataFrame:
    """Grain: one row per (event_date, repo_id) with at least one PR event."""
    a, o = F.col("pr_action"), F.col("pr_outcome")

    def count_if(cond):
        return F.sum(F.when(cond, 1).otherwise(0))

    g = pr.groupBy("event_date", "repo_id").agg(
        F.count("*").alias("pr_events"),
        count_if(a == "opened").alias("prs_opened"),
        count_if((a == "opened") & ~F.col("is_bot")).alias("prs_opened_by_humans"),
        count_if(o == "merged").alias("prs_merged"),
        count_if(o == "closed_unmerged").alias("prs_closed_unmerged"),
        count_if(o == "closed_unknown").alias("prs_closed_unknown"),
    )
    return (
        g.withColumn("prs_closed",
                     F.col("prs_merged") + F.col("prs_closed_unmerged") + F.col("prs_closed_unknown"))
         # NULL, not a guess, whenever any closed PR has unknown merge status
         .withColumn("merge_rate",
                     F.when((F.col("prs_closed_unknown") == 0) & (F.col("prs_closed") > 0),
                            F.col("prs_merged") / F.col("prs_closed")))
         .withColumn("payload_era", era_of_date())
    )