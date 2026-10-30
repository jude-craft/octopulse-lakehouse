from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ghpipe.schemas.payloads import PR_PAYLOAD_SCHEMA


def transform_pull_requests(df: DataFrame) -> DataFrame:
    """Parse the payload into small columns. source_file/ingest_ts stay so dedup can order by them."""
    parsed = df.withColumn("_p", F.from_json(F.col("payload"), PR_PAYLOAD_SCHEMA))

    action = F.col("_p.action")
    merged = F.col("_p.pull_request.merged")

    outcome = (
        F.when(action == "merged", F.lit("merged"))
         .when((action == "closed") & (merged == F.lit(True)), F.lit("merged"))
         .when((action == "closed") & (merged == F.lit(False)), F.lit("closed_unmerged"))
         .when(action == "closed", F.lit("closed_unmerged"))
    )

    return parsed.select(
        "event_id", "event_ts", "event_date", "event_hour", "source_file", "ingest_ts",
        "repo_id", "actor_id", "is_bot", "payload_era",
        action.alias("pr_action"),
        F.coalesce(F.col("_p.number"), F.col("_p.pull_request.number")).alias("pr_number"),
        F.col("_p.pull_request.id").alias("pr_id"),
        merged.alias("merged"),
        merged.isNotNull().alias("merged_known"),
        outcome.alias("pr_outcome"),
        ((action == "closed") & merged.isNull()).alias("outcome_inferred"),
        F.to_timestamp(F.col("_p.pull_request.created_at")).alias("pr_created_at"),
        F.to_timestamp(F.col("_p.pull_request.merged_at")).alias("merged_at"),
        F.col("_p.pull_request.base.repo.language").alias("base_language"),
    )