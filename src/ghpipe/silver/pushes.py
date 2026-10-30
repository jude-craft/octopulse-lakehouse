from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ghpipe.schemas.payloads import PUSH_PAYLOAD_SCHEMA


def transform_pushes(df: DataFrame) -> DataFrame:
    """Input: deduped PushEvent rows with `payload`, is_bot and payload_era."""
    parsed = df.withColumn("_p", F.from_json(F.col("payload"), PUSH_PAYLOAD_SCHEMA))
    return parsed.select(
        "event_id", "event_ts", "event_date", "event_hour", "source_file", "ingest_ts",
        "repo_id", "actor_id", "is_bot", "payload_era",
        F.col("_p.push_id").alias("push_id"),
        F.col("_p.ref").alias("ref"),
        F.col("_p.head").alias("head_sha"),
        F.col("_p.size").alias("size"),
        F.col("_p.distinct_size").alias("distinct_size"),
        F.col("_p.distinct_size").isNotNull().alias("has_commit_counts"),
    )