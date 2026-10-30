from pyspark.sql.types import (BooleanType, IntegerType, LongType, 
                         StringType, StructField, StructType)

PR_PAYLOAD_SCHEMA = StructType([
    StructField("action", StringType()),
    StructField("number", LongType()),
    StructField("pull_request", StructType([
        StructField("id", LongType()),
        StructField("number", LongType()),
        StructField("merged", BooleanType()),
        StructField("merged_at", StringType()),
        StructField("created_at", StringType()),
        StructField("base", StructType([
            StructField("repo", StructType([
                StructField("language", StringType()),
            ])),
        ])),
    ])),
])

PUSH_PAYLOAD_SCHEMA = StructType([
    StructField("push_id", LongType()),
    StructField("ref", StringType()),
    StructField("head", StringType()),
    StructField("size", IntegerType()),
    StructField("distinct_size", IntegerType()),
])