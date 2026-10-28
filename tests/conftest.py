import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("pytest-ghpipe")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled","false")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .getOrCreate())
    yield s
    s.stop()