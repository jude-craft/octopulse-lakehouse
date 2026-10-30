from pyspark.sql import SparkSession

def get_spark(app_name: str, cores:int = 4, driver_memory:str = "6g", 
              shuffle_partitions: int = 16) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(f"local[{cores}]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark