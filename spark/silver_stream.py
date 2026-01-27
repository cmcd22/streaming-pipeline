from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp,
    to_date
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    MapType,
    DoubleType,
    IntegerType
)

bronze_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("ride_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("payload", MapType(StringType(), StringType()), True),
    StructField("ingest_time", TimestampType(), True),
    StructField("kafka_partition", LongType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("event_date", StringType(), True)
])

event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("ride_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("payload", StructType([
        StructField("fare", DoubleType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("distance_km", DoubleType(), True)
    ]), True)
])

spark = (
    SparkSession.builder
    .appName("rides-silver-stream")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

bronze_df = (
    spark.readStream
    .format("parquet")
    .schema(bronze_schema)
    .load("data/bronze/rides")
)

silver_parsed = (
    bronze_df
    .withColumn("event_ts", to_timestamp(col("event_time")))
    .filter(col("event_ts").isNotNull())
)

silver_deduped = (
    silver_parsed
    .withWatermark("event_ts", "2 hours")
    .dropDuplicates(["event_id"])
)

silver_df = (
    silver_deduped
    .select(
        col("event_id"),
        col("event_type"),
        col("event_ts"),
        col("ride_id"),
        col("user_id"),
        col("driver_id"),
        col("city"),
        col("payload.fare"),
        col("payload.duration_seconds"),
        col("payload.distance_km"),
        to_date(col("event_ts")).alias("event_date"),
        current_timestamp().alias("processed_time")
    )
)

query = (
    silver_df
    .writeStream
    .format("parquet")
    .option("path", "data/silver/rides")
    .option("checkpointLocation", "checkpoints/silver/rides")
    .partitionBy("event_date")
    .outputMode("append")
    .start()
)

query.awaitTermination()
