from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_date
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    MapType
)

# Define the schema for the ride events
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("ride_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("payload", MapType(StringType(), StringType()), True)
])

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("rides-bronze-stream")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka topic 'rides.events.raw'
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "rides.events.raw")
    .option("startingOffsets", "earliest")
    .load()
)

# Parse the Kafka value as JSON using the defined schema
parsed_df = (
    kafka_df
    .select(
        col("key").cast("string").alias("kafka_key"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), event_schema).alias("event")
    )
)

# Transform and enrich the data for the bronze layer
bronze_df = (
    parsed_df
    .select(
        col("event.event_id"),
        col("event.event_type"),
        col("event.event_time"),
        col("event.ride_id"),
        col("event.user_id"),
        col("event.driver_id"),
        col("event.city"),
        col("event.payload"),
        current_timestamp().alias("ingest_time"),
        col("kafka_partition"),
        col("kafka_offset"),
        to_date(col("event.event_time")).alias("event_date")
    )
)

# Write the bronze data to Parquet files, partitioned by event_date
query = (
    bronze_df
    .writeStream
    .format("parquet")
    .option("path", "data/bronze/rides")
    .option("checkpointLocation", "checkpoints/bronze/rides")
    .partitionBy("event_date")
    .outputMode("append")
    .start()
)

query.awaitTermination()
