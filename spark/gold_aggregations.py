from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    avg,
    count,
    expr,
    to_date
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    IntegerType
)

silver_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_ts", TimestampType(), False),
    StructField("ride_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("fare", DoubleType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("event_date", StringType(), False),
    StructField("processed_time", TimestampType(), False)
])

spark = (
    SparkSession.builder
    .appName("rides-gold-aggregations")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

silver_df = (
    spark.readStream
    .format("parquet")
    .schema(silver_schema)
    .load("data/silver/rides")
)

completed_rides = (
    silver_df
    .filter(col("event_type") == "ride_completed")
)

windowed_metrics = (
    completed_rides
    .withWatermark("event_ts", "2 hours")
    .groupBy(
        window(col("event_ts"), "5 minutes"),
        col("city")
    )
    .agg(
        count("*").alias("rides_completed"),
        avg("fare").alias("avg_fare"),
        expr("percentile_approx(duration_seconds, 0.95)").alias("p95_duration_seconds")
    )
)

gold_df = (
    windowed_metrics
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("city"),
        col("rides_completed"),
        col("avg_fare"),
        col("p95_duration_seconds"),
        to_date(col("window.start")).alias("event_date")
    )
)

query = (
    gold_df
    .writeStream
    .format("parquet")
    .option("path", "data/gold/rides_city_metrics")
    .option("checkpointLocation", "checkpoints/gold/rides_city_metrics")
    .partitionBy("event_date")
    .outputMode("append")
    .start()
)

query.awaitTermination()
