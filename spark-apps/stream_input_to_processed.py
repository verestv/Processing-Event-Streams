from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    LongType,
)

INPUT_TOPIC = "input"
OUTPUT_TOPIC = "processed"
KAFKA_BOOTSTRAP = "kafka:9092"
CHECKPOINT_DIR = "/tmp/checkpoints/stream_input_to_processed"

schema = StructType([
    StructField("meta", StructType([
        StructField("domain", StringType(), True),
        StructField("dt", StringType(), True),
    ]), True),
    StructField("page_title", StringType(), True),
    StructField("performer", StructType([
        StructField("user_is_bot", BooleanType(), True),
        StructField("user_id", LongType(), True),
    ]), True),
])

spark = (
    SparkSession.builder
    .appName("WikiInputToProcessed")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
)

filtered_df = (
    parsed_df
    .filter(col("meta.domain").isin(
        "en.wikipedia.org",
        "www.wikidata.org",
        "commons.wikimedia.org"
    ))
    .filter(col("performer.user_is_bot") == False)
)

processed_df = (
    filtered_df
    .select(
        col("performer.user_id").alias("user_id"),
        col("meta.domain").alias("domain"),
        col("meta.dt").alias("created_at"),
        col("page_title").alias("page_title")
    )
)

kafka_out_df = processed_df.select(
    to_json(
        struct(
            col("user_id"),
            col("domain"),
            col("created_at"),
            col("page_title")
        )
    ).alias("value")
)

query = (
    kafka_out_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", OUTPUT_TOPIC)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .outputMode("append")
    .start()
)

query.awaitTermination()