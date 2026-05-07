from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
)

KAFKA_BOOTSTRAP = "kafka:9092"
INPUT_TOPIC = "processed"
CHECKPOINT_DIR = "/tmp/checkpoints/stream_processed_to_cassandra"

KEYSPACE = "wiki_stream"
TABLE = "page_creates"

schema = StructType([
    StructField("user_id", LongType(), True),
    StructField("domain", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("page_title", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("WikiProcessedToCassandra")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.cassandra.connection.port", "9042")
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
    .na.drop(subset=["user_id", "domain", "created_at", "page_title"])
    .withColumn("created_at", to_timestamp(col("created_at")))
)

def write_to_cassandra(batch_df, batch_id):
    count = batch_df.count()
    print(f"=== foreachBatch called batch_id={batch_id}, count={count} ===", flush=True)
    if count == 0:
        return

    (
        batch_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table=TABLE, keyspace=KEYSPACE)
        .save()
    )

query = (
    parsed_df.writeStream
    .foreachBatch(write_to_cassandra)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .start()
)

query.awaitTermination()