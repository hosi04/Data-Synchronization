from pyspark.sql.functions import *
from pyspark.sql.types import *
from config.spark_config import SparkConnect
def main():

    jar_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.7.3",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
    ]

    spark = SparkConnect(
        app_name="thanhdz",
        master_url="local[*]",
        executor_cores=2,
        executor_memory="4g",
        driver_memory="2g",
        num_executors=3,
        jar_packages=jar_packages,
        log_level="WARN"
    ).spark

    schema_kafka = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("avatar_url", StringType(), True),
        StructField("url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("log_timestamp", StringType(), True)
    ])

    # ReadStream Data From Kafka Topic
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "thanhdepzai") \
        .option("startingOffsets", "earliest") \
        .load()

    df_property = df_kafka.select(col("value").cast("string"))

    # ^\": dấu " dau chuoi.
    # \"$: dấu " cuoi chuoi.
    df_property = df_property.withColumn("value", regexp_replace(col("value"), "\\\\", "")) \
        .withColumn("value", regexp_replace(col("value"), "^\"|\"$", ""))

    # ("data.*"): Bung toan bo data thanh cac cot doc lap
    df_property = df_property.select(from_json(col("value"), schema_kafka).alias("data")) \
        .select("data.*")

    mongo_stream = df_property.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "/home/ngocthanh/Prime/learn-myself/data-engineer/de-datdang/data-synchronization-problem/checkpoint") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .option('spark.mongodb.connection.uri', 'mongodb://thanhdepzai:thanhdepzaivailon@localhost:27017') \
        .option('spark.mongodb.database', 'github_data') \
        .option('spark.mongodb.collection', 'users') \
        .trigger(continuous="0.1 seconds") \
        .outputMode("append") \
        .start()

    mongo_stream.awaitTermination()

if __name__ == "__main__":
    main()
