from pyspark.sql.functions import *
from pyspark.sql.types import *

from config.database_config import get_database_config
from config.spark_config import SparkConnect
from pymongo import MongoClient
from config.spark_config import get_spark_config

# Hàm xử lý từng micro-batch
def process_batch_to_mongodb(df_batch, batch_id):
    if not df_batch.isEmpty():
        spark_conf = get_spark_config()
        mongo_uri = spark_conf["mongo"]["uri"]
        mongo_db = spark_conf["mongo"]["database"]
        mongo_collection_name = spark_conf["mongo"]["collection"]

        # Sử dụng foreachPartition để tối ưu kết nối (Khong tao connect nhieu lan!) MongoDB
        df_batch.foreachPartition(lambda records: write_partition_to_mongodb(records, mongo_uri, mongo_db, mongo_collection_name))

# Hàm ghi dữ liệu của từng partition vào MongoDB
def write_partition_to_mongodb(records, mongo_uri, mongo_db, mongo_collection_name):
    client = None
    try:
        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        collection = db[mongo_collection_name]

        for record in records:
            data_dict = record.asDict()
            user_id = data_dict["user_id"]
            state = data_dict["state"]

            # Chuẩn bị dữ liệu để ghi vào MongoDB
            document_to_write = {
                "user_id": data_dict["user_id"],
                "login": data_dict["login"],
                "gravatar_id": data_dict["gravatar_id"],
                "avatar_url": data_dict["avatar_url"],
                "url": data_dict["url"]
            }

            if state == "INSERT":
                # Chèn mới. Nếu user_id đã tồn tại, có thể xảy ra lỗi DuplicateKeyError
                # Nếu bạn muốn hành vi "upsert" (chèn nếu không có, cập nhật nếu có) cho INSERT,
                # bạn có thể dùng update_one với upsert=True thay vì insert_one.
                try:
                    collection.insert_one(document_to_write)
                except Exception as e:
                    # Xử lý trường hợp trùng lặp (nếu insert_one bị gọi lại cho bản ghi đã tồn tại)
                    print(f"Lỗi khi chèn user_id {user_id} (có thể đã tồn tại): {e}")

            elif state == "UPDATE":
                # Cập nhật bản ghi dựa trên user_id. upsert=True sẽ chèn nếu không tìm thấy.
                collection.update_one({"user_id": user_id}, {"$set": document_to_write}, upsert=True)

            elif state == "DELETE":
                # Xóa bản ghi dựa trên user_id
                collection.delete_one({"user_id": user_id})

            else:
                print(f"Trạng thái không xác định: {state} cho user_id: {user_id}")

    except Exception as e:
        print(f"Lỗi khi xử lý partition: {e}")
    finally:
        if client:
            client.close()

def main():
    database_config = get_database_config()
    jar_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.7.3",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
    ]

    spark_conf = {
        "spark.mongodb.connection.uri": "{}".format(database_config["mongodb"].uri),
        "spark.mongodb.database": "{}".format(database_config["mongodb"].database),
        "spark.mongodb.collection": "users"
    }

    # Khởi tạo SparkSession với cấu hình MongoDB
    spark = SparkConnect(
        app_name="thanhdz",
        master_url="local[*]",
        executor_cores=2,
        executor_memory="4g",
        driver_memory="2g",
        num_executors=3,
        jar_packages=jar_packages,
        spark_conf=spark_conf,
        log_level="WARN"
    ).spark

    # # Thiết lập các tùy chọn MongoDB trực tiếp trong SparkSession
    # spark.conf.set('spark.mongodb.connection.uri', 'mongodb://thanhdepzai:thanhdepzaivailon@localhost:27017')
    # spark.conf.set('spark.mongodb.database', 'github_data')
    # spark.conf.set('spark.mongodb.collection', 'users')

    schemaKafka = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("avatar_url", StringType(), True),
        StructField("url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("log_timestamp", StringType(), True)
    ])

    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "thanhdepzai") \
        .option("startingOffsets", "earliest") \
        .load()

    df_property = df_kafka.select(col("value").cast("string"))

    df_property = df_property.withColumn("value", regexp_replace(col("value"), "\\\\", "")) \
        .withColumn("value", regexp_replace(col("value"), "^\"|\"$", ""))

    df_property = df_property.select(from_json(col("value"), schemaKafka).alias("data")) \
        .select("data.*")

    # Sử dụng foreachBatch để xử lý từng loại thao tác
    mongo_stream = df_property.writeStream \
        .option("checkpointLocation", "/home/ngocthanh/Prime/learn-myself/data-engineer/de-datdang/data-synchronization-problem/checkpoint") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .trigger(processingTime="1 second") \
        .foreachBatch(process_batch_to_mongodb) \
        .start()

    mongo_stream.awaitTermination()

if __name__ == "__main__":
    main()