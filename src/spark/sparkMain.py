from pyspark.sql.functions import col
from src.spark.sparkWriteDatabase import sparkWriteDatabase
from config.databaseConfig import get_database_config
from config.sparkConfig import sparkConnect
from pyspark.sql.types import *

def main():
    db_config = get_database_config()

    spark_conf = {
        # Tải JAR MySQL vào Spark session
        "spark.jars": (
            "file:///E:/study/TU_HOC/DE/DE_ETL_MEET/data_synchronization_problem/lib/mysql-connector-j-9.2.0.jar,"
            "file:///E:/study/TU_HOC/DE/DE_ETL_MEET/data_synchronization_problem/lib/mongo-spark-connector_2.12-10.2.0-all.jar"
        ),
        # Đảm bảo executors Spark có thể tìm thấy JDBC JAR
        "spark.driver.extraClassPath": (
            "E:/study/TU_HOC/DE/DE_ETL_MEET/data_synchronization_problem/lib/mysql-connector-j-9.2.0.jar;"
            "E:/study/TU_HOC/DE/DE_ETL_MEET/data_synchronization_problem/lib/mongo-spark-connector_2.12-10.2.0-all.jar"
        ),
        # MongoDB URI (dùng cho ghi dữ liệu)
        "spark.mongodb.output.uri": "mongodb://127.0.0.1/github_data.Users"
    }

    spark = sparkConnect(
        app_name="thanhdepzai",
        master_url="local[*]",
        executor_cores=2,
        executor_memory="4g",
        driver_memory="2g",
        num_executors=3,
        spark_conf=spark_conf,
        log_level="WARN"
    ).spark

    schema_mysql = StructType([
        StructField(name="actor", dataType=StructType([
            StructField(name="id", dataType=LongType(), nullable=False),
            StructField(name="login", dataType=StringType(), nullable=True),
            StructField(name="gravatar_id", dataType=StringType(), nullable=True),
            StructField(name="url", dataType=StringType(), nullable=True),
            StructField(name="avatar_url", dataType=StringType(), nullable=True),
        ]), nullable=True),
        StructField(name="repo", dataType=StructType([
            StructField(name="id", dataType=LongType(), nullable=False),
            StructField(name="name", dataType=StringType(), nullable=True),
            StructField(name="url", dataType=StringType(), nullable=True),
        ]), nullable=True),
    ])

    df = spark.read.schema(schema_mysql).json("../../data/2015-03-01-17.json")

    write_to_users = df.select(
        col("actor.id").alias("user_id"),
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.url").alias("url"),
        col("actor.avatar_url").alias("avatar_url"),
    )

    df_write_mysql = sparkWriteDatabase(spark, db_config)
    df_write_mysql.sparkWriteMySql(
        write_to_users,
        "Users",
        db_config["mysql"].jdbc_url,
        db_config["mysql"]
    )
    print("-------------------------Ghi dữ liệu vào MySQL thành công-------------------------")

    # writer = write_to_users.write
    # writer = writer.format("mongodb").mode("append")
    # writer.save()
    # print("-------------------------Ghi dữ liệu vào MongoDB thành công-------------------------")

if __name__ == "__main__":
    main()