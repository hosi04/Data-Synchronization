from pyspark.sql.functions import col, lit
from pyspark.sql.types import *

from config.database_config import get_database_config
from config.spark_config import SparkConnect
from config.spark_config import get_spark_config
from spark_write_database import SparkWriteDatabase


def main():
    db_config = get_database_config()

    jar_packages = [
        db_config["mysql"].jar_path,
        db_config["mongodb"].jar_path
    ]

    spark_connect = SparkConnect(
        app_name="thanhdz",
        master_url="local[*]",
        executor_cores=2,
        executor_memory="4g",
        driver_memory="2g",
        num_executors=3,
        jar_packages=jar_packages,
        log_level="WARN"
    )

    schema_read_file = StructType([
        StructField(name="actor", dataType=StructType([
            StructField(name="id", dataType=IntegerType(), nullable=False),
            StructField(name="login", dataType=StringType(), nullable=True),
            StructField(name="gravatar_id", dataType=StringType(), nullable=True),
            StructField(name="url", dataType=StringType(), nullable=True),
            StructField(name="avatar_url", dataType=StringType(), nullable=True)
        ]), nullable=True),
        StructField(name="repo", dataType=StructType([
            StructField(name="id", dataType=LongType(), nullable=False),
            StructField(name="name", dataType=StringType(), nullable=True),
            StructField(name="url", dataType=StringType(), nullable=True)
        ]), nullable=True)
    ])

    df = spark_connect.spark.read.schema(schema_read_file).json("/home/ngocthanh/Prime/learn-myself/data-engineer/de-datdang/data-synchronization-problem/data/2015-03-01-17.json")

    df_write_database = df.withColumn('spark_temp', lit('sparkwrite')).select(
        col("actor.id").alias("user_id"),  # ép kiểu ở đây
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.avatar_url").alias("avatar_url"),
        col("actor.url").alias("url"),
        col('spark_temp').alias('spark_temp')
    )

    spark_config = get_spark_config()

    df_write = SparkWriteDatabase(spark_connect.spark, spark_config)
    df_write.spark_write_all_database(df_write_database)

    df_validate = SparkWriteDatabase(spark_connect.spark, spark_config)
    spark_connect.stop()

    # config = get_spark_config()
    # with MySqlConnect(config["mysql"]["config"]["host"], config["mysql"]["config"]["port"], config["mysql"]["config"]["user"], config["mysql"]["config"]["password"]) as mysql_client:
    #     connection, cursor = mysql_client.connection, mysql_client.cursor
    #     create_mysql_trigger(connection, cursor)

if __name__ == "__main__":
    main()