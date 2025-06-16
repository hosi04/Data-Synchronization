from pyspark.sql.functions import col, lit
from config.database_config import get_database_config
from config.spark_config import SparkConnect
from pyspark.sql.types import *
from spark_write_database import SparkWriteDatabase
from config.spark_config import get_spark_config
from src.spark import spark_write_database


def main():
    db_config = get_database_config()

    jar_packages = [
        db_config["mysql"].jar_path, # Use jar packages
        db_config["mongodb"].jar_path # Use jar packages
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

    df = spark_connect.spark.read.schema(schema_read_file).json(r"E:\study\TU_HOC\DE\DE_ETL_MEET\data_synchronization_problem\data\2015-03-01-17.json")

    df_write_database = df.withColumn('spark_temp', lit('sparkwriter')).select(
        col("actor.id").alias("user_id"),  # ép kiểu ở đây
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.url").alias("url"),
        col("actor.avatar_url").alias("avatar_url"),
        col('spark_temp').alias('spark_temp')
    )

    spark_config = get_spark_config()

    df_write = SparkWriteDatabase(spark_connect.spark, spark_config)
    df_write.spark_write_all_database(df_write_database, mode="append")

    df_validate = SparkWriteDatabase(spark_connect.spark, spark_config)
    df_validate.validate_spark_mysql(df_write_database, spark_config["mysql"]["table"], spark_config["mysql"]["jdbc_url"], spark_config["mysql"]["config"])

    spark_connect.stop()

if __name__ == "__main__":
    main()