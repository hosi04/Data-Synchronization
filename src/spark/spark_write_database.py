from typing import Dict
from pyspark.sql import DataFrame, SparkSession

from database.mysql_connect import MySqlConnect


class SparkWriteDatabase:
    def __init__(self, spark: SparkSession, spark_config: Dict):
        self.spark = spark
        self.spark_config = spark_config

    def spark_write_mysql(self, df: DataFrame, table_name: str, jdbc_url: str, config, mode: str = "append"):
        try:
            with MySqlConnect(config["host"], config["port"], config["user"], config["password"]) as mysql_client:
                connection, cursor = mysql_client.connection, mysql_client.cursor
                database = "github_data"
                connection.database = database
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN spark_temp VARCHAR(255)")
                connection.commit()
                mysql_client.close()
        except Exception as e:
            raise Exception(f"--------------------------------Fall while add column--------------------------------")

        df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", config["user"]) \
        .option("password", config["password"]) \
        .mode(mode) \
        .save()
        print(f"--------------------------------Spark Write Data To MySQL Successfully--------------------------------")

    def spark_write_mongodb(self, df: DataFrame, uri: str, database: str, collection: str, mode: str = "append"):
        df.write \
            .format("mongo") \
            .option("uri", uri) \
            .option("database", database) \
            .option("collection", collection) \
            .mode(mode) \
            .save()
        print(f"--------------------------------Spark Write Data To MongoDB Successfully--------------------------------")

    def spark_write_all_database(self, df: DataFrame, mode: str = "append"):
        self.spark_write_mysql(
            df,
            self.spark_config["mysql"]["table"],
            self.spark_config["mysql"]["jdbc_url"],
            self.spark_config["mysql"]["config"],
            mode
        )

        self.spark_write_mongodb(
            df,
            self.spark_config["mongo"]["uri"],
            self.spark_config["mongo"]["database"],
            self.spark_config["mongo"]["collection"],
            mode
        )