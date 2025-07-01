from typing import Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from config.database_config import get_database_config
from config.spark_config import get_spark_config
from database.mongo_db_connect import MongoDBConnect
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

    def spark_validate_mysql(self, df_write: DataFrame, table_name: str, jdbc_url: str, config, mode: str = "append"):
        try:
            df_read = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("dbtable", f"(SELECT * FROM {table_name} WHERE spark_temp = 'sparkwrite') AS subq") \
                .option("user", config["user"]) \
                .option("password", config["password"]) \
                .load()

            if df_write.count() == df_read.count():
                print("--------------------------------Insert Data To MySQL Successfully (Not Missing Data)--------------------------------")
            else:
                df_missing = df_write.exceptAll(df_read)
                df_temp = df_missing
                df_missing.write \
                    .format("jsdbc") \
                    .option("url", jdbc_url) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .option("dbtable", table_name) \
                    .option("user", config["user"]) \
                    .option("password", config["password"]) \
                    .mode(mode) \
                    .save()
                print(f"--------------------------------Inserted {df_temp.count()} Missing Records Data Successfully--------------------------------")
        except Exception as e:
            raise Exception(f"--------------------------------Function validate_spark_mysql has been failed--------------------------------")
        finally:
            try:
                with MySqlConnect(config["host"], config["port"], config["user"], config["password"]) as mysql_client:
                    connection, cursor = mysql_client.connection, mysql_client.cursor
                    database = "github_data"
                    connection.database = database
                    cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN spark_temp")
                    connection.commit()
                    mysql_client.close()
            except Exception as e:
                raise Exception(f"--------------------------------Fall While Drop Column--------------------------------")

    def spark_write_mongodb(self, df: DataFrame, uri: str, database: str, collection: str, mode: str = "append"):
        df.write \
            .format("mongo") \
            .option("uri", uri) \
            .option("database", database) \
            .option("collection", collection) \
            .mode(mode) \
            .save()
        print(f"--------------------------------Spark Write Data To MongoDB Successfully--------------------------------")

    def spark_validate_mongodb(self, df_write: DataFrame, uri: str, database: str, collection: str, config, mode: str = "append"):
        query = {"spark_temp": "sparkwrite"}
        df_read = self.spark.read \
            .format("mongo") \
            .option("uri", uri) \
            .option("database", database) \
            .option("collection", collection) \
            .option("pipeline", str([{"$match": query}])) \
            .load()

        df_read = df_read.select(
            col("user_id"),
            col("login"),
            col("gravatar_id"),
            col("avatar_url"),
            col("url"),
            col("spark_temp")
        )

        if df_write.count() == df_read.count():
            print("--------------------------------Insert Data To MongoDB Successfully (Not Missing Data)--------------------------------")
        else:
            try:
                df_missing = df_write.exceptAll(df_read)
                df_temp = df_missing
                df_missing.write \
                    .format("mongo") \
                    .option("uri", uri) \
                    .option("database", database) \
                    .option("collection", collection) \
                    .mode(mode) \
                    .save()
                print(f"--------------------------------Inserted {df_temp.count()} Missing Records Data Successfully--------------------------------")
            except Exception as e:
                raise Exception(f"--------------------------------Error While Insert Missing Records To MongoDB: {e}--------------------------------")
        with MongoDBConnect(config["uri"], config["database"]) as mongo_client:
            mongo_client.db.users.update_many({},{"$unset": {"spark_temp": ""}})

    def spark_write_all_database(self, df: DataFrame):
        self.spark_write_mysql(
            df,
            self.spark_config["mysql"]["table"],
            self.spark_config["mysql"]["jdbc_url"],
            self.spark_config["mysql"]["config"]
        )

        self.spark_write_mongodb(
            df,
            self.spark_config["mongo"]["uri"],
            self.spark_config["mongo"]["database"],
            self.spark_config["mongo"]["collection"]
        )

    def spark_validate(self, df: DataFrame):
        self.spark_validate_mysql(
            df,
            self.spark_config["mysql"]["table"],
            self.spark_config["mysql"]["jdbc_url"],
            self.spark_config["mysql"]["config"]
        )

        self.spark_validate_mongodb(
            df,
            self.spark_config["mongo"]["uri"],
            self.spark_config["mongo"]["database"],
            self.spark_config["mongo"]["collection"],
            self.spark_config["mongo"]
        )