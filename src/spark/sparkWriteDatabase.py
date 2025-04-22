from typing import Dict
from database.mySqlConnect import mySqlConnect

from pyspark.sql import DataFrame, SparkSession


class sparkWriteDatabase:

    def __init__(self, spark: SparkSession, db_config: Dict):
        self.spark = spark
        self.db_conf = db_config

    def sparkWriteMySql(self, df: DataFrame, table_name: str, jdbc_url: str, config, mode: str = "append"):
        try:
            mysql_client = mySqlConnect(config)
            mysql_client.connect()
            mysql_client.close()
        except Exception as e:
            raise Exception (f"--------------------Failed to connect to MySQL database: {e}------------------------")

        df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", config.user) \
        .option("password", config.password) \
        .mode(mode) \
        .save()