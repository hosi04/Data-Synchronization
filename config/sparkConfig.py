# This file has the function of ConfigSpark to create a SparkSession, after connect to MySQL

import os.path
from typing import Optional, List, Dict
from pyspark.sql import SparkSession
from config.databaseConfig import get_database_config

class sparkConnect():

    def __init__(
            self,
            app_name: str,
            master_url: str = "local[*]",
            executor_cores: Optional[int] = 2,
            executor_memory: Optional[str] = "4g",
            driver_memory: Optional[str] = "2g",
            num_executors: Optional[int] = 3,
            jars: Optional[List[str]] = None,
            spark_conf: Optional[Dict[str, str]] = None,
            log_level: str = "WARM"
    ):
        self.app_name = app_name
        # **********
        self.spark = self.create_spark_session(master_url, executor_cores, executor_memory, driver_memory, num_executors, jars, spark_conf, log_level)

    def create_spark_session(
            # app_name: str,
            self,
            master_url: str = "local[*]",
            executor_cores: Optional[int] = 2,
            executor_memory: Optional[str] = "4g",
            driver_memory: Optional[str] = "2g",
            num_executors: Optional[int] = 3,
            jars: Optional[List[str]] = None,
            spark_conf: Optional[Dict[str, str]] = None,
            log_level: str = "WARM"
    ) -> SparkSession:

        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(master_url)

        if executor_memory:
            builder.config("spark.executor.memory", executor_memory)
        if executor_cores:
            builder.config("spark.executor.cores", executor_cores)
        if driver_memory:
            builder.config("spark.driver.memory", driver_memory)
        if num_executors:
            builder.config("spark.executor.instances", num_executors)


        if jars:
            jars_path = ",".join([os.path.abspath(jar) for jar in jars])
            builder.config("spark.jars", jars_path)

        # {"spark.sql.shuffle.partitions"" 10}
        if spark_conf:
            for key, value in spark_conf.items():
                builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)
        return spark

    def stop(self):
        if self.spark:
            self.spark.stop()
            print("-------------Stop SparkSession-------------")

    # spark = create_spark_session(
    #         app_name = "datdz",
    #         master_url = "local[*]",
    #         executor_cores = 2,
    #         executor_memory = "4g",
    #         driver_memory = "2g",
    #         num_executors = 3,
    #         jars = None,
    #         spark_conf = {"spark.sql.shuffle.partitions": "10"},
    #         log_level = "INFO"
    # )

    def connect_to_mysql(spark: SparkSession, config: Dict[str, str], table_name: str):
        df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:mysql://127.0.0.1:3307/github_data") \
            .option("dbtable", table_name) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        return df

    # jar_path = "..\lib\mysql-connector-j-9.2.0.jar"
    #
    # # Init var Spark with function CreateSparkSession
    # spark = create_spark_session(
    #         app_name = "thanhdepzai",
    #         master_url = "local[*]",
    #         executor_memory = "4g",
    #         jars = [jar_path],
    #         log_level = "INFO"
    # )
    #
    # db_config = get_database_config()
    # mysql_table = "Repositories"
    #
    # df = connect_to_mysql(spark, db_config, mysql_table)
    # df.printSchema()
