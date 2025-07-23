# 📦 Data Synchronization Project

📝 Project Description
This project aims to synchronize data across multiple databases. It simulates a scenario where MySQL acts as the primary data source, and all changes in MySQL are automatically propagated to MongoDB and Redis to ensure data consistency across systems.

A trigger is created in MySQL to capture data changes, which are then pushed to Apache Kafka. Apache Spark consumes these Kafka events and synchronizes the changes to MongoDB and Redis in real time.


📂 Project Structure
data-synchronization-problem/
├── .env # Environment variables
├── .gitignore # Git ignore config
├── checkpoint/ # Spark checkpoint directory
├── config/ # Configuration files
│ ├── database_config.py
│ └── spark_config.py
├── data/ # (Optional) Input/output data
├── database/ # DB connection and schema utilities
│ ├── mongo_db_connect.py
│ ├── mysql_connect.py
│ ├── redis_connect.py
│ └── schema_manager.py
├── lib/ # Static resources
│ └── describe_project.jpg
├── sql/ # SQL scripts (optional)
├── src/ # Main source code
│ ├── main.py
│ ├── etl/ # ETL & sync logic
│ │ ├── consumer.py
│ │ ├── sync_data_mongo.py
│ │ ├── sync_data_with_mongo.py
│ │ └── trigger_kafka.py
│ └── spark/ # Spark jobs
│ ├── spark_main.py
│ └── spark_write_database.py
├── redis.acl # Redis ACL config
├── requirements.txt # Python dependencies
└── README.md # Project documentation
