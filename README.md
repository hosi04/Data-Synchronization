# 📦 Data Synchronization Project

📝 Project Description
This project aims to synchronize data across multiple databases. It simulates a scenario where MySQL acts as the primary data source, and all changes in MySQL are automatically propagated to MongoDB and Redis to ensure data consistency across systems.

A trigger is created in MySQL to capture data changes, which are then pushed to Apache Kafka. Apache Spark consumes these Kafka events and synchronizes the changes to MongoDB and Redis in real time.
