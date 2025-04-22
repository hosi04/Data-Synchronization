from config.databaseConfig import *
from database.mongoDbConnect import MongoDBConnect
from database.schemaManager import create_mongodb_schema, create_mysql_schema, validate_mysql_schema, create_redis_schema, validate_redis_schema
from database.mySqlConnect import mySqlConnect
from database.redisConnect import redisConnect

def main(config):


    # # MongoDB
    # with MongoDBConnect(config["mongodb"].uri, config["mongodb"].database) as mongo_client:
    #     # Connect to MongoDB with uri
    #     create_mongodb_schema(mongo_client.connect())
    #
    #     # Insert data to table Users
    #     try:
    #         mongo_client.db.Users.insert_one({
    #             "user_id": 1234,
    #             "login": "GoogleCodeExporter",
    #             "gravatar_id": "",
    #             "url": "https://www.google.com/accounts/o8/login",
    #             "avatar": "https://www.google.com/accounts/o8/avatar",
    #         })
    #         print("Successfully inserted user")
    #     except Exception as e:
    #         print(f"Error inserting data: {e}")

    # MySQL
    with mySqlConnect(config["mysql"]) as mysql_client:
        connection, cursor = mysql_client.connection, mysql_client.cursor
        create_mysql_schema(connection, cursor)
        cursor.execute("INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (%s, %s, %s, %s, %s)", (1, "GoogleCodeExporter", "", "https://www.google.com/accounts/o8/login", "https://www.google.com/accounts/o8/avatar"))
        connection.commit()
        validate_mysql_schema(cursor)

    # # Redis
    # with redisConnect(config["redis"].host, config["redis"].port, config["redis"].user, config["redis"].password, config["redis"].database) as redis_client:
    #     create_redis_schema(redis_client.connect())
    #     validate_redis_schema(redis_client.connect())

    # # Redis Hosi
    # with redisConnect(config["redis"]) as redis_client:
    #     redis_client.connect()
    #     try:
    #         create_redis_schema(redis_client.client)
    #     except Exception as e:
    #         print(f"Error inserting data: {e}")



if __name__ == "__main__":
    config = get_database_config()
    main(config)