from config.databaseConfig import *
from database.mongoDbConnect import MongoDBConnect
from database.schemaManager import create_mongodb_schema


def main():
    config_mongo = get_database_config()
    with MongoDBConnect(config_mongo["mongodb"].uri, config_mongo["mongodb"].database) as mongo_client:
        # Connect to MongoDB with uri
        create_mongodb_schema(mongo_client.connect())

        # Insert data to table Users
        try:
            mongo_client.db.Users.insert_one({
                "user_id": 1234,
                "login": "GoogleCodeExporter",
                "gravatar_id": "",
                "url": "https://www.google.com/accounts/o8/login",
                "avatar": "https://www.google.com/accounts/o8/avatar",
            })
            print("Successfully inserted user")
        except Exception as e:
            print(f"Error inserting data: {e}")

if __name__ == "__main__":
    main()