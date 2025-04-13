from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from config.databaseConfig import get_database_config
from database.schemaManager import create_mongodb_schema, validate_mongodb_schema


# Step1: def(get mongo config)
# Step2: def(connect)
# Step3: def(disconnect)
# Step4: def(reconnect)
# Step5: def(exit)

class MongoDBConnect:
    def __init__(self, uri, database):
        self.uri = uri
        self.database = database
        self.client = None
        self.db = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self.client:
            self.client.close()

    def connect(self):
        try:
            self.client = MongoClient(self.uri)
            self.client.server_info() # To test connection to MongoDB
            self.db = self.client[self.database]
            if self.client.server_info():
                print("-----------------------MongoDB has been connected-----------------------")
            return self.db
        except ConnectionFailure as error:
            raise Exception(f"-----------------------Failed to connect to MongoDB: {error}-----------------------") from error

def main():
    config_mongo = get_database_config()
    with MongoDBConnect(config_mongo["mongodb"].uri, config_mongo["mongodb"].database) as mongo_client:
        # Connect to MongoDB with uri
        create_mongodb_schema(mongo_client.connect())

        # Insert data to table Users
        try:
            mongo_client.db.Users.insert_one({
                "user_id": 123,
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