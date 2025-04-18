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