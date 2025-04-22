# import redis
# from config.databaseConfig import get_database_config
#
# class redisConnect:
#
#
#     def connect(self):
#
#         redis_config = {
#             "host": self.config.host,
#             # "username": self.config.user,
#             "password": self.config.password,
#             "port": self.config.port,
#             # "db": self.config.database
#         }
#
#         self.client = redis.Redis(**redis_config)
#         self.client.ping() # Test connection?
#         print(f"---------------Connected to Redis! {self.client.ping()}---------------")
#
#     def close(self):
#         if self.client:
#             self.client.close()
#             print("---------------Closing Redis!---------------")
#
#     def __enter__(self):
#         self.connect()
#         return self
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.client.close()
#         print("---------------Close connection to Redis!---------------")
#     def __init__(self, config):
#         self.config = config
#         self.client = None

import redis
from redis.exceptions import ConnectionError

class redisConnect:
    def __init__(self, host, port, user, password, db):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.config = {
            "host": host,
            "port": port,
            "username": user,
            "password": password,
            "db": db
        }
        self.client = None

    def connect(self):
        try:
            self.client = redis.Redis(**self.config, decode_responses=True)
            self.client.ping() # To test connection
            print("-------------------------------Connected to Redis-------------------------------")
            return self.client
        except ConnectionError as e:
            raise Exception(f"-------------------------------Failed to connect to Redis! {e}-------------------------------")

    def close(self):
        if self.client:
            self.client.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
