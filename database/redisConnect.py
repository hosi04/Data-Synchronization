import redis
from config.databaseConfig import get_database_config

class redisConnect:

    def __init__(self, config):
        self.config = config
        self.client = None

    def connect(self):

        redis_config = {
            "host": self.config.host,
            # "username": self.config.user,
            "password": self.config.password,
            "port": self.config.port,
            # "db": self.config.database
        }

        self.client = redis.Redis(**redis_config)
        self.client.ping()
        print(f"---------------Connected to Redis! {self.client.ping()}---------------")

    def close(self):
        if self.client:
            self.client.close()
            print("---------------Closing Redis!---------------")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()
        print("---------------Close connection to Redis!---------------")