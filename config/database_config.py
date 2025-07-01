from dataclasses import dataclass
from typing import Dict, Optional
from dotenv import load_dotenv
import os

# SupperClass
class DatabaseConfig():
    def validate(self) -> None:
        for key, value in self.__dict__.items():
            if value is None:
                raise ValueError(f"-------------------Missing Value of key: {key}-------------------")

# Inheritance from Class DatabaseConfig
# SubClass
@dataclass
class MySqlConfig(DatabaseConfig):
    host: str
    port: int
    user: str
    password: str
    database: str
    jar_path: Optional[str] = None
    table: str = "users"

@dataclass
class MongoDBConfig(DatabaseConfig):
    uri: str
    database: str
    jar_path: Optional[str] = None
    collection: str = "users"

@dataclass
class RedisConfig(DatabaseConfig):
    host: str
    port: int
    user: str
    password: str
    database: str
    jar_path: Optional[str] = None
    key_column: str = "id"

def get_database_config() -> Dict[str, DatabaseConfig]:
    load_dotenv()
    config = {
        "mysql": MySqlConfig(
            host = os.getenv("MYSQL_HOST"),
            port = int(os.getenv("MYSQL_PORT")),
            user = os.getenv("MYSQL_USER"),
            password = os.getenv("MYSQL_PASSWORD"),
            database = os.getenv("MYSQL_DATABASE"),
            jar_path = os.getenv("MYSQL_JAR_PATH")
        ),
        "mongodb": MongoDBConfig(
            uri = os.getenv("MONGODBB_URI"),
            database = os.getenv("MONGODB_DATABASE"),
            jar_path = os.getenv("MONGO_PACKAGE_PATH")
        ),
        "redis": RedisConfig(
            host = os.getenv("REDIS_HOST"),
            user = os.getenv("REDIS_USER"),
            password = os.getenv("REDIS_PASSWORD"),
            port = int(os.getenv("REDIS_PORT")),
            database = os.getenv("REDIS_DATABASE"),
            jar_path = os.getenv("REDIS_JAR_PATH")
        )
    }
    for key,value in config.items():
        value.validate()

    return config