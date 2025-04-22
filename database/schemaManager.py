from pathlib import Path
from mysql.connector import Error

# -----------------------------------------MONGO DB-----------------------------------------
def create_mongodb_schema(db):
    collections = db.list_collection_names()
    if "Users" not in collections:
        # db.drop_collection('Users')
        db.create_collection("Users", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["user_id", "login"],
                "properties": {
                    "user_id": {
                        "bsonType": "int"
                    },
                    "login": {
                        "bsonType": "string"
                    },
                    "gravatar_id": {
                        "bsonType": ["string", "null"]
                    },
                    "avatar_url": {
                        "bsonType": ["string", "null"]
                    },
                    "url": {
                        "bsonType": ["string", "null"]
                    }
                }
            }
        })
        # Config primary key
        db.Users.create_index("user_id", unique = True)
    else:
        print("Collection already exists")

def validate_mongodb_schema(db):
    collections = db.list_collection_names()
    if "Users" not in collections:
        raise Exception("-----------------------Missing 'Users' collection-----------------------")

# -----------------------------------------MYSQL-----------------------------------------
def create_mysql_schema(connection, cursor):
    SQL_FILE_PATH = Path("../sql/schema.sql")
    DATABASE_NAME = "github_data"
    cursor.execute(f"DROP DATABASE IF EXISTS {DATABASE_NAME}")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

    try:
        connection.database = DATABASE_NAME
        with open(SQL_FILE_PATH, 'r') as sql_file:
            sql_script = sql_file.read()
            commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]
            for cmd in commands:
                cursor.execute(cmd)
            connection.commit()

    except Error as e:
        connection.rollback()
        raise Exception(f"Failed to create database schema: {e}") from e

def validate_mysql_schema(cursor):
    # table has been existed?
    # record has been inserted?

    cursor.execute("SHOW TABLES")
    # print(cursor.fetchall())
    tables = [item[0] for item in cursor.fetchall()]
    # print(tables)
    if "Users" and "Repositories" not in tables:
        raise ValueError("---------------------Missing table-----------------------")

    cursor.execute("SELECT * FROM Users WHERE user_id = 1")
    user = cursor.fetchone()
    if not user:
        raise ValueError("User not found")

# -----------------------------------------REDIS-----------------------------------------
# def create_redis_schema(redis_client):
#     redis_client.hset("user:1", mapping={
#         "user_id": 1,
#         "login": "GoogleCodeExporter",
#         "gravatar_id": "",
#         "avatar_url": "https://www.google.com/accounts/o8/avatar",
#         "url": "https://www.google.com/accounts/o8/login"
#     })
#
#     redis_client.hset("user:2", mapping={
#         "user_id": 2,
#         "login": "MicrosoftCodeExporter",
#         "gravatar_id": "",
#         "avatar_url": "https://www.google.com/accounts/o8/avatar",
#         "url": "https://www.google.com/accounts/o8/login"
#     })
#     redis_client.sadd("users", 1, 2)

def create_redis_schema(redis_client):
    try:
        redis_client.flushdb()

        redis_client.set("user:1:login","GoogleCodeExporter")
        redis_client.set("user:1:gravatar_id","")
        redis_client.set("user:1:avatar_url","https://www.google.com/accounts/o8/avatar")
        redis_client.set("user:1:url","https://www.google.com/accounts/o8/login")
        redis_client.sadd("user_id","user:1")

        print("--------------------Add data to redis successfully-----------------------")
    except Exception as e:
        raise Exception(f"--------------------Failed to add data to redis! {e}-------------------------") from e

def validate_redis_schema(redis_client):
    if not redis_client.get("user:1:login") == "GoogleCodeExporter":
        raise ValueError("--------------------Failed to add data to redis!-------------------------")

    if not redis_client.sismember("user_id", "user:1"):
        raise ValueError("--------------------User not set in Redis!-----------------------")