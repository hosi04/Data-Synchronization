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

def create_mysql_trigger(connection, cursor):
    TRIGGER_FILE_PATH = Path("/home/ngocthanh/Prime/LearnMyself/DataEngineer/DE_ETL_MEET/data_synchronization_problem/sql/trigger.sql")
    DATABASE_NAME = "github_data"
    try:
        connection.database = DATABASE_NAME
        with open(TRIGGER_FILE_PATH, 'r') as sql_file:
            sql_script = sql_file.read()
            delimiter = "DELIMITER //"
            statements = sql_script.split(delimiter)
            for statement in statements:
                if statement.strip():
                    if "CREATE TRIGGER" in statement.upper():
                        cursor.execute("DELIMITER //")
                        trigger_sql = statement.split("DELIMITER ;")[0].strip()
                        cursor.execute(trigger_sql)
                        cursor.execute("DELIMITER ;")
                    else:
                        cursor.execute(statement)
                        connection.commit()
                        print("SQL file executed successfully!")
    except Error as e:
        connection.rollback()
        raise Exception(f"Failed to create trigger: {e}") from e

def validate_mysql_schema(cursor):
    # table has been existed?
    # record has been inserted?

    cursor.execute("SHOW TABLES")
    # print(cursor.fetchall())
    tables = [item[0] for item in cursor.fetchall()]
    # print(tables)
    if "users" and "repositories" not in tables:
        raise ValueError("---------------------Missing table-----------------------")

    cursor.execute("SELECT * FROM users WHERE user_id = 1")
    user = cursor.fetchone()
    if not user:
        raise ValueError("User not found")

# -----------------------------------------MONGO DB-----------------------------------------
def create_mongodb_schema(db):
    collections = db.list_collection_names()
    db.drop_collection('users')
    if "users" not in collections:
        db.create_collection("users", validator={
            "$jsonSchema": {
                # "bsonType": "object",
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
        db.users.create_index("user_id", unique = False)
        print("---------------------Create MongoDB Schema Successfully---------------------")
    else:
        print("---------------------Collection already exists---------------------")

def validate_mongodb_schema(db):
    collections = db.list_collection_names()
    if "users" not in collections:
        raise Exception("-----------------------Missing 'users' collection-----------------------")

# -----------------------------------------REDIS-----------------------------------------
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