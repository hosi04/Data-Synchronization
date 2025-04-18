from enum import unique
from pathlib import Path
from wsgiref.validate import validator
from mysql.connector import Error


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
