from enum import unique
from wsgiref.validate import validator


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