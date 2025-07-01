from config.database_config import get_database_config
from database.mongo_db_connect import MongoDBConnect
from database.mysql_connect import MySqlConnect
from database.schema_manager import create_mongodb_schema, validate_mongodb_schema
from database.schema_manager import create_mysql_schema


def main(config):
    with MySqlConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user, config["mysql"].password) as mysql_client:
        connection, cursor = mysql_client.connection, mysql_client.cursor
        create_mysql_schema(connection, cursor)
        cursor.execute("INSERT INTO users (user_id, login, gravatar_id, url, avatar_url) VALUES (%s, %s, %s, %s, %s)", (1, "NgocThanh", "", "https://api.github.com/users/GoogleCodeExporter", "https://avatars.githubusercontent.com/u/9614759?"))
        connection.commit()
        print(f"-----------Inserted to table Users-----------")

    with MongoDBConnect(config["mongodb"].uri, config["mongodb"].database) as mongo_client:
        create_mongodb_schema(mongo_client.connect())
        mongo_client.db.users.insert_one({
            "user_id":1,
            "login":"GoogleCodeExporter",
            "gravatar_id":"",
            "url":"https://api.github.com/users/GoogleCodeExporter",
            "avatar_url":"https://avatars.githubusercontent.com/u/1?"
        })
        validate_mongodb_schema(mongo_client.connect())

if __name__ == '__main__':
    config = get_database_config()
    main(config)