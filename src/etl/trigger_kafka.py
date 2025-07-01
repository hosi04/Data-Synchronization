import time

from config.database_config import get_database_config
from database.mysql_connect import MySqlConnect
import json
from kafka import KafkaProducer

config = get_database_config()

def get_data_trigger(mysql_client, last_timestamp):
    try:
        connection, cursor = mysql_client.connection, mysql_client.cursor
        database = "github_data"
        connection.database = database

        query = ("SELECT user_id, login, gravatar_id, avatar_url, url, state,"
                       "DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s') "
                       "FROM users_log_after"
                 )

        if last_timestamp: # First Time Will Not Run Here
            query += "WHERE DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s.%f') > DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s.%f')"
            cursor.execute(query)
        else:
            cursor.execute(query)

        rows = cursor.fetchall() # Save result of query as type tuple()
        connection.commit()

        for row in rows:
            print(row)

        schema = ["user_id", "login", "gravatar_id", "avatar_url", "url", "state", "log_timestamp"]
        data = [dict(zip(schema, row)) for row in rows] # Data Example For Row (1, 'alice', 'g001'....)

        # get last_timestamp lastest
        new_timestamp = max((row["log_timestamp"] for row in data), default=last_timestamp) if data else last_timestamp
        return data, new_timestamp

    except Exception as e:
        print(f"----------Error as : {e}--------")
        return [], last_timestamp

def main():
    config = get_database_config()
    last_timestamp = None
    while True:
        with MySqlConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user,
                          config["mysql"].password) as mysql_client:

            # Send to kafka
            producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            while True:
                data, newest_timestamp = get_data_trigger(mysql_client, last_timestamp)
                last_timestamp = newest_timestamp

                for record in data:
                    time.sleep(0.5)
                    producer.send('thanhdepzai', record)
                    producer.flush()
                    print(record)

if __name__ == "__main__":
    main()