from config.database_config import get_database_config
from database.mysql_connect import MySqlConnect
import json
from kafka import KafkaProducer
import time

config = get_database_config()


def get_data_trigger(mysql_client, last_timestamp):
    try:
        connection, cursor = mysql_client.connection, mysql_client.cursor
        database = "github_data"
        connection.database = database

        query = ("SELECT user_id, login, gravatar_id, avatar_url, url, state, "
                 " DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s.%f') AS log_timestamp "
                 "FROM users_log_after"
                 )

        if last_timestamp:  # First Time Will Not Run Here
            # Đảm bảo định dạng thời gian khớp với định dạng trong cơ sở dữ liệu
            query += " WHERE log_timestamp > STR_TO_DATE('{}', '%Y-%m-%d %H:%i:%s.%f')".format(last_timestamp)
            cursor.execute(query)
        else:
            cursor.execute(query)

        rows = cursor.fetchall()  # Save result of query as type tuple()
        # Không cần commit ở đây nếu đây chỉ là truy vấn SELECT
        connection.commit()

        schema = ["user_id", "login", "gravatar_id", "avatar_url", "url", "state", "log_timestamp"]
        data = [dict(zip(schema, row)) for row in rows]  # Data Example For Row (1, 'alice', 'g001'....)

        # get last_timestamp lastest
        newest_timestamp = max((row["log_timestamp"] for row in data),
                               default=last_timestamp) if data else last_timestamp
        return data, newest_timestamp

    except Exception as e:
        print(f"----------Error as : {e}--------")
        return [], last_timestamp


def main():
    config = get_database_config()
    last_timestamp = None
    # Khởi tạo Kafka Producer một lần duy nhất
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    while True:
        with MySqlConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user, config["mysql"].password) as mysql_client:
            while True:
                data, newest_timestamp = get_data_trigger(mysql_client, last_timestamp)
                last_timestamp = newest_timestamp

                if data:  # Chỉ gửi nếu có dữ liệu
                    for record in data:
                        producer.send('thanhdepzai', record) # Data will send into Buffer
                        print(record)
                    producer.flush()  # Gọi flush sau khi gửi tất cả các bản ghi trong lô, send data into broker
                time.sleep(1)  # Tránh lặp quá nhanh, kiểm tra định kỳ

if __name__ == "__main__":
    main()