import mysql.connector
from mysql.connector import Error

class MySqlConnect:
    # def __init__(self, config):
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self):
        config = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password
        }
        try:
            self.connection = mysql.connector.connect(**config)
            self.cursor = self.connection.cursor()
            print("---------------------Connected to MySQL database---------------------")
            return self.connection, self.cursor
        except Error as error:
            raise Exception(f"---------------------Failed to connect to MySQL database: {error}---------------------")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        print("---------------------MYSQL HAS BEEN CLOSE---------------------")