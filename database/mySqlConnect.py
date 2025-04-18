import mysql.connector
from mysql.connector import Error

class mySqlConnect:
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.cursor = None

    def connect(self):
        config = {
            "host": self.config.host,
            "port": self.config.port,
            "user": self.config.user,
            "password": self.config.password,
            # "database": self.config.database,
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