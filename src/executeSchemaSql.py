import mysql.connector

db_config = {
    "host": "127.0.0.1",
    "port": 3307,
    "user": "root",
    "password": 'U;TDRI05+58BP0.?i7Q*_0jThz6Epj%:'
}

# ** Will extract key-value in db_config into connection
connection = mysql.connector.connect(**db_config)
print("database connected")
database_name = "github_data"
sql_file_path = "E:\study\TU_HOC\DE\DE_ETL_MEET\data_synchronization_problem\src\schema.sql"
cursor = connection.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
print("database created")
connection.database = database_name

with open(sql_file_path, 'r') as file:
    sql_script = file.read()

sql_command = sql_script.split(';')
# print(sql_command)

for cmd in sql_command:
    if cmd.strip(): # Check if after strip() like "" => false, oppsite => true
        cursor.execute(cmd)
        print(f"executed sql command: {cmd}")
connection.commit()