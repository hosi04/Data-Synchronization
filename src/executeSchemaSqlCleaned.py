import mysql.connector
from config.mySqlConfig import get_info_db_config
from pathlib import Path
from mysql.connector import Error

def connect_mysql(config):

    try:
        connection = mysql.connector.connect(**config)
        return connection
    except Error as err:
        raise Exception(f"------------{err}------------") from err

def create_database(cursor, database_name):
    if cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}"):
        print("Database created")

def execute_schema_sql(cursor, sql_file_path):
    with open(sql_file_path, "r") as file:
        sql_script = file.read()
    cmds = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]

    for cmd in cmds:
        try:
            cursor.execute(cmd)
            print(f"executed sql command: {cmd}")
        except Error as err:
            print(f"------------Can't execute command because: {err}------------")

def main():
    try:
        # Get info config
        info_config = get_info_db_config()
        info_config_not_database = {k : v for k, v in info_config.items() if k != 'database'}

        # Connect database
        connection = connect_mysql(info_config_not_database)
        cursor = connection.cursor()

        # Create database
        create_database(cursor, info_config["database"])
        connection.database = info_config["database"]

        # Execute schemaSql
        sql_file_path = "E:\study\TU_HOC\DE\DE_ETL_MEET\data_synchronization_problem\src\schema.sql"
        execute_schema_sql(cursor, sql_file_path)
        connection.commit()
        print(f"------------File schemaSql has been executed sucessfully------------")
    except Error as err:
        print(f"------------{err}------------")
        if (connection and connection.is_connected()):
            connection.rollback() # Hoan tac tat ca nhung thuc thi chua duoc commit => Neu o tren try co dang chay ma ERROR thi xuong day no se rollback lai de chuong trinh khong bi ERROR
    finally:
        if cursor:
            cursor.close()
        if (connection and connection.is_connected()):
            connection.close()
            print(f"------------Disconnect from MySQL------------")

if __name__ == "__main__":
    main()