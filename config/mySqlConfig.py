from dotenv import load_dotenv
import os
from urllib.parse import urlparse

def get_info_db_config():
    # Load cac gia tri trong file env
    load_dotenv()

    jdbc_url = os.getenv('DB_URL')

    parser_url = urlparse(jdbc_url.replace("jdbc:", "",1))
    host = parser_url.hostname
    port = parser_url.port
    database = parser_url.path[1:]
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
    }

info_config = get_info_db_config()
print(info_config)


