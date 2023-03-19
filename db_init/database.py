from clickhouse_driver import Client

from typing import Optional, List

from instagramstories import settings
from mysql.connector import connect, Error

from instagramstories.logs.logs_config import LoggerHandle

log = LoggerHandle()
log.logger_config()


def db_decorator(func):
    def wrapper(*args, **kwargs):
        con = connect(
            host=settings.social_services_db['host'],
            user=settings.social_services_db['user'],
            password=settings.social_services_db['password']
        )
        try:
            result = func(*args, connection=con, **kwargs)
        except Error as e:
            log.logger.warning(e, 'SQL Failed!')
            print(e, 'SQL Failed!')
        else:
            con.commit()
            return result
        finally:
            con.close()

    return wrapper


class MariaDataBase:
    def __init__(self, db_name):
        self.db_name = db_name

    @db_decorator
    def get_data_for_parse(self,  table_name, quantity=500, _type: int = 4, stability: int = 1, worker: int = 4, *args, **kwargs):
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f'USE {self.db_name}')
        cursor.execute(f"SELECT screen_name FROM {table_name} WHERE type = {_type} AND stability = {stability} AND worker = {worker} ORDER BY RAND() LIMIT {quantity}")

        return [item for item in cursor.fetchall()]

    @db_decorator
    def send_to_table(self, table_name: str, columns: tuple, collection: list, *args, **kwargs) -> None:
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        insert = '%s,' * len(columns)
        cursor.execute(f"USE {self.db_name}")
        if len(collection) > 1:
            cursor.executemany(f"INSERT INTO {table_name} ({', '.join([col for col in columns])}) VALUES ({insert[:-1]})", collection)
        else:
            cursor.execute(f"INSERT INTO {table_name} ({', '.join([col for col in columns])}) VALUES ({insert[:-1]})", collection)

    @db_decorator
    def get_account_id(self, *args, **kwargs):
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT id FROM resource_social WHERE screen_name = "{args[0]}";')

        return min([item[0] for item in cursor.fetchall()])

    @db_decorator
    def get_account_credentials(self, table_name: str, soc_type: int = 4, _type: str = 'INST_STORY_PARSER', work: int = 1, limit: int = 5, *args,  **kwargs) -> Optional[List]:
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT login, password FROM {table_name} WHERE soc_type = {soc_type} AND type = "{_type}" AND work = {work} ORDER BY RAND() LIMIT {limit};')

        return [item for item in cursor.fetchall()]

    @db_decorator
    def mark_account_as_used(self, table_name: str, login: str, work: int = 0, _type: str = 'INST_STORY_PARSER', *args, **kwargs) -> None:
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'UPDATE {table_name} SET work = {work} WHERE type = "{_type}" AND login = "{login}";')

    @db_decorator
    def get_proxies(self, table_name: str, script: str = 'insta_story', limit: int = 5, *args, **kwargs):
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT proxy, port, login, password FROM {table_name} WHERE script = "{script}" ORDER BY RAND() LIMIT {limit};')

        return [item for item in cursor.fetchall()]

    @db_decorator
    def get_new_yadisk_token(self, table_name: str, work: int = 1, limit: int = 1, *args, **kwargs) -> str:
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT token FROM {table_name} WHERE work = {work} ORDER BY RAND() LIMIT {limit}')

        return cursor.fetchone()[0]

    @db_decorator
    def update_status_of_yadisk_token(self, table_name: str, token: str, *args, **kwargs) -> None:
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f'USE {self.db_name}')
        cursor.execute(f'UPDATE {table_name} SET work = 0 WHERE token = "{token}"')

    @db_decorator
    def get_yandex_disk_capacity(self, table_name: str, token: str, *args, **kwargs) -> int:
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT space_used FROM {table_name} WHERE token = "{token}"')

        return cursor.fetchone()[0]

    @db_decorator
    def update_yandex_disk_capacity(self, table_name: str, space_used: int, token: str, *args, **kwargs) -> None:
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'UPDATE {table_name} SET space_used = {space_used} WHERE token = "{token}"')



class ClickHouseDatabase:
    def __init__(self, db_name: str, host: str, port: int, username: str, password: str):
        self.db_name = db_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.connection = Client(host=self.host, user=self.username, port=self.port, password=self.password)

    def get_data_for_parse(self,  table_name, quantity=500, _type: int = 4, stability: int = 1, worker: int = 4):
        return self.connection.execute(f"SELECT screen_name FROM {self.db_name}.{table_name} WHERE type = {_type} AND stability = {stability} AND worker = {worker} ORDER BY RAND() LIMIT {quantity}")

    def create_db(self):
        self.connection.execute(f'CREATE DATABASE {self.db_name}')

    def create_table(self, *args, **kwargs):
        self.connection.execute(f"CREATE TABLE IF NOT EXISTS {args[0]} ({', '.join([arg for arg in args[1:]])}) ENGINE = MergeTree() ORDER BY account_id")

    def send_to_table(self, table_name: str, collection: [tuple, list]):
        self.connection.execute(f"INSERT INTO {self.db_name}.{table_name} (account_id, path_vid, path_img, content) VALUES", collection)

    def get_account_id(self, account):
        return self.connection.execute(f"SELECT id FROM {self.db_name}.resource_social WHERE screen_name = '{account}'")