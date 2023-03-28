from typing import Optional, List
from clickhouse_driver import Client
from instagramstories import settings
from mysql.connector import connect, Error

from instagramstories.logs.logs_config import log


def db_decorator(func):
    """ Maintaining connection and execution of any operation to MariaDB """

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
        else:
            con.commit()
            return result
        finally:
            con.close()

    return wrapper


class MariaDataBase:
    def __init__(self, db_name: str) -> None:
        self.db_name = db_name

    @db_decorator
    def get_data_for_parse(self,  table_name, quantity=500, _type: int = 4, stability: int = 1, worker: int = 4, *args, **kwargs) -> Optional[list]:
        """ Retrieves accounts from DataBase which will be parsed """

        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f'USE {self.db_name}')
        cursor.execute(f"SELECT screen_name FROM {table_name} WHERE type = {_type} AND stability = {stability} AND worker = {worker} ORDER BY RAND() LIMIT {quantity}")

        return [item for item in cursor.fetchall()]

    @db_decorator
    def send_to_table(self, table_name: str, columns: tuple, collection: list, *args, **kwargs) -> None:
        """ Inserts parsed information into specified table in database """

        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        insert = '%s,' * len(columns)
        cursor.execute(f"USE {self.db_name}")
        if len(collection) > 1:
            cursor.executemany(f"INSERT INTO {table_name} ({', '.join([col for col in columns])}) VALUES ({insert[:-1]})", collection)
        else:
            cursor.execute(f"INSERT INTO {table_name} ({', '.join([col for col in columns])}) VALUES ({insert[:-1]})", collection)

    @db_decorator
    def get_account_id(self, *args, **kwargs) -> int:
        """ Retrieves account_id from database """

        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT id FROM resource_social WHERE screen_name = "{args[0]}";')

        return cursor.fetchone()[0]

    @db_decorator
    def get_account_credentials(self, table_name: str, soc_type: int = 4, _type: str = 'INST_STORY_PARSER', work: int = 1, limit: int = 5, *args,  **kwargs) -> Optional[List]:
        """ Retrieves credentials which will be used for login into instagram account """

        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT login, password FROM {table_name} WHERE soc_type = {soc_type} AND type = "{_type}" AND work = {work} ORDER BY RAND() LIMIT {limit};')

        return [item for item in cursor.fetchall()]

    @db_decorator
    def mark_account_as_used(self, table_name: str, login: str, work: int = 0, _type: str = 'INST_STORY_PARSER', *args, **kwargs) -> None:
        """ Marks account for non-duplicate purpose.
            Once account is banned we should never take it. """

        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'UPDATE {table_name} SET work = {work} WHERE type = "{_type}" AND login = "{login}";')

    @db_decorator
    def get_proxies(self, table_name: str, script: str = 'insta_story', limit: int = 5, *args, **kwargs) -> Optional[list]:
        """ Retrieves proxies from database """

        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT proxy, port, login, password FROM {table_name} WHERE script = "{script}" ORDER BY RAND() LIMIT {limit};')

        return [item for item in cursor.fetchall()]

    @db_decorator
    def get_new_yadisk_token(self, table_name: str, work: int = 1, limit: int = 1, *args, **kwargs) -> str:
        """ Retrieves new yadisk token.
            Once free space on yandex disk is refilled we should take another token. """

        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT token FROM {table_name} WHERE work = {work} ORDER BY RAND() LIMIT {limit}')

        return cursor.fetchone()[0]

    @db_decorator
    def update_status_of_yadisk_token(self, table_name: str, token: str, *args, **kwargs) -> None:
        """ Updates status of particular token.
            Once we took token we should update its status. """

        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f'USE {self.db_name}')
        cursor.execute(f'UPDATE {table_name} SET work = 0 WHERE token = "{token}"')

    @db_decorator
    def get_yandex_disk_capacity(self, table_name: str, token: str, *args, **kwargs) -> int:
        """ This method helps our script in detecting how much free space is left on yandex disk. """

        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT space_used FROM {table_name} WHERE token = "{token}"')

        return cursor.fetchone()[0]

    @db_decorator
    def update_yandex_disk_capacity(self, table_name: str, space_used: int, token: str, *args, **kwargs) -> None:
        """ Each time we load files into yandex disk we should know how much space is left. """

        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'UPDATE {table_name} SET space_used = {space_used} WHERE token = "{token}"')


class ClickHouseDatabase:
    def __init__(self, db_name: str, host: str, port: int, username: str, password: str) -> None:
        self.db_name = db_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.connection = Client(host=self.host, user=self.username, port=self.port, password=self.password)

    def get_data_for_parse(self,  table_name, quantity=500, _type: int = 4, stability: int = 1, worker: int = 4) -> Optional[list]:
        """ Retrieves accounts from DataBase which will be parsed """

        return self.connection.execute(f"SELECT screen_name FROM {self.db_name}.{table_name} WHERE type = {_type} AND stability = {stability} AND worker = {worker} ORDER BY RAND() LIMIT {quantity}")

    def send_to_table(self, table_name: str, collection: [tuple, list]) -> None:
        """ Inserts parsed information into specified table in database """

        self.connection.execute(f"INSERT INTO {self.db_name}.{table_name} (account_id, path_vid, path_img, content) VALUES", collection)

    def get_account_id(self, account: str) -> int:
        """ Retrieves account_id from database """

        return self.connection.execute(f"SELECT id FROM {self.db_name}.resource_social WHERE screen_name = '{account}'")[0][0]
