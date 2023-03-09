from typing import Optional, List
from instagramstories import settings
from mysql.connector import connect, Error
from instagramstories.logs.logs_config import LoggerHandle

log = LoggerHandle()
log.logger_config()


def connection_params(host, user, password):
    def db_decorator(func):
        def wrapper(*args, **kwargs):
            con = connect(
                host=host,
                user=user,
                password=password,
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

    return db_decorator


class DataBase:
    def __init__(self, db_name):
        self.db_name = db_name

    @connection_params(settings.imas_account['host'], settings.imas_account['user'], settings.imas_account['password'])
    def create_db(self, *args, **kwargs):
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"CREATE DATABASE {self.db_name}")
        print('DATABASE SUCCESSFULLY CREATED')

    @connection_params(settings.imas_account['host'], settings.imas_account['user'], settings.imas_account['password'])
    def create_table(self, *args, **kwargs):
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f'USE {self.db_name}')
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {args[0]} ({', '.join([arg for arg in args[1:]])});")
        print(f'TABLE {args[0]} SUCCESSFULLY CREATED')

    @connection_params(settings.imas_account['host'], settings.imas_account['user'], settings.imas_account['password'])
    def get_data_for_parse(self,  table_name, quantity=1000, _type: int = 4, stability: int = 1, worker: int = 4, *args, **kwargs):
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f'USE {self.db_name}')
        cursor.execute(f"SELECT screen_name FROM {table_name} WHERE type = {_type} AND stability = {stability} AND worker = {worker} ORDER BY RAND() LIMIT {quantity}")

        return [item for item in cursor.fetchall()]

    @connection_params(settings.imas_account['host'], settings.imas_account['user'], settings.imas_account['password'])
    def send_to_table(self, table_name: str, columns: tuple, collection: list, *args, **kwargs) -> None:
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        insert = '%s,' * len(columns)
        cursor.execute(f"USE {self.db_name}")
        if len(collection) > 1:
            cursor.executemany(f"INSERT INTO {table_name} ({', '.join([col for col in columns])}) VALUES ({insert[:-1]})", collection)
        else:
            cursor.execute(f"INSERT INTO {table_name} ({', '.join([col for col in columns])}) VALUES ({insert[:-1]})", collection)

        print("DATA SUCCESSFULLY ADDED TO DATABASE")

    @connection_params(settings.imas_account['host'], settings.imas_account['user'], settings.imas_account['password'])
    def get_account_id(self, *args, **kwargs):
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT id FROM resource_social WHERE screen_name = "{args[0]}" ORDER BY ASC LIMIT 1;')

        return min([item[0] for item in cursor.fetchall()])

    @connection_params(settings.imas_account['host'], settings.imas_account['user'], settings.imas_account['password'])
    def get_account_credentials(self, table_name: str, soc_type: int = 4, _type: str = 'INST_PARSER', limit: int = 5, *args,  **kwargs) -> Optional[List]:
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT login, password FROM {table_name} WHERE soc_type = "{soc_type}" AND type = "{_type}" ORDER BY RAND() LIMIT {limit};')

        return [item for item in cursor.fetchall()]

    @connection_params(settings.imas_account['host'], settings.imas_account['user'], settings.imas_account['password'])
    def get_proxies(self, table_name: str, script: str = 'stories', limit: int = 5, *args, **kwargs):
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT proxy, port, login, password FROM {table_name} WHERE script = "{script}" ORDER BY RAND() LIMIT {limit};')

        return [item for item in cursor.fetchall()]


class DataBase2(DataBase):
    def __init__(self, db_name):
        super().__init__(db_name)

    @connection_params(settings.social_services['host'], settings.social_services['user'], settings.social_services['password'])
    def create_db(self, *args, **kwargs):
        super().create_db()

    @connection_params(settings.social_services['host'], settings.social_services['user'], settings.social_services['password'])
    def create_table(self, *args, **kwargs):
        super().create_table()

    @connection_params(settings.social_services['host'], settings.social_services['user'], settings.social_services['password'])
    def get_data_for_parse(self,  quantity=1000, *args, **kwargs):
        return super().get_data_for_parse()

    @connection_params(settings.social_services['host'], settings.social_services['user'], settings.social_services['password'])
    def get_account_id(self, *args, **kwargs):
        return super().get_account_id()

    @connection_params(settings.social_services['host'], settings.social_services['user'], settings.social_services['password'])
    def get_account_credentials(self, table_name: str, soc_type: int = 4, _type: str = 'INST_PARSER', limit: int = 1, *args, **kwargs) -> Optional[List]:
        return super().get_account_credentials()

    @connection_params(settings.social_services['host'], settings.social_services['user'], settings.social_services['password'])
    def get_proxies(self, table_name: str, script: str = 'stories', limit: int = 1, *args, **kwargs):
        return super().get_proxies()


class DataBase3(DataBase2):
    def __init__(self, db_name):
        super().__init__(db_name)

    @connection_params(settings.attachments['host'], settings.attachments['user'], settings.attachments['password'])
    def create_db(self, *args, **kwargs):
        super().create_db()

    @connection_params(settings.social_services['host'], settings.social_services['user'], settings.social_services['password'])
    def create_table(self, *args, **kwargs):
        super().create_table()

    @connection_params(settings.attachments['host'], settings.attachments['user'], settings.attachments['password'])
    def get_data_for_parse(self, quantity=1000, *args, **kwargs):
        return super().get_data_for_parse()

    @connection_params(settings.attachments['host'], settings.attachments['user'], settings.attachments['password'])
    def get_account_id(self, *args, **kwargs):
        return super().get_account_id()

    @connection_params(settings.attachments['host'], settings.attachments['user'], settings.attachments['password'])
    def get_account_credentials(self, table_name: str, soc_type: int = 4, _type: str = 'INST_PARSER', limit: int = 1, *args, **kwargs) -> Optional[List]:
        return super().get_account_credentials()

    @connection_params(settings.attachments['host'], settings.attachments['user'], settings.attachments['password'])
    def get_proxies(self, table_name: str, script: str = 'stories', limit: int = 1, *args, **kwargs):
        return super().get_proxies()

