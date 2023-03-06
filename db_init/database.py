from typing import Optional, List
from instagramstories import settings
from mysql.connector import connect, Error


def db_decorator(func):
    def wrapper(*args, **kwargs):
        con = connect(
            host='localhost',
            user=settings.mysql_account['login'],
            password=settings.mysql_account['password'],
        )
        try:
            result = func(*args, connection=con, *kwargs)
        except Error as e:
            print(e, 'SQL Failed!')
        else:
            con.commit()
            return result
        finally:
            con.close()

    return wrapper


class DataBase:
    def __init__(self, db_name):
        self.db_name = db_name

    @db_decorator
    def create_db(self, *args, **kwargs):
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"CREATE DATABASE {self.db_name}")
        print('DATABASE SUCCESSFULLY CREATED')

    @db_decorator
    def create_table(self, *args, **kwargs):
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f'USE {self.db_name}')
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {args[0]} ({', '.join([arg for arg in args[1:]])});")
        print(f'TABLE {args[0]} SUCCESSFULLY CREATED')

    @db_decorator
    def get_data_for_parse(self, *args, **kwargs):
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f'USE {self.db_name}')
        cursor.execute(f"SELECT {args[0]} FROM {args[1]};")

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

        print("DATA SUCCESSFULLY ADDED TO DATABASE")

    @db_decorator
    def get_account_id(self, *args, **kwargs):
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT account_id FROM accounts WHERE account = "{args[0]}";')

        return min([item[0] for item in cursor.fetchall()])

    @db_decorator
    def get_account_credentials(self, table_name: str, *args, status: str = 'stream_',  **kwargs) -> Optional[List]:
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT login, password, session FROM {table_name} WHERE status = "{status}" ORDER BY RAND() LIMIT 1;')

        return cursor.fetchone()

    @db_decorator
    def get_length_of_table(self, table_name: str, *args, status: str = 'stream_', **kwargs):
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT COUNT({args[0]}) FROM {table_name} WHERE status = "{status}"')

        return cursor.fetchone()



