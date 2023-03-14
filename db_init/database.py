import os
import time

from clickhouse_driver import Client

from typing import Optional, List

from instagramstories import settings
from mysql.connector import connect, Error

from instagramstories.accounts import get_cred, get_accs
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
    def create_db(self, *args, **kwargs):
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"CREATE DATABASE {self.db_name}")
        print('DATABASE SUCCESSFULLY CREATED')

    db_decorator
    def create_table(self, *args, **kwargs):
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f'USE {self.db_name}')
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {args[0]} ({', '.join([arg for arg in args[1:]])});")
        print(f'TABLE {args[0]} SUCCESSFULLY CREATED')

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

        print("DATA SUCCESSFULLY ADDED TO DATABASE")

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
    def get_proxies(self, table_name: str, script: str = 'insta_story', limit: int = 5, *args, **kwargs):
        connection = kwargs.pop('connection')
        cursor = connection.cursor()

        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f'SELECT proxy, port, login, password FROM {table_name} WHERE script = "{script}" ORDER BY RAND() LIMIT {limit};')

        return [item for item in cursor.fetchall()]


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
        self.connection.execute(f"INSERT INTO {table_name} (account_id, type, path, text) VALUES", collection)

    def get_account_id(self, account):
        return self.connection.execute(f"SELECT id FROM {self.db_name}.resource_social WHERE screen_name = '{account}'")


# a = ClickHouseDatabase('attachments', settings.attachments_db['host'], settings.attachments_db['port'], settings.attachments_db['user'], settings.attachments_db['password'])
# a.create_db()
# a.create_table('atc_resource',
#                'account_id INT',
#                'type INT',
#                'path VARCHAR(255)')
# a.send_to_table('atc_resource', [[12313, 1, 'sdjf', ''], [12313, 1, 'sdjf', ''], [12313, 1, '', 'dsfdsfs'], [12313, 1, '', 'esffsdf']])
# c = ClickHouseDatabase('imas', settings.imas_db['host'], settings.imas_db['port'], settings.imas_db['user'], settings.imas_db['password'])
# print([item[0] for item in c.get_data_for_parse('resource_social')])
# m = MariaDataBase('social_services')
# print(m.get_account_credentials('soc_accounts', 4, 'INST_STORY_PARSER', 1, 5))
# print(m.get_proxies('proxies'))

# p = MariaDataBase('social_services')
# print(p.get_proxies('proxies', 'Ramis'))

# PATH_TO_ACCOUNTS = '/home/newuser/work_artem/instagramstories/accounts/account_for_parse.txt'
# PATH_TO_CREDENTIALS = '/home/newuser/work_artem/instagramstories/accounts/credentials.txt'
# db1 = DataBase('imas')
# db2 = DataBase('social_services')
# db3 = DataBase('attachments')
#
# db1.create_db()
# db2.create_db()
# db3.create_db()
#
# # Creating table proxies
# db2.create_table('proxies',
#                  'proxy_id INT PRIMARY KEY AUTO_INCREMENT',
#                  'proxy VARCHAR(255)',
#                  'port VARCHAR(255)',
#                  'login VARCHAR(255)',
#                  'password VARCHAR(255)',
#                  'script VARCHAR(255)', )
#
# # Creating table resource_social
# db1.create_table('resource_social',
#                 'id int PRIMARY KEY AUTO_INCREMENT',
#                 'type int',
#                 'stability int',
#                 'worker int',
#                 'screen_name VARCHAR(255) NOT NULL',
#                 )
#
# # Creating table attachments
# db3.create_table('attachments',
#                 'record_id int PRIMARY KEY AUTO_INCREMENT',
#                 'account_id int NOT NULL',
#                 'type int NOT NULL',
#                 'path TEXT NOT NULL',
#                 )
#
# # Creating table credentials
# db2.create_table('soc_accounts',
#                 'credentials_id int PRIMARY KEY AUTO_INCREMENT',
#                 'login VARCHAR(255)',
#                 'password VARCHAR(255)',
#                 'soc_type int',
#                 'work int',
#                 'type VARCHAR(255)',
#                 )
#
# collection = [('185.156.178.105', '3021', 'indy361400', 'Degq3961Qz', 'stories'),
#               ('185.156.178.105', '3022', 'indy361400', 'Degq3961Qz', 'stories'),
#               ('185.156.178.105', '3023', 'indy361400', 'Degq3961Qz', 'stories'),
#               ('185.156.178.105', '3024', 'indy361400', 'Degq3961Qz', 'stories'),
#               ('185.156.178.105', '3025', 'indy361400', 'Degq3961Qz', 'stories'),
#               ]
#
# # Fill table proxies with initial data
# db2.send_to_table('proxies', ('proxy', 'port', 'login', 'password', 'script'), collection)
#
# # Fill table credentials with initial data
# try:
#     db2.send_to_table('soc_accounts', ('login', 'password', 'soc_type', 'work', 'type'),
#                      get_cred.get_credentials(PATH_TO_CREDENTIALS))
# except Exception:
#     print('There is no accounts to send!')
#
# # Fill table accounts with initial data
# try:
#     db1.send_to_table('resource_social', ('screen_name', 'type', 'stability', 'worker'), get_accs.get_accounts(PATH_TO_ACCOUNTS))
# except Exception:
#     print('There is no accounts to send!')