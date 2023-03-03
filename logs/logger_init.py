import sqlite3

from datetime import datetime
from pathlib import Path


class Logging():
    """ Logger class. Write logs records into SQLite3"""
    def __init__(self):
        self.start_datetime = datetime.now()
        self.db = Path.cwd() / 'log.db'
        self.connection = sqlite3.connect(str(self.db))
        self.cursor = self.connection.cursor()

        self.cursor.execute("CREATE TABLE IF NOT EXISTS logs ( \
                        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, \
                        status INTEGER DEFAULT 0 NOT NULL, \
                        start_time TEXT NOT NULL, \
                        end_time TEXT, \
                        rest_time TEXT, \
                        total_run_time TEXT, \
                        publications INTEGER DEFAULT 0 NOT NULL, \
                        user_id INTEGER NOT NULL, \
                        an_id INTEGER NOT NULL, \
                        format TEXT NOT NULL, \
                        query_string TEXT NOT NULL \
                    )")

        self.cursor.execute("CREATE TABLE IF NOT EXISTS verify ( \
                        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, \
                        start_time TEXT NOT NULL, \
                        error TEXT NOT NULL, \
                        user_id INTEGER NOT NULL, \
                        an_id INTEGER NOT NULL, \
                        query_string TEXT NOT NULL \
                    )")

        self.cursor.execute("CREATE TABLE IF NOT EXISTS errors ( \
                        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, \
                        start_time TEXT NOT NULL, \
                        error TEXT NOT NULL, \
                        user_id INTEGER NOT NULL, \
                        an_id INTEGER NOT NULL, \
                        query_string TEXT NOT NULL \
                    )")

        self.connection.commit()

    def __del__(self):
        self.connection.close()

    def add_start_parameters(self, query_parameters):
        self.user_id = query_parameters.get('user_id')
        self.str_time = datetime.strftime(self.start_datetime, '%Y-%m-%d %H:%M:%S')
        try:
            self.an_id = int(query_parameters.get('an_id'))
        except:
            self.an_id = 0
        self.query = query_parameters.get('query')

        self.cursor.execute(
            f"INSERT INTO logs ( \
                start_time, \
                user_id, \
                an_id, \
                format, \
                query_string \
            ) VALUES( \
                '{self.str_time}', \
                {self.user_id}, \
                {self.an_id}, \
                '{query_parameters.get('format')}', \
                '{self.query}' \
        )")
        self.connection.commit()

    def add_verify_data(self, error_str):
        self.cursor.execute(
            f"INSERT INTO verify( \
                start_time, \
                error, \
                user_id, \
                an_id, \
                query_string \
            ) VALUES( \
                '{self.str_time}', \
                '{error_str}', \
                {self.user_id}, \
                {self.an_id}, \
                '{self.query}' \
        )")
        self.connection.commit()

    def add_error_data(self, error_str):
        self.cursor.execute(
            f"INSERT INTO errors( \
                start_time, \
                error, \
                user_id, \
                an_id, \
                query \
            ) VALUES( \
                '{self.str_time}', \
                '{error_str}', \
                {self.user_id}, \
                {self.an_id}, \
                '{self.query}' \
        )")
        self.connection.commit()

    def add_rest_data(self, rows: int = 0):
        dt = datetime.now()
        self.cursor.execute(
            f"UPDATE logs \
            SET \
                publications = {rows}, \
                rest_time = '{str(dt - self.start_datetime)[:-7]}' \
            WHERE \
                user_id = {self.user_id} \
            AND \
                start_time = '{self.str_time}'"
            )
        self.connection.commit()

    def add_end_parameters(self, status: int = 0):
        dt = datetime.now()
        end_time = datetime.strftime(dt, '%Y-%m-%d %H:%M:%S')
        self.cursor.execute(
            f"UPDATE logs \
            SET \
                status = {status}, \
                end_time = '{end_time}', \
                total_run_time = '{str(dt - self.start_datetime)[:-7]}' \
            WHERE \
                user_id = {self.user_id} \
            AND \
                start_time = '{self.str_time}'"
            )
        self.connection.commit()