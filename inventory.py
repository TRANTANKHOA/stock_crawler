import logging
import sqlite3

from common import handler


class Inventory:
    """
    This can be replaced by DynamoDB in practice.
    """

    def __init__(self):
        self.con = sqlite3.connect('sqlite.db')  # change to
        self.cur = self.con.cursor()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        self.cur.execute(f'''
        CREATE TABLE IF NOT EXISTS file_inventory (
            filename TEXT,
            timestamp INTEGER,
            checksum TEXT,
            PRIMARY KEY (filename)
        );
        ''')
        self.con.commit()

    def __enter__(self):
        self.__init__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def put(self, filename, timestamp, checksum):
        self.cur.execute(f'''
        REPLACE INTO file_inventory (filename, timestamp, checksum)
        VALUES (?,?,?);
        ''', [filename, timestamp, checksum])
        self.con.commit()

    def fetch(self, filename):
        self.cur.execute(f"SELECT * FROM file_inventory WHERE filename='{filename}';")
        return self.cur.fetchone()

    def close(self):
        self.con.commit()
        self.con.close()
