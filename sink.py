import logging
import sqlite3
import pandas

from common import handler


class Sink:
    table_name = 'asx_indexes'

    def __init__(self):
        self.con = sqlite3.connect('sqlite.db')  # change to
        self.cur = self.con.cursor()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

    def create_table(self, fields):
        fields.pop('DATE_OF_INDEX', None)
        fields.pop('INDEX_CODE', None)
        create_query = f'''
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            DATE_OF_INDEX REAL,
            INDEX_CODE TEXT,
        '''
        for field in fields:
            create_query = create_query + f' {field} {fields[field]},'
        create_query = create_query + '''
            PRIMARY KEY (DATE_OF_INDEX, INDEX_CODE)
        );
        '''
        self.logger.info(create_query)
        self.cur.execute(create_query)
        self.con.commit()

    def load(self, file):
        """
        Using overwrite mode isn't correct, but the correct thing to do is perform consolidation in memory. Need more
        time
        :param file:
        :return:
        """
        pandas.read_csv(file, delimiter='\t').to_sql(self.table_name, self.con, if_exists='replace', index=False)

    def fetch(self, limit):
        self.cur.execute(f"SELECT * FROM {self.table_name} LIMIT {limit}")
        fetchall = self.cur.fetchall()
        for r in fetchall:
            print(r)

    def close(self):
        self.con.commit()
        self.con.close()
