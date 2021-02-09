import json
import logging
import sqlite3
from common import handler

INDEX_CODE = 'INDEX_CODE'
DATE_OF_INDEX = 'DATE_OF_INDEX'
EFFECTIVE_DATE = 'EFFECTIVE_DATE'
DATE_OF_INDEX_SCHEMA_JSON_FILE_NAME = f'{DATE_OF_INDEX}.json'
EFFECTIVE_DATE_SCHEMA_JSON_FILE_NAME = f'{EFFECTIVE_DATE}.json'


class Sink:
    table_name = 'asx_indexes'

    def __init__(self):
        self.con = sqlite3.connect('sqlite.db')  # change to
        self.cur = self.con.cursor()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

    def create_table(self, fields, date_column, date_type):
        """
        Create new SQL table with listed `fields` and different `date_column`

        :type fields: dict
        :type date_type: str
        :type date_column: str
        """
        fields.pop(date_column, None)
        fields.pop(INDEX_CODE, None)
        create_query = f'''
        CREATE TABLE IF NOT EXISTS {self.table_name}_{date_column} (
            {date_column} {date_type},
            {INDEX_CODE} TEXT,
        '''
        for field in fields:
            create_query = create_query + f' {field} {fields[field]},'
        create_query = create_query + f'''
            PRIMARY KEY ({date_column}, {INDEX_CODE})
        );
        '''
        self.logger.info(create_query)
        self.cur.execute(create_query)
        self.con.commit()

    def load(self, header, lines):
        """
        Load a single line to SQL
        :type lines: list
        :type header: list
        """
        if DATE_OF_INDEX in header:
            self.load_by_schema(header, lines, DATE_OF_INDEX, DATE_OF_INDEX_SCHEMA_JSON_FILE_NAME)
        elif EFFECTIVE_DATE in header:
            self.load_by_schema(header, lines, EFFECTIVE_DATE, EFFECTIVE_DATE_SCHEMA_JSON_FILE_NAME)
        else:
            self.logger.error(f'Found invalid header: {header}')

    def load_by_schema(self, header, lines, date_column, schema_file_name):
        """
        Load one line by joining it with data already in SQL if primary keys match

        :param header:
        :type header: list
        :param lines:
        :type lines: list
        :param date_column:
        :type date_column: str
        :param schema_file_name:
        :type schema_file_name: str
        :return:
        """
        with open(schema_file_name, 'r') as schema_file:
            json_schema = json.loads(schema_file.read())
            records = []
            for line in lines:
                record = {}
                missing_fields = []
                for field in json_schema:
                    if field in header:
                        record[field] = line[header.index(field)]
                    else:
                        missing_fields.append(field)
                if missing_fields:
                    missing_values = self.fetch_fields(missing_fields,
                                                       date_column,
                                                       line[header.index(date_column)],
                                                       line[header.index(INDEX_CODE)])
                    if missing_values:
                        for field in missing_fields:
                            record[field] = missing_values[missing_fields.index(field)]
                records.append(record)
        for rec in records:  # each record has a different number of fields so it's hard to write them together in batch
            keys = rec.keys()
            # make sure that each key will default to None if the key doesn't exist in the json entry.
            vals = [rec.get(key, None) for key in keys]
            self.con.execute(f"""
                REPLACE INTO {self.table_name}_{date_column} ({', '.join(keys)}) 
                VALUES ({', '.join(['?' for each in keys if True])});
            """, vals)
        self.con.commit()

    def fetch_fields(self, fields, date_column, date, code):
        query = f'''
            SELECT {','.join(fields)} 
            FROM {self.table_name}_{date_column} 
            WHERE {date_column} = '{date}' 
            AND {INDEX_CODE} = '{code}'
        '''
        return self.cur.execute(query).fetchone()

    def fetch(self, limit=10, date_column=DATE_OF_INDEX):
        self.cur.execute(f"SELECT * FROM {self.table_name}_{date_column} LIMIT {limit}")
        for r in self.cur.fetchall():
            print(r)

    def close(self):
        self.con.commit()
        self.con.close()
