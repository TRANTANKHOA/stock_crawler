import csv
import json
import re
import pysftp
import logging
from common import handler
from sink import Sink, DATE_OF_INDEX_SCHEMA_JSON_FILE_NAME, DATE_OF_INDEX, EFFECTIVE_DATE, \
    EFFECTIVE_DATE_SCHEMA_JSON_FILE_NAME


def get_sftp_connection():
    """
    Please place your private SSH key file at `./key` for the authentication to work
    :return: an pysftp client connection
    """
    return pysftp.Connection(host='s-5bd0c837c63242888.server.transfer.ap-southeast-2.amazonaws.com',
                             username='deuser',
                             private_key='./key')


def remove_non_alpha_numeric(field):
    """
    Striping out non-alphanumerical characters in fields' name for ease of reading and also avoid error in SQL queries
    :param field:
    :return:
    """
    return re.sub('[^0-9a-zA-Z]+', '_', field)


def clean_header(header):
    """
    Striping out non-alphanumerical characters in fields of 'header'
    :type header: list
    """
    new_header = []
    for field in header:
        new_header.append(remove_non_alpha_numeric(field))
    return new_header


class Pipeline:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        self.sink = Sink()

    def init_table(self):
        """
        Scanning all files in SFTP servers, reading 10 lines from each file and create a valid SQL table schema for
        storing data from these files.

        For simplicity, any given field is either TEXT or REAL data. It is found that files come with two type of
        date columns
        :return:
        """
        with get_sftp_connection() as sftp:
            date_of_index_fields = {}
            effective_date_fields = {}
            max_lines = 10
            for file_name in sftp.listdir('/'):
                self.logger.info(f"Parsing file {file_name}")
                with sftp.open(file_name) as file:
                    reader = csv.reader(file, dialect="excel-tab")
                    header = clean_header(next(reader, None))
                    if DATE_OF_INDEX in header:
                        self.parse_lines(date_of_index_fields, header, max_lines, reader)
                    if EFFECTIVE_DATE in header:
                        self.parse_lines(effective_date_fields, header, max_lines, reader)
            self.write_schema(date_of_index_fields, DATE_OF_INDEX_SCHEMA_JSON_FILE_NAME)
            self.sink.create_table(date_of_index_fields, DATE_OF_INDEX, 'REAL')
            self.write_schema(effective_date_fields, EFFECTIVE_DATE_SCHEMA_JSON_FILE_NAME)
            self.sink.create_table(effective_date_fields, EFFECTIVE_DATE, 'TEXT')

    @staticmethod
    def write_schema(date_of_index_fields, filename):
        with open(filename, 'w') as schema_file:
            schema_file.writelines(json.dumps(date_of_index_fields, indent=2))

    @staticmethod
    def parse_lines(fields, header, max_lines, reader):
        count = 0
        for line in reader:
            count += 1
            if count > max_lines or len(line) != len(header):
                break
            for field in header:
                field_index = header.index(field)
                if line[field_index].replace('.', '', 1).isdigit() and fields.get(field) != 'TEXT':
                    fields[field] = 'REAL'
                else:
                    fields[field] = 'TEXT'

    def load(self):
        """
        Loading all files in SFTP server to a predefined table given by `self.init_table`
        :return:
        """
        with get_sftp_connection() as sftp:
            for file_name in sftp.listdir('/'):
                self.logger.info(f"Loading file {file_name}")
                with sftp.open(file_name) as file:
                    reader = csv.reader(file, dialect="excel-tab")
                    header = clean_header(next(reader, None))
                    self.sink.load(header, lines=[ln for ln in reader if len(ln) == len(header)])
