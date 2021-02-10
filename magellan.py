import csv
import hashlib
import json
import re
from datetime import datetime

import pysftp
import logging
from common import handler
from inventory import Inventory
from sink import Sink
from common import DATE_OF_INDEX_SCHEMA_JSON_FILE_NAME, DATE_OF_INDEX, EFFECTIVE_DATE, \
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


def convert_time(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%b %d %Y %H:%M:%S')


def write_csv_to_sink(file, sink):
    reader = csv.reader(file, dialect="excel-tab")
    header = clean_header(next(reader, None))
    sink.load(header, lines=[ln for ln in reader if len(ln) == len(header)])


class Pipeline:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

    def init_table(self):
        """
        Scanning all files in SFTP servers, reading 10 lines from each file and create a valid SQL table schema for
        storing data from these files.

        For simplicity, any given field is either TEXT or REAL data. It is found that files come with two type of
        date columns
        :return:
        """
        with get_sftp_connection() as sftp, Sink() as sink:
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
            sink.create_table(date_of_index_fields, DATE_OF_INDEX, 'REAL')
            self.write_schema(effective_date_fields, EFFECTIVE_DATE_SCHEMA_JSON_FILE_NAME)
            sink.create_table(effective_date_fields, EFFECTIVE_DATE, 'TEXT')

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
        with get_sftp_connection() as sftp, Sink() as sink, Inventory() as inventory:
            for attr in sftp.listdir_attr():
                self.logger.info(f"Loading file {attr.filename} with timestamp "
                                 f"{convert_time(attr.st_mtime)}")
                existing_file = inventory.fetch(attr.filename)
                if existing_file:
                    filename, timestamp, checksum = existing_file
                    timestamp = int(timestamp)
                    if timestamp >= attr.st_mtime:
                        self.logger.info(f"This file was previously loaded with a later or equal timestamp "
                                         f"{convert_time(timestamp)}. Skipping..")
                    else:
                        self.logger.info(f"This file was previously loaded with an earlier timestamp "
                                         f"{convert_time(timestamp)}")
                        with sftp.open(attr.filename) as file:
                            # file size is quicker to compute but is a less restrictive constrain
                            new_checksum = hashlib.md5(file.read()).hexdigest()
                            if new_checksum == checksum:
                                self.logger.info(f"This file has the same checksum with previous file. Skipping..")
                                continue
                            self.logger.info(f"This file has new checksum. Updating inventory..")
                            inventory.put(attr.filename, attr.st_mtime, new_checksum)
                            file.seek(0)
                            write_csv_to_sink(file, sink)
                else:
                    with sftp.open(attr.filename) as file:
                        inventory.put(attr.filename, attr.st_mtime, hashlib.md5(file.read()).hexdigest())
                        file.seek(0)
                        write_csv_to_sink(file, sink)


Pipeline().load()
