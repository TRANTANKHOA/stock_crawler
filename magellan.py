import csv
import json
import re
import pysftp
import logging
from common import handler
from sink import Sink


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

        For simplicity, any given field is either TEXT or REAL data
        :return:
        """
        with get_sftp_connection() as sftp:
            fields = {}
            max_lines = 10
            for file_name in sftp.listdir('/'):
                self.logger.info(f"Parsing file {file_name}")
                with sftp.open(file_name) as file:
                    reader = csv.reader(file, dialect="excel-tab")
                    header = next(reader, None)
                    count = 0
                    for line in reader:
                        count += 1
                        if count > max_lines or len(line) != len(header):
                            break
                        for field in header:
                            field_index = header.index(field)
                            field_name = remove_non_alpha_numeric(field)
                            if line[field_index].replace('.', '', 1).isdigit() and fields.get(field_name) != 'TEXT':
                                fields[field_name] = 'REAL'
                            else:
                                fields[field_name] = 'TEXT'
            with open('schema.json', 'w') as schema_file:
                schema_file.writelines(json.dumps(fields, indent=2))
            self.sink.create_table(fields)

    def load(self):
        """
        Loading all files in SFTP server to a predefined table given by `self.init_table`
        :return:
        """
        with get_sftp_connection() as sftp:
            tmp_tsv = 'tmp.tsv'
            for file_name in sftp.listdir('/'):
                self.logger.info(f"Loading file {file_name}")
                with sftp.open(file_name) as file:
                    lines = list(csv.reader(file, dialect="excel-tab"))
                    # need to replace the original header with its non-alphanumeric version for SQL loading to works
                    header = lines[0]
                    lines = ['\t'.join(l) for l in lines if len(l) == len(header)]
                    new_header = ''
                    for field in header:
                        re_sub = remove_non_alpha_numeric(field)
                        if new_header:
                            new_header = new_header + '\t' + re_sub
                        else:
                            new_header = re_sub
                    lines[0] = new_header
                    with open(tmp_tsv, 'w') as tmp_file:
                        tmp_file.write("\n".join(lines))
                    with open(tmp_tsv, 'r') as tsv:
                        self.sink.load(tsv)
