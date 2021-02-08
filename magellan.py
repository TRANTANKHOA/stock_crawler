import csv
import re
import pysftp
import logging
from common import handler
from sink import Sink


class Pipeline:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        self.sink = Sink()

    def init_table(self):
        with pysftp.Connection(host='s-5bd0c837c63242888.server.transfer.ap-southeast-2.amazonaws.com',
                               username='deuser',
                               private_key="./key") as sftp:
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
                            field_name = re.sub('[^0-9a-zA-Z]+', '_', field)
                            if line[field_index].replace('.', '', 1).isdigit() and fields.get(field_name) != 'TEXT':
                                fields[field_name] = 'REAL'
                            else:
                                fields[field_name] = 'TEXT'
            self.sink.create_table(fields)

    def load(self):
        with pysftp.Connection(host='s-5bd0c837c63242888.server.transfer.ap-southeast-2.amazonaws.com',
                               username='deuser',
                               private_key="./key") as sftp:
            for file_name in sftp.listdir('/'):
                self.logger.info(f"Loading file {file_name}")
                with sftp.open(file_name) as file:
                    lines = list(csv.reader(file, dialect="excel-tab"))
                    header = lines[0]
                    lines = ['\t'.join(l) for l in lines if len(l) == len(header)]
                    new_header = ''
                    for field in header:
                        re_sub = re.sub('[^0-9a-zA-Z]+', '_', field)
                        if new_header:
                            new_header = new_header + '\t' + re_sub
                        else:
                            new_header = re_sub
                    lines[0] = new_header
                    tmp_tsv = 'tmp.tsv'
                    with open(tmp_tsv, 'w') as tmp_file:
                        tmp_file.write("\n".join(lines))
                    with open(tmp_tsv, 'r') as tsv:
                        self.sink.load(tsv)
