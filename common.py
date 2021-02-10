import logging
import sys

INDEX_CODE = 'INDEX_CODE'
DATE_OF_INDEX = 'DATE_OF_INDEX'
EFFECTIVE_DATE = 'EFFECTIVE_DATE'
DATE_OF_INDEX_SCHEMA_JSON_FILE_NAME = f'{DATE_OF_INDEX}.json'
EFFECTIVE_DATE_SCHEMA_JSON_FILE_NAME = f'{EFFECTIVE_DATE}.json'

handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s|%(name)s|%(levelname)s: %(message)s'))
handler.setLevel(logging.DEBUG)
