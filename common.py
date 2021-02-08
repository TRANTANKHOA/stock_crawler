import logging
import sys

handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s|%(name)s|%(levelname)s: %(message)s'))
handler.setLevel(logging.DEBUG)
