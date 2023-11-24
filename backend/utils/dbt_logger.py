""" Global logger format"""
import logging
import yaml # pylint: disable=E0401

# load config
with open('configs/yfin_etl.yaml', 'r', encoding='utf-8') as file:
    config = yaml.safe_load(file)

# Read in variables
FILE_NAME = config['dbt-logger']['file']['filename']
ROOT_LEVEL = config['dbt-logger']['root']['level']
LOG_FORMAT = config['dbt-logger']['formatters']['kafka-logs']['format']

# Set logging configuration
logging.basicConfig(
    level=ROOT_LEVEL,
    filename=FILE_NAME,
    filemode='a',
    format=LOG_FORMAT
)

logger = logging.getLogger(__name__)
