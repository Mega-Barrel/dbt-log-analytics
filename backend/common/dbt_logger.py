""" Global logger format"""
import logging
import yaml # pylint: disable=E0401

# load config
with open('configs/logger.yaml', 'r', encoding='utf-8') as file:
    config = yaml.safe_load(file)

logger = logging.getLogger(__name__)

# Read in variables
FILE_NAME = config['dbt-logger']['file']['filename']
FILE_HANDLER = logging.FileHandler(FILE_NAME)
LOG_FORMAT = config['dbt-logger']['formatters']['kafka-logs']['format']
ROOT_LEVEL = config['dbt-logger']['root']['level']

FORMATTER = logging.Formatter(LOG_FORMAT)
FILE_HANDLER.setFormatter(FORMATTER)

# Add the file handler to the logger
logger.addHandler(FILE_HANDLER)

# Set the logging level (you can adjust this based on your needs)
logger.setLevel(ROOT_LEVEL)
