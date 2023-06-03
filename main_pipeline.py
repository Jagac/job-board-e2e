import requests
import os
import logging.config
import configparser
import json

# setting up a nicely formatted log
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

config = configparser.ConfigParser()
config.read('etl_config.ini')
JobConfig = config['ETL_Log_Job']

formatter = logging.Formatter('%(levelname)s:  %(asctime)s:  %(process)s:  %(funcName)s:  %(message)s')
stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler(JobConfig['LogName'])
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def request_from_api():
    url = "https://justjoin.it/api/offers"
    r = requests.get(url, allow_redirects=True)
    open('jobs.json', 'wb').write(r.content)


def load_data():
    f = open('jobs.json', encoding="utf-8")

    return json.load(f)


request_from_api()
test = load_data()



