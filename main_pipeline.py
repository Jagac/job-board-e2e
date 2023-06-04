import requests
import os
import logging.config
import configparser
import json
import polars as pl


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

logger.info('Start Extract Session')
logger.info('Source Filename: {}'.format(JobConfig['SrcObject']))


def extract():
    logger.info('Start Extract Session')

    def request_from_api():
        try:
            url = 'https://justjoin.it/api/offers'
            r = requests.get(url, allow_redirects=True)
            open('jobs.json', 'wb').write(r.content)

        except ValueError as e:
            logger.error(e)

        logger.info('Request completed successfully')

    def load_data():
        try:
            #request_from_api()
            f = open('jobs.json', encoding="utf-8")
            logger.info('Source Filename: {}'.format(JobConfig['SrcObject']))
            return json.load(f)

        except ValueError as e:
            logger.error(e)

    def create_dataframe():
        loaded_json = load_data()
        data = []
        i = 0
        for i, record in enumerate(loaded_json):
            data.append([
                loaded_json[i]['title'],
                loaded_json[i]['street'],
                loaded_json[i]['city'],
                loaded_json[i]['country_code'],
                loaded_json[i]['address_text'],
                loaded_json[i]['marker_icon'],
                loaded_json[i]['workplace_type'],
                loaded_json[i]['company_name'],
                loaded_json[i]['company_url'],
                loaded_json[i]['company_size'],
                loaded_json[i]['experience_level'],
                loaded_json[i]['latitude'],
                loaded_json[i]['longitude'],
                loaded_json[i]['published_at'],
                loaded_json[i]['remote_interview'],
                loaded_json[i]['id'],
                loaded_json[i]['employment_types'],
                loaded_json[i]['company_logo_url'],
                loaded_json[i]['skills'],
                loaded_json[i]['remote'],
                loaded_json[i]['open_to_hire_ukrainians']])

            df_pl = pl.DataFrame(data)
            df_pl = df_pl.select(
                [pl.col("column_0").alias("Title")])

            return df_pl

    df = create_dataframe()

    return df


test = extract()
print(test.head(5))


