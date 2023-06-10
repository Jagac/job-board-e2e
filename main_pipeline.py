import json
import pandas as pd
import requests
import logging.config
import configparser
from datetime import datetime, timedelta
import time
import psutil


# setting up a nicely formatted log
today = datetime.today().strftime('%m-%d-%Y')
yesterday = (datetime.now() - timedelta(1)).strftime('%m-%d-%Y')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

config = configparser.ConfigParser()
config.read('etl_config.ini')
JobConfig = config['ETL_Log_Job']

formatter = logging.Formatter('%(levelname)s:  %(asctime)s:  %(process)s:  %(funcName)s:  %(message)s')
stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler(f'/home/jagac/projects/job-board-e2e/etl_logs/etl_log_job {today}.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def extract():
    """
    Extract data from the json from justjoin API and transform it into a pandas dataframe and csv
    :return: pd.DataFrame
    """
    logger.info('Start Extract Session')

    def request_from_api():
        try:
            url = 'https://justjoin.it/api/offers'
            r = requests.get(url, allow_redirects=True)
            open(f'/home/jagac/projects/job-board-e2e/json_data/jobs {today}.json', 'wb').write(r.content)

        except Exception as e:
            logger.exception(e)

        logger.info('Request completed successfully')

    def load_data():
        try:
            request_from_api()
            f = open(f'/home/jagac/projects/job-board-e2e/json_data/jobs {today}.json', encoding="utf-8")
            logger.info('Loaded: {}'.format(JobConfig['src_object']))
            return json.load(f)

        except Exception as e:
            logger.exception(e)

    def create_dataframe():
        loaded_json = load_data()
        logger.info('Converting to csv file')
        data = []
        i = 0
        try:
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
                
                i += 1
            
            df = pd.DataFrame(data,
                                columns=['Title',
                                        'Street',
                                        'City',
                                        'Country Code',
                                        'Address Text',
                                        'Marker Icon',
                                        'Workplace Type',
                                        'Company Name',
                                        'Company Url',
                                        'Company Size',
                                        'Experience Level',
                                        'Latitude',
                                        'Longitude',
                                        'Published At',
                                        'Remote interview',
                                        'ID',
                                        'Employment Types',
                                        'Company Logo',
                                        'Skills',
                                        'Remote',
                                        'Open to Hire Ukrainians'])

        except Exception as e:
            logger.exception(e)
            
        return df

                
    df_converted = create_dataframe()
    df_converted = df_converted.drop_duplicates(subset=['ID'])
    df_converted.to_csv(f'/home/jagac/projects/job-board-e2e/csv_data/data {today}.csv')
    logger.info('Successfully converted to csv')


def merge():
    new = pd.read_csv(f'/home/jagac/projects/job-board-e2e/csv_data/data {today}.csv')
    try:
        old = pd.read_csv(f'/home/jagac/projects/job-board-e2e/csv_data/data {yesterday}.csv')
    except:
        old = pd.DataFrame()

    new_df = pd.concat([new, old], axis=0)
    new_df.to_csv(f'/home/jagac/projects/job-board-e2e/csv_data/data {today}.csv')
    logger.info('Successfully merged data')
    

def main():
    start1 = time.time()
    extract()
    end1 = time.time() - start1
    logger.info("Extract took : {} seconds".format(end1))
    logger.info('Extract CPU usage {}%'.format(psutil.cpu_percent()))
    logger.info('RAM memory {}% used'.format(psutil.virtual_memory().percent))

    start2 = time.time()
    merge()
    end2 = time.time() - start2
    logger.info("Merge took : {} seconds".format(end2))
    logger.info('Merge CPU usage {}%'.format(psutil.cpu_percent()))
    logger.info('RAM memory {}% used'.format(psutil.virtual_memory().percent))


if __name__ == "__main__":
    main()
