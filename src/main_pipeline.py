import json
import pandas as pd
import numpy as np
import requests
import logging.config
from datetime import datetime, timedelta
import time
import psutil
from data_quality_tests import run_data_tests
import yaml

with open("C:\\Users\\jagos\\Documents\\GitHub\\job-board-e2e\\src\\config.yaml", "r") as file:
    global_variables = yaml.safe_load(file)

json_path = global_variables['paths_to_save']['json_path']
csv_path = global_variables['paths_to_save']['csv_path']
logs_path = global_variables['paths_to_save']['logs_path']

today = datetime.today().strftime('%Y-%m-%d')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s:  %(asctime)s:  %(process)s:  %(funcName)s:  %(message)s')
file_handler = logging.FileHandler(f'{logs_path}/etl_log_job {today}.log')
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
            open(f'{json_path}/jobs {today}.json', 'wb').write(r.content)

        except Exception as e:
            logger.exception(e)

        logger.info('Request completed successfully')

    def load_data():
        try:
            request_from_api()
            f = open(f'{json_path}/jobs {today}.json', encoding="utf-8")
            logger.info('Loaded: {}'.format(f))
            return json.load(f)

        except Exception as e:
            logger.exception(e)

    def create_dataframe():
        """Parses json with multiple levels and joins them all on index

        Returns:
            pd.DataDrame: Dataframe containing data from parsed json
        """
        loaded_json = load_data()
        logger.info('Converting to csv file')
        data = []
        skills_data = []
        employment_type_data = []
        salary_temp = []
        salary_from = []
        salary_to = []
        
        i = 0
        try:
            for i, record in enumerate(loaded_json):
                for j in loaded_json[i]['skills']:
                    skills_data.append([i, j['name']])
                    
                for k in loaded_json[i]['employment_types']:
                    employment_type_data.append([i, k['type']])
                    
                for l in loaded_json[i]['employment_types']:
                    salary_temp.append(l['salary'])
        
                data.append([
                    loaded_json[i]['title'],
                    loaded_json[i]['street'],
                    loaded_json[i]['city'],
                    loaded_json[i]['country_code'],
                    loaded_json[i]['address_text'],
                    loaded_json[i]['marker_icon'],
                    loaded_json[i]['workplace_type'],
                    loaded_json[i]['company_name'],
                    loaded_json[i]['company_size'],
                    loaded_json[i]['experience_level'],
                    loaded_json[i]['latitude'],
                    loaded_json[i]['longitude'],
                    loaded_json[i]['published_at'],
                    loaded_json[i]['remote_interview'],
                    loaded_json[i]['id'],
                    loaded_json[i]['remote'],
                    loaded_json[i]['open_to_hire_ukrainians']])
                i += 1

            
            for i in range(len(salary_temp)):
                try:
                    salary_from.append([i, salary_temp[i]['from']])
                except:
                    salary_from.append([i, np.nan])
                    
            for i in range(len(salary_temp)):
                try:
                    salary_to.append([i, salary_temp[i]['to']])
                except:
                    salary_to.append([i, np.nan])
                    
            df = pd.DataFrame(data,
                            columns=['Title',
                                    'Street',
                                    'City',
                                    'Country Code',
                                    'Address Text',
                                    'Marker Icon',
                                    'Workplace Type',
                                    'Company Name',
                                    'Company Size',
                                    'Experience Level',
                                    'Latitude',
                                    'Longitude',
                                    'Published At',
                                    'Remote interview',
                                    'ID',
                                    'Remote',
                                    'Open to Hire Ukrainians'])
                
            df_skills = pd.DataFrame(skills_data, columns=('Index', "Skills"))
            df_employment_types = pd.DataFrame(employment_type_data, columns=('Index', 'Employment Types'))
            df_from = pd.DataFrame(salary_from, columns=('Index', 'Salary From'))
            df_to = pd.DataFrame(salary_to, columns=('Index', 'Salary To'))
            #df_skills = df_skills.groupby('Index')['Skills'].agg(lambda col: ', '.join(col))
            #df_skills.columns = ['Index', 'Skills']
            df['Index'] = df.index
            merge1 = pd.merge(df, df_skills, on='Index', how='inner')
            merge2 = pd.merge(merge1, df_employment_types, on='Index', how='inner')
            merge3 = pd.merge(merge2, df_from, on='Index', how='inner')
            merge4 = pd.merge(merge3, df_to, on='Index', how='inner')
            
        except Exception as e:
            logger.exception(e)
            
        return merge4

    
    df_converted = create_dataframe()
    df_converted = df_converted.drop_duplicates(subset=['ID'])
    df_converted.to_csv(f'{csv_path}/data {today}.csv', index=False)
    logger.info('Successfully converted to csv')


def transform():
    df = pd.read_csv(f'{csv_path}/data {today}.csv', index_col=False)
    df = df.drop_duplicates(subset=['ID'])
    df['Report Date'] = today
    df['Published At'] = pd.to_datetime(df['Published At'])
    df['Published At'] = df['Published At'].dt.strftime('%Y-%m-%d')
    df['Report Date'] = pd.to_datetime(df['Report Date'])
    df['Salary From'] = df['Salary From'].astype(float).astype('Int64')
    df['Salary To'] = df['Salary To'].astype(float).astype('Int64')
    
    df = df[['Report Date', 'Published At', 'ID', 'Title', 'Street', 'City' , 'Country Code', 
                     'Address Text','Marker Icon', 'Workplace Type', 'Company Name', 
                     'Company Size', 'Experience Level', 'Latitude', 'Longitude' ,
                     'Remote interview', 'Remote', 'Open to Hire Ukrainians', 'Skills',
                     'Employment Types', 'Salary From', 'Salary To']]


    df.to_csv(f'{csv_path}/data {today}.csv', index=False)
    logger.info('Successfully merged data')
    

def start_pipeline():
    start1 = time.time()
    extract()
    end1 = time.time() - start1
    logger.info("Extract took : {} seconds".format(end1))
    logger.info('Extract CPU usage {}%'.format(psutil.cpu_percent()))
    logger.info('RAM memory {}% used'.format(psutil.virtual_memory().percent))

    start2 = time.time()
    transform()
    end2 = time.time() - start2
    logger.info("Merge took : {} seconds".format(end2))
    logger.info('Merge CPU usage {}%'.format(psutil.cpu_percent()))
    logger.info('RAM memory {}% used'.format(psutil.virtual_memory().percent))

    start3 = time.time()
    if run_data_tests(csv_path) == True:
        logger.info("Passed quality tests")
    else:
        logger.info("Failed quality tests, please check quality report")
        
    end3 = time.time() - start3
    logger.info("Quality tests took : {} seconds".format(end3))
    logger.info('Quality test CPU usage {}%'.format(psutil.cpu_percent()))
    logger.info('RAM memory {}% used'.format(psutil.virtual_memory().percent))
    

if __name__ == "__main__":
    start_pipeline()
