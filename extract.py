import requests
import pandas as pd
from datetime import datetime
import uuid
from log import log_etl
from connection_str import engine

def extract(run_id):
    try:
        # 325
        #api_link = 'https://api.apify.com/v2/datasets/dhH9TbM1aIAO7CGDZ/items?token=apify_api_b0FBQehuRkyzfeMLejiNClPx61w5Af13Gbi5'
        
        # 100
        #api_link = 'https://api.apify.com/v2/datasets/0KcpJr49vsU7LEObx/items?token=apify_api_ySeiFojrzWpLmixvddqazIVXZQa1Mb2cARZR'
        
        # 175
        api_link = 'https://api.apify.com/v2/datasets/eb11Ac2uUqrYbWzuD/items?token=apify_api_ySeiFojrzWpLmixvddqazIVXZQa1Mb2cARZR'

        start_time = datetime.now()
        
        response = requests.get(api_link)
        response.raise_for_status()
        data = response.json()
        end_time = datetime.now()
        
        log_etl(run_id, 'extract', 'success', count_rows=len(data), start = start_time, end = end_time)
        return data
    except requests.exceptions.RequestException as e:
        log_etl(run_id, 'extract', 'failed', error_msg=str(e))
        return None


def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], name + a + '_')
        elif isinstance(x, list):
            for i, a in enumerate(x):
                flatten(a, name + str(i) + '_')
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def normalize_nested_json(data):
    flattened_data = [flatten_json(record) for record in data]
    return pd.DataFrame(flattened_data)

def save_to_staging_area():
    try:
        start_time = datetime.now()
        # create batch_id 
        run_id = uuid.uuid4()
        
        # for transform just extract from the 
        
        raw_data = extract(run_id)
        
        raw_df = pd.DataFrame(raw_data)
        
        selected_columns = ['id', 'titleText', 'titleType', 'certificate', 'releaseDate', 'runtime', 'ratingsSummary', 'genres', 'directors', 'productionBudget', 'worldwideGross']
        selected_df = raw_df[selected_columns]
        
        # converting selected_df to dictionary as normalize_nested_json takes dictionary
        selected_dictionary = selected_df.to_dict(orient='records')
        
        normalized_df = normalize_nested_json(selected_dictionary)
        
        
        normalized_df['run_id'] = run_id
        
        # create created_at
        normalized_df['created_at'] = datetime.now()

        # save to staging table
        normalized_df.to_sql('staging_table', con=engine, if_exists='append', index=False)
        
        end_time = datetime.now()
        
        log_etl(run_id, 'save_to_staging_area', 'success', count_rows=len(normalized_df), start = start_time, end = end_time)
    
    except Exception as e:
        log_etl(run_id,'save_to_staging_area', 'failed', error_msg=str(e))
    
    return normalized_df