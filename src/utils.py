import json
import requests
import pandas as pd
from tqdm import tqdm
from pandas_gbq import to_gbq
from google.cloud import bigquery
import re
import pytz
import os
from datetime import datetime, timedelta

import logging
import logging.handlers

# Set the timezone to Tokyo (UTC+9)
tokyo_tz = pytz.timezone('Asia/Tokyo')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
os.makedirs('logs', exist_ok=True)
logger_file_handler = logging.handlers.RotatingFileHandler(
    os.path.join(os.getcwd(), 'logs', f"{datetime.now(tokyo_tz).strftime('%Y-%m-%d')}.log"),
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)
logger.addHandler(logger_file_handler)

project_id = 'mal-data-engineering'
dataset_id = 'my_anime_list'
anime_info_table_r = 'anime_info'
top_anime_table_r = 'top_airing_anime'
anime_info_table = 'anime_info'
top_anime_table = 'top_airing_anime'


def get_keys():
    logger.info(f'Current Working Directory: {os.getcwd()}')
    # Read the contents of secrets/keys.json
    with open(os.path.join(os.getcwd(), 'secrets', 'keys.json'), 'r') as file:
        keys_data = json.load(file)

    # Log the contents to data/test.log
    logging.info('Contents of keys.json: %s', json.dumps(keys_data, indent=2))
    PATH = os.path.join(os.getcwd(), 'secrets', 'google_api_keys.json')
    try:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH
        logger.info(f'Google Application Credentials Set')
    except:
        logger.info(f'Google Application Credentials NOT Set')
    try:
        PATH = os.path.join(os.getcwd(), 'secrets', 'api_headers.json')
        logger.info(PATH)
        with open(PATH, 'r') as file:
            headers = json.load(file)
        logger.info(f'MAL Headers Loaded')
        return headers
    except:
        logger.info(f'MAL headers NOT Loaded')

def get_top_airing_anime(headers):
    # API endpoint
    url = "https://myanimelist.p.rapidapi.com/anime/top/airing"

    # Make a GET request to the API with the provided headers
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse JSON response
        api_data = response.json()

        # Extract desired fields from the API response
        extracted_data = []
        for entry in api_data:
            anime_data = {
                "date_pulled": datetime.now(tokyo_tz).strftime('%Y-%m-%d'),  # Record the date when data is pulled
                "myanimelist_id": entry.get("myanimelist_id"),
                "title": entry.get("title"),
                "rank": entry.get("rank"),
                "score": entry.get("score")
            }
            extracted_data.append(anime_data)

        # Convert extracted data to a pandas DataFrame
        df = pd.DataFrame(extracted_data)

        return df

    else:
        logger.info("Failed to fetch data from API")
        return None

def get_anime_info(headers, my_anime_ids, debug=False):
    # API URL for anime details
    api_url = "https://myanimelist.p.rapidapi.com/anime/"
    
    # New DataFrame to store flattened data
    flattened_data_df = pd.DataFrame()
    
    # Iterate through my_anime_ids
    for myanimelist_id in tqdm(my_anime_ids):
        # Make API request for detailed anime information
        response = requests.get(f'{api_url}{myanimelist_id}', headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Parse JSON response
            api_data = response.json()
            
            # Flatten the nested JSON structure
            flattened_data = pd.json_normalize(api_data)
            
            # Add 'myanimelist_id' as an additional column
            flattened_data['myanimelist_id'] = myanimelist_id
            
            # Append flattened data to the existing DataFrame
            flattened_data_df = pd.concat([flattened_data_df, flattened_data])
        
        else:
            logger.info(f"Failed to fetch data for myanimelist_id: {myanimelist_id}")

        if debug:
            logger.info('Debug Mode exiting loop after 1 request')
            break
    
    return flattened_data_df

def extract_names_from_list_of_dicts(lst):
    # Extract 'name' values from dictionaries and join them into a string
    return ', '.join([dct['name'] for dct in lst])

def map_dtype_to_bq(dtype):
    # Map DataFrame data types to BigQuery data types
    # Modify this function to handle specific data types as needed
    if dtype == 'int64':
        return 'INTEGER'
    elif dtype == 'float64':
        return 'FLOAT'
    elif dtype == 'bool':
        return 'BOOLEAN'
    elif dtype == 'object':
        return 'STRING'
    # Handle other data types as necessary
    else:
        return 'STRING'  # Default to STRING for unknown data types

def sanitize_column_name(col_name):
    # Replace invalid characters with underscores and make the name lowercase
    return re.sub(r'\W+', '_', col_name.lower())

def update_bigquery_table(project_id, dataset_id, table_id, dataframe):
    # Ensure that dataframe column names are valid
    dataframe.columns = [sanitize_column_name(col) for col in dataframe.columns]
    
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    
    # Check if the table exists
    try:
        table = client.get_table(table_ref)
        logger.info('Table exists')
    except:
        table = None
        logger.info('Table does not exist')
    
    # Get schema from the dataframe
    schema = [bigquery.SchemaField(col, map_dtype_to_bq(dataframe[col].dtype)) for col in dataframe.columns]
    
    if table:
        # Identify missing columns in the existing schema
        existing_columns = set(field.name for field in table.schema)
        new_columns = [field for field in schema if field.name not in existing_columns]

        # Alter the table to add missing columns
        if new_columns:
            updated_schema = table.schema + new_columns
            table.schema = updated_schema
            client.update_table(table, ['schema'])
    else:
        # Create the table with the inferred schema
        table_ref = client.dataset(dataset_id).table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)  # API request
    
    # Append data to the table
    to_gbq(dataframe, f'{dataset_id}.{table_id}', project_id=project_id, if_exists='append')

def check_top_airing_anime_updated(date=None):
    _ = get_keys()
    if date is None:
        date = "CURRENT_DATE('Asia/Tokyo)"
    else:
        date = f"DATE('{date}')"
    try:
        client = bigquery.Client()
        query = f"""
        SELECT date_pulled
        FROM `{project_id}.{dataset_id}.{top_anime_table_r}`
        WHERE DATE(date_pulled) = {date}
        """
        logger.info(query)
        query_job = client.query(query)
        # Convert the result to a list
        results = [row.date_pulled for row in query_job]
        if len(results) == 0:
            return False
        else:
            logger.info(len(results))
            return True
    except:
        return False

def ingest_top_airing_anime():
    headers = get_keys()

    if ~check_top_airing_anime_updated():
        logger.info('Top Airing Anime not yet updated. Pulling from API')
        top_airing = get_top_airing_anime(headers)
        PATH = os.path.join(os.getcwd(),'data')
        os.makedirs(PATH, exist_ok=True)
        PATH = os.path.join(PATH, 'top_airing.parquet')
        top_airing.to_parquet(PATH)
    else:
        logger.info('Top Airing Anime already updated')

def update_top_airing_anime():
    _ = get_keys()
    PATH = os.path.join(os.getcwd(),'data', 'top_airing.parquet')
    top_airing = pd.read_parquet(PATH)
    current_date = top_airing['date_pulled'].max()
    top_airing['top_airing_id'] = top_airing['date_pulled'] + '-' + top_airing['myanimelist_id'].astype(str)
    if check_top_airing_anime_updated(date=current_date) == False:
        # Current date not yet in database, upload
        update_bigquery_table(project_id, dataset_id, top_anime_table, top_airing)
        logger.info(f'Top Airing Anime updated')
    else:
        logger.info(f'Top Airing Anime already updated')

def check_anime_info():
    _ = get_keys()
    # Initialize BigQuery client
    client = bigquery.Client()

    try:
        # Define your query
        query = f"""
        SELECT DISTINCT(taa.myanimelist_id)
        FROM `{project_id}.{dataset_id}.{top_anime_table_r}` AS taa
        LEFT JOIN `{project_id}.{dataset_id}.{anime_info_table_r}` AS ai
        ON taa.myanimelist_id = ai.myanimelist_id
        WHERE DATE(taa.date_pulled) = CURRENT_DATE()
        AND ai.myanimelist_id IS NULL
        """

        # Run the query
        logger.info(query)
        query_job = client.query(query)

        # Convert the result to a list
        results = [row.myanimelist_id for row in query_job]

        logger.info(f'Anime ids not in table yet: {len(results)}')
        return results
    except:
        logger.info(f'Anime Info was not accessed.')
        # If the above fails, anime_info table might not exist get all unique IDs from top_airing_anime, and query it all.
        try:
            # Define your query
            query = f"""
            SELECT DISTINCT(myanimelist_id)
            FROM `{project_id}.{dataset_id}.{top_anime_table_r}` AS taa
            """
            # Run the query
            logger.info(query)
            query_job = client.query(query)

            # Convert the result to a list
            results = [row.myanimelist_id for row in query_job]

            logger.info(f'Anime ids not in table yet: {len(results)}')
            return results
        except:
            logger.info(f'Tables was not accessed.')
            return []

def ingest_anime_info(debug=False):

    headers = get_keys()

    results = check_anime_info()
    anime_info = get_anime_info(headers, results, debug=debug)
    # Cleans columns that are lists of dicts -> just list of names
    anime_info = anime_info.applymap(lambda x: extract_names_from_list_of_dicts(x) if isinstance(x, list) else x)
    
    # If this column is populated that means the anime information is not yet available
    if 'data' in anime_info.columns:
        anime_info = anime_info[anime_info['data'].isna()]
        anime_info.drop(columns=['data'], errors='ignore', inplace=True)

    PATH = os.path.join(os.getcwd(),'data')
    os.makedirs(PATH, exist_ok=True)
    PATH = os.path.join(PATH, 'anime_info.parquet')
    anime_info.to_parquet(PATH)

def update_anime_info():
    _ = get_keys()
    results = check_anime_info()
    PATH = os.path.join(os.getcwd(),'data', 'anime_info.parquet')
    anime_info = pd.read_parquet(PATH)
    anime_info = anime_info.loc[anime_info['myanimelist_id'].isin(results)]
    if anime_info.shape[0] > 0:
        logger.info(f'Anime Info rows to upload:{anime_info.shape[0]}')
        update_bigquery_table(project_id, dataset_id, anime_info_table, anime_info)
    else:
        logger.info('Anime Info already updated')

