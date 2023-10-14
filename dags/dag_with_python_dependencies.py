from datetime import datetime, timedelta
import os
import json
import requests
import pandas as pd
from tqdm import tqdm
from airflow import DAG
from airflow.operators.python import PythonOperator

from pandas_gbq import to_gbq
from google.cloud import bigquery
import re
import pytz

# Set the timezone to Tokyo (UTC+9)
tokyo_tz = pytz.timezone('Asia/Tokyo')


default_args = {
    'owner': 'kiel',
    'start_date': datetime(2023, 10, 14, tzinfo=tokyo_tz),
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}
project_id = 'mal-data-engineering'
dataset_id = 'my_anime_list'
anime_info_table_r = 'anime_info'
top_anime_table_r = 'top_airing_anime'
anime_info_table = 'anime_info'
top_anime_table = 'top_airing_anime'

def get_keys():
    print(f'Current Working Directory: {os.getcwd()}')
    PATH = os.path.join(os.getcwd(), 'secrets', 'mal-data-engineering-6a8e57388653.json')
    try:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH
        print(f'Google Application Credentials Set')
    except:
        print(f'Google Application Credentials NOT Set')
    try:
        PATH = os.path.join(os.getcwd(), 'secrets', 'api_headers.json')
        with open(PATH, 'r') as file:
            headers = json.load(file)
        return headers
        print(f'MAL Headers Loaded')
    except:
        print(f'MAL headers NOT Loaded')

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
                "date_pulled": datetime.now().strftime('%Y-%m-%d'),  # Record the date when data is pulled
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
        print("Failed to fetch data from API")
        return None

def get_anime_info(headers, my_anime_ids):
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
            print(f"Failed to fetch data for myanimelist_id: {myanimelist_id}")
    
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
        print('Table exists')
    except:
        table = None
        print('Table does not exist')
    
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

def ingest_top_airing_anime():
    headers = get_keys()
    
    top_airing = get_top_airing_anime(headers)
    update_bigquery_table(project_id, dataset_id, top_anime_table, top_airing)

def ingest_anime_info():

    headers = get_keys()

    # Initialize BigQuery client
    client = bigquery.Client()

    try:
        # Define your query
        query = f"""
        SELECT taa.myanimelist_id
        FROM `{project_id}.{dataset_id}.{top_anime_table_r}` AS taa
        LEFT JOIN `{project_id}.{dataset_id}.{anime_info_table_r}` AS ai
        ON taa.myanimelist_id = ai.myanimelist_id
        WHERE DATE(taa.date_pulled) = CURRENT_DATE()
        AND ai.myanimelist_id IS NULL
        """

        # Run the query
        print(query)
        query_job = client.query(query)

        # Convert the result to a list
        results = [row.myanimelist_id for row in query_job]

        print(f'Anime ids not in table yet: {len(results)}')
    except:
        print(f'Table was not accessed.')

    # anime_info = get_anime_info(headers, results)
    anime_info = get_anime_info(headers, results)
    # Cleans columns that are lists of dicts -> just list of names
    anime_info = anime_info.applymap(lambda x: extract_names_from_list_of_dicts(x) if isinstance(x, list) else x)
    
    # If this column is populated that means the anime information is not yet available
    if 'data' in anime_info.columns:
        anime_info = anime_info[anime_info['data'].isna()]
        anime_info.drop(columns=['data'], errors='ignore', inplace=True)

    update_bigquery_table(project_id, dataset_id, anime_info_table, anime_info)

with DAG(
    default_args=default_args,
    dag_id="ingest_MAL_data",
    schedule_interval='@daily'
) as dag:

    task_ingest_top_airing_anime = PythonOperator(
        task_id = 'ingest_top_airing_anime',
        python_callable = ingest_top_airing_anime
    )

    task_ingest_anime_info = PythonOperator(
        task_id = 'ingest_anime_info',
        python_callable = ingest_anime_info
    )

    task_ingest_top_airing_anime >> task_ingest_anime_info