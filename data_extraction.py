import pandas as pd
from sqlalchemy import create_engine
import tabula
import requests
import boto3
from io import BytesIO, StringIO


class DataExtractor:

    def read_rds_table(self, table_name, engine):
        query = f"SELECT * FROM {table_name};"
        return pd.read_sql_query(query, engine)

    def retrieve_pdf_data(self, link):
        try:
            dfs = tabula.read_pdf(link, pages='all')
            dfs = pd.concat(dfs, ignore_index=True)
            dfs = dfs.apply(pd.to_numeric, errors='ignore')
            return dfs
        except Exception as e:
            print(f"An error occurred while retrieving PDF data: {e}")
            return pd.DataFrame()

    def list_number_of_stores(self, NUMBER_OF_STORES_ENDPOINT, headers):
        response = requests.get(NUMBER_OF_STORES_ENDPOINT, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data['number_stores']
        else:
            response.raise_for_status()

    def retrieve_stores_data(self, STORE_DETAILS_ENDPOINT, headers, number_of_stores):
        stores_data = []
        for store_number in range(1, number_of_stores + 1):
            url = STORE_DETAILS_ENDPOINT.format(store_number=store_number)
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                store_data = response.json()
                stores_data.append(store_data)
            else:
                response.raise_for_status()
        pd.DataFrame(stores_data)
    

    def extract_from_s3(self, s3_address, aws_access_key_id, aws_secret_access_key):
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        bucket, key = s3_address.replace("s3://", "").split("/", 1)
        obj = s3.get_object(Bucket=bucket, Key=key)
        products_data = obj['Body'].read().decode('utf-8')
        return pd.read_csv(StringIO(products_data))

    def extract_json_from_s3(self):
        date_times_df = pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
        return date_times_df
