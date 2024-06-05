import pandas as pd
from sqlalchemy import create_engine
import tabula
import requests
import boto3
from io import BytesIO, StringIO

class DataExtractor:
    def __init__(self, db_creds):
        self.engine = self.init_db_engine(db_creds)

    def init_db_engine(self, db_creds):
        engine_str = (f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@"
                      f"{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        return create_engine(engine_str)

    def read_rds_table(self, table_name):
        query = f"SELECT * FROM {table_name};"
        return pd.read_sql_query(query, self.engine)

    def retrieve_pdf_data(self, link):
        try:
            dfs = tabula.read_pdf(link, pages='all')
            dfs = pd.concat(dfs, ignore_index=True)
            for c in dfs.columns:
                try:
                    dfs[c] = pd.to_numeric(dfs[c])
                except (ValueError, TypeError):
                    pass
            return dfs
        except Exception as e:
            print(f"An error occurred while retrieving PDF data: {e}")
            return pd.DataFrame()

    def list_number_of_stores(self, endpoint, headers):
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            return response.json().get('number_of_stores', 0)
        except requests.RequestException as e:
            print(f"An error occurred while fetching the number of stores: {e}")
            return 0

    def retrieve_stores_data(self, endpoint, headers, number_of_stores):
        stores_data = []
        for store_number in range(1, number_of_stores + 1):
            store_endpoint = endpoint.format(store_number=store_number)
            try:
                response = requests.get(store_endpoint, headers=headers)
                response.raise_for_status()
                stores_data.append(response.json())
            except requests.RequestException as e:
                print(f"An error occurred while fetching data for store {store_number}: {e}")
        return pd.DataFrame(stores_data)

    def extract_from_s3(self, s3_address, aws_access_key_id, aws_secret_access_key):
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        bucket, key = s3_address.replace("s3://", "").split("/", 1)
        obj = s3.get_object(Bucket=bucket, Key=key)
        products_data = obj['Body'].read().decode('utf-8')
        products_df = pd.read_csv(StringIO(products_data))
        return products_df

    def extract_json_from_s3(self, s3_address, aws_access_key_id, aws_secret_access_key):
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        bucket, key = s3_address.replace("https://", "").replace("s3://", "").split("/", 1)
        obj = s3.get_object(Bucket=bucket, Key=key)
        json_data = obj['Body'].read()
        data_df = pd.read_json(BytesIO(json_data))
        return data_df
