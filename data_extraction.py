import pandas as pd
from sqlalchemy import create_engine
import tabula
import requests

class DataExtractor:
    def __init__(self, db_creds):
        self.engine = self.init_db_engine(db_creds)

    def init_db_engine(self, db_creds):
        engine_str = f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"
        return create_engine(engine_str)

    def read_rds_table(self, table_name):
        query = f"SELECT * FROM {table_name};"
        return pd.read_sql_query(query, self.engine)

    def retrieve_pdf_data(self, link):
        try:
            dfs = tabula.read_pdf(link, pages='all')
            dfs = pd.concat(dfs, ignore_index=True)
            return dfs
        except Exception as e:
            print(f"An error occurred while retrieving PDF data: {e}")
            return pd.DataFrame()

    def list_number_of_stores(self, endpoint, headers):
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            store_count = response.json().get('number_of_stores')
            return store_count
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
                store_data = response.json()
                stores_data.append(store_data)
            except requests.RequestException as e:
                print(f"An error occurred while fetching data for store {store_number}: {e}")
                continue
        return pd.DataFrame(stores_data)
