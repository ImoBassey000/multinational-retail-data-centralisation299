import pandas as pd
from sqlalchemy import create_engine
import tabula

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
        dfs = tabula.read_pdf(link, pages='all')
        dfs = pd.concat(dfs, ignore_index=True)
        return dfs
