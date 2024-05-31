import yaml
from sqlalchemy import create_engine, MetaData, Table, Column, BigInteger, Text, Date
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

with open("db_creds.yaml", "r") as file:
    db_creds = yaml.safe_load(file)


data_cleaner = DataCleaning(db_creds)
legacy_users_cleaned = data_cleaner.clean_user_data()


db_connector = DatabaseConnector()
db_connector.upload_to_db(legacy_users_cleaned, "Sales['dim_users']")


with open ("local_cred.yaml", 'r') as file_2:
    local_cred = yaml.safe_load(file_2)

engine_2 = create_engine(f"postgresql+psycopg2://{local_cred['username']}:{local_cred['password']}@localhost/{database}')

