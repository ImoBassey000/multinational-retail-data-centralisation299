import yaml
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

with open("db_creds.yaml", "r") as file:
    db_creds = yaml.safe_load(file)


data_cleaner = DataCleaning(db_creds)
legacy_users_cleaned = data_cleaner.clean_user_data()

db_connector = DatabaseConnector()
db_connector.upload_to_db(legacy_users_cleaned, "Sales['dim_users']")

