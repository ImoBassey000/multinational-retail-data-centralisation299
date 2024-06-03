import yaml
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

with open("db_creds.yaml", "r") as file:
    db_creds = yaml.safe_load(file)
    

data_cleaner = DataCleaning(db_creds)
legacy_users_cleaned = data_cleaner.clean_user_data()

db_connector = DatabaseConnector()
db_connector.upload_to_db(legacy_users_cleaned, "dim_users")

link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
card_dfs = data_cleaner.extractor.retrieve_pdf_data(link)
clean_pdf_card = data_cleaner.clean_card_data(card_dfs)

db_connector.upload_to_db(clean_pdf_card, "dim_card_details")