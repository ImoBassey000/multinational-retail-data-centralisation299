from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

# Load database credentials
db_connector = DatabaseConnector()
db_creds = db_connector.read_db_creds()

# Initialize data cleaner
data_cleaner = DataCleaning(db_creds)

# Clean user data and upload
legacy_users_cleaned = data_cleaner.clean_user_data()
db_connector.upload_to_db(legacy_users_cleaned, "dim_users")

# Clean card data and upload
link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
card_dfs = data_cleaner.extractor.retrieve_pdf_data(link)
clean_pdf_card = data_cleaner.clean_card_data(card_dfs)
db_connector.upload_to_db(clean_pdf_card, "dim_card_details")

# Clean store data
api_keys = db_connector.read_api_keys()
number_of_stores = data_cleaner.extractor.list_number_of_stores(api_keys['number_of_stores_endpoint'], api_keys['headers'])
store_data_df = data_cleaner.extractor.retrieve_stores_data(api_keys['retrieve_store_endpoint'], api_keys['headers'], number_of_stores)
cleaned_store_data_df = data_cleaner.clean_store_data(store_data_df)
db_connector.upload_to_db(cleaned_store_data_df, "dim_store_details")
