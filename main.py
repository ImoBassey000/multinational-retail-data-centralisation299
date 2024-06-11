from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

# Directory where the config files are located
config_dir = "/Users/imobassey/Desktop/DevOps/Git/multinational-retail-data-centralisation299/"  

# Instaciate the class 
db_connector = DatabaseConnector(config_dir)
data_extract = DataExtractor()
# data_cleaner = DataCleaning(db_creds)

# instaciate the connection and list the tables from rds
my_engine = db_connector.init_db_engine()
db_read_table = db_connector.list_db_tables(my_engine)
print(db_read_table)


# reading legacy_users table from rds
legacy_userss = data_extract.read_rds_table('legacy_users', my_engine)
print(legacy_userss)


# Clean user data and upload
# legacy_users_cleaned = data_cleaner.clean_user_data()
# db_connector.upload_to_db(legacy_users_cleaned, "dim_users")

# Clean card data and upload
# link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
# card_df = data_cleaner.extractor.retrieve_pdf_data(link)
# clean_card_data = data_cleaner.clean_card_data(card_df)
# db_connector.upload_to_db(clean_card_data, "dim_card_details")


# Extract, clean store data and upload
api_keys = db_connector.read_api_keys()
print(api_keys)
print(api_keys['headers'])

endpoint = api_keys['NUMBER_OF_STORES_ENDPOINT']
headers = api_keys['headers']
number_of_stores = data_extract.list_number_of_stores(endpoint, headers)
print(number_of_stores)

# Retrieve stores data
endpoint_2 = api_keys['STORE_DETAILS_ENDPOINT']
stores_df = data_extract.retrieve_stores_data(endpoint_2, headers, number_of_stores)
print(stores_df)
# clean_stores_df = data_cleaner.clean_store_data(stores_df)
# db_connector.upload_to_db(clean_stores_df, 'dim_store_details')

# ## Extract, clean, and upload product data
# aws_keys = db_connector.aws_credentials()
# s3_address = 's3://data-handling-public/products.csv'
# products_df = data_cleaner.extractor.extract_from_s3(s3_address, aws_keys['aws_access_key_id'], aws_keys['aws_secret_access_key'])

# # Clean product data
# products_df = data_cleaner.convert_product_weights(products_df)
# clean_products_df = data_cleaner.clean_products_data(products_df)
# db_connector.upload_to_db(clean_products_df, "dim_products")

# # List all tables in the database
# tables = db_connector.list_db_tables()
# print("Tables in the database:", tables)

# # Extract, clean, and upload orders data
# orders_df = data_cleaner.extractor.read_rds_table("orders_table")
# cleaned_orders_df = data_cleaner.clean_orders_data(orders_df)
# db_connector.upload_to_db(cleaned_orders_df, "orders_table")

# # Extract JSON data from S3
# date_times_df = data_cleaner.extractor.extract_json_from_s3()

# # Clean and upload JSON data
# cleaned_date_times_df = data_cleaner.clean_date_times_data(date_times_df)
# db_connector.upload_to_db(cleaned_date_times_df, "dim_date_times")
