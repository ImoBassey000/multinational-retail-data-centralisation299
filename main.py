from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

# Define the directory where the config files are located
config_dir = "/Users/imobassey/Desktop/DevOps/Git/multinational-retail-data-centralisation299/"  # Change this to the correct path

# Load database credentials
db_connector = DatabaseConnector(config_dir)
db_creds = db_connector.read_db_creds()

# Initialize data cleaner
data_cleaner = DataCleaning(db_creds)

# Clean user data and upload
legacy_users_cleaned = data_cleaner.clean_user_data()
db_connector.upload_to_db(legacy_users_cleaned, "dim_users")

# Clean card data and upload
link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
card_df = data_cleaner.extractor.retrieve_pdf_data(link)
clean_card_data = data_cleaner.clean_card_data(card_df)
db_connector.upload_to_db(clean_card_data, "dim_card_details")

# Clean store data
api_keys = db_connector.read_api_keys()
number_of_stores = data_cleaner.extractor.list_number_of_stores(api_keys['number_of_stores_endpoint'], api_keys['headers'])
store_data_df = data_cleaner.extractor.retrieve_stores_data(api_keys['retrieve_store_endpoint'], api_keys['headers'], number_of_stores)
cleaned_store_data_df = data_cleaner.clean_store_data(store_data_df)
db_connector.upload_to_db(cleaned_store_data_df, "dim_store_details")

# Extract, clean, and upload product data
aws_keys = db_connector.aws_credentials()
s3_address = 's3://data-handling-public/products.csv'
products_df = data_cleaner.extractor.extract_from_s3(s3_address, aws_keys['aws_access_key_id'], aws_keys['aws_secret_access_key'])

# Clean product data
products_df = data_cleaner.convert_product_weights(products_df)
clean_products_df = data_cleaner.clean_products_data(products_df)
db_connector.upload_to_db(clean_products_df, "dim_products")

# List all tables in the database
tables = db_connector.list_db_tables()
print("Tables in the database:", tables)

# Extract orders data
orders_df = data_cleaner.extractor.read_rds_table("orders_table")

# Clean orders data
cleaned_orders_df = data_cleaner.clean_orders_data(orders_df)

# Upload cleaned orders data
db_connector.upload_to_db(cleaned_orders_df, "orders_table")

# Step Extract JSON data from S3
my_s3_keys = db_connector.s3_credentials()
s3_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
date_times_df = data_cleaner.extractor.extract_json_from_s3(s3_url, aws_keys['aws_access_key_id'], aws_keys['aws_secret_access_key'])

# Clean JSON data
cleaned_date_times_df = data_cleaner.clean_date_times_data(date_times_df)

# Upload cleaned JSON data to the database
db_connector.upload_to_db(cleaned_date_times_df, "dim_date_times")
