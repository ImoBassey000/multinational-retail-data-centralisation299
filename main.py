from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
from data_extraction import DataExtractor


#  Uploading dim_users table to sales_data db
def upload_dim_users():
    # Directory where the config files are located and Instantiate the classes
    config_dir = "/Users/imobassey/Desktop/DevOps/Git/multinational-retail-data-centralisation299/"  
    db_connector = DatabaseConnector(config_dir)
    data_extract = DataExtractor()
    data_cleaner = DataCleaning()
    db_creds = db_connector.read_db_creds() 
    engine = db_connector.init_db_engine()
# list the tables available    
    list_tables = db_connector.list_db_tables(engine)
    print(f"The available tables are", list_tables)
# Reading the data from the legacy_users table
    legacy_users = data_extract.read_rds_table('legacy_users', engine)
# Uploading the clean data to dim_users table in sales_data db    
    legacy_users_cleaned = data_cleaner.clean_user_data(legacy_users)
    db_connector.upload_to_db(legacy_users_cleaned, "dim_users")

# Uploading card data to dim_card_details table in sales_data db
def upload_card_data():
    config_dir = "/Users/imobassey/Desktop/DevOps/Git/multinational-retail-data-centralisation299/"  
    db_connector = DatabaseConnector(config_dir)
    data_extract = DataExtractor()
    data_cleaner = DataCleaning()
    link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    card_df = data_extract.retrieve_pdf_data(link)
    clean_card_data = data_cleaner.clean_card_data(card_df)
    db_connector.upload_to_db(clean_card_data, "dim_card_details")

def upload_store_data():
    config_dir = "/Users/imobassey/Desktop/DevOps/Git/multinational-retail-data-centralisation299/"  
    db_connector = DatabaseConnector(config_dir)
    data_extract = DataExtractor()
    data_cleaner = DataCleaning()
    api_keys = db_connector.read_api_keys()
    number_of_stores = data_extract.list_number_of_stores(api_keys['NUMBER_OF_STORES_ENDPOINT'], api_keys['headers'])
    print(f" The number of stores are: ", number_of_stores)
    stores_df = data_extract.retrieve_stores_data(api_keys['STORE_DETAILS_ENDPOINT'], api_keys['headers'], number_of_stores)
    clean_stores_df = data_cleaner.clean_store_data(stores_df)
    db_connector.upload_to_db(clean_stores_df, 'dim_store_details')

def upload_product_data():
    config_dir = "/Users/imobassey/Desktop/DevOps/Git/multinational-retail-data-centralisation299/"  
    db_connector = DatabaseConnector(config_dir)
    data_extract = DataExtractor()
    data_cleaner = DataCleaning()
    aws_keys = db_connector.aws_credentials()
    s3_address = 's3://data-handling-public/products.csv'
    products_df = data_extract.extract_from_s3(s3_address, aws_keys['aws_access_key_id'], aws_keys['aws_secret_access_key'])
    products_df = data_cleaner.convert_product_weights(products_df)
    clean_products_df = data_cleaner.clean_products_data(products_df)

    db_connector.upload_to_db(clean_products_df, "dim_products")

# def upload_orders_data():
#     config_dir = "/Users/imobassey/Desktop/DevOps/Git/multinational-retail-data-centralisation299/"  
#     db_connector = DatabaseConnector(config_dir)
#     data_extract = DataExtractor()
#     data_cleaner = DataCleaning()
#     engine = db_connector.init_db_engine()
#     orders_df = data_extract.read_rds_table("orders_table", engine)
#     print(orders_df)
#     cleaned_orders_df = data_cleaner.clean_orders_data(orders_df)
#     db_connector.upload_to_db(cleaned_orders_df, "orders_table")

# def upload_date_times_data():
#     date_times_df = data_extract.extract_json_from_s3()
#     cleaned_date_times_df = data_cleaner.clean_date_times_data(date_times_df)
#     db_connector.upload_to_db(cleaned_date_times_df, "dim_date_times")

# # List all tables in the database
# tables = db_connector.list_db_tables(engine)
# print("Tables in the database:", tables)

if __name__ == "__main__":
    upload_dim_users()
    upload_card_data()
    upload_store_data()
    upload_product_data()
    # upload_orders_data()
    # upload_date_times_data()
