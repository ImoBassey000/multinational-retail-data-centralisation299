import yaml
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

with open("db_creds.yaml", "r") as file:
    db_creds = yaml.safe_load(file)
    
    
with open("local_cred.yaml", "r") as file_2:
    local_konet = yaml.safe_load(file_2)

    def init_local_engine(self):
        local_creds = self.read_local_creds()
        engine_2 = f"postgresql+psycopg2://{local_cred['username']}:{local_cred['password']}@{local_cred['host']}:{local_creds['port']}/{local_cred['database']}"
        return create_engine(engine_2)

data_cleaner = DataCleaning(db_creds)
legacy_users_cleaned = data_cleaner.clean_user_data()

db_connector = DatabaseConnector()
db_connector.upload_to_db(legacy_users_cleaned, "Sales['dim_users']")

