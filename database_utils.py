import yaml
from sqlalchemy import create_engine, inspect


class DatabaseConnector:

    def read_db_creds(self):
        with open("db_creds.yaml", "r") as file:
         return yaml.safe_load(file)
        
    def init_db_engine(self):
        db_creds = self.read_db_creds()
        engine_str = f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"
        return create_engine(engine_str)
    
    def read_local_creds(self):
       with open("local_cred.yaml", "r") as file_2:
        return yaml.safe_load(file_2)

    def init_local_engine(self):
        local_creds = self.read_local_creds()
        engine_2 = f"postgresql+psycopg2://{local_cred['username']}:{local_cred['password']}@{local_cred['host']}:{local_creds['port']}/{local_cred['database']}"
        return create_engine(engine_2)

    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine) 
        return inspector.get_table_names() 
    
    def upload_to_db(self, legacy_users_df, legacy_users):
       engine = self.init_db_engine()
       legacy_users_df.to_sql(legacy_users, engine, if_exists='replace', index=False)
