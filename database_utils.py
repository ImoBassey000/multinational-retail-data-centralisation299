import yaml
from sqlalchemy import create_engine, inspect
import os

class DatabaseConnector:
    def __init__(self, config_dir=""):
        self.config_dir = config_dir

    def _read_yaml_file(self, filename):
        filepath = os.path.join(self.config_dir, filename)
        with open(filepath, "r") as file:
            return yaml.safe_load(file)

    def read_db_creds(self):
        return self._read_yaml_file("db_creds.yaml")

    def init_db_engine(self):
        db_creds = self.read_db_creds()
        engine_str = f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"
        return create_engine(engine_str)

    def read_local_creds(self):
        return self._read_yaml_file("local_cred.yaml")

    def init_local_engine(self):
        local_creds = self.read_local_creds()
        engine_str = f"postgresql+psycopg2://{local_creds['username']}:{local_creds['password']}@{local_creds['host']}:{local_creds['port']}/{local_creds['database']}"
        return create_engine(engine_str)

    def read_api_keys(self):
        return self._read_yaml_file("apis_keys.yaml")

    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()

    def upload_to_db(self, df, table_name):
        engine = self.init_local_engine()
        with engine.connect() as connection:
            df.to_sql(table_name, connection, if_exists='replace', index=False)

    def aws_credentials(self):
        return self._read_yaml_file("aws_keys.yaml")

    def s3_credentials(self):
        return self._read_yaml_file("s3_keys.yaml")