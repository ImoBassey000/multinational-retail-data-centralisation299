import yaml
from sqlalchemy import create_engine, MetaData, Table, Column, BigInteger, Text, Date


class ConnectLocal:

    def read_local_creds(self):
        with open("local_cred.yaml", "r") as file_2:
         return yaml.safe_load(file_2)

    def init_local_engine(self):
        local_creds = self.read_local_creds()
        engine_2 = f"postgresql+psycopg2://{local_cred['username']}:{local_cred['password']}@{local_cred['host']}:{local_creds['port']}/{local_cred['database']}"
        return create_engine(engine_2)