import pandas as pd
from data_extraction import DataExtractor

class DataCleaning:
    def __init__(self, db_creds):
        self.extractor = DataExtractor(db_creds)
        self.read = self.extractor.read_rds_table('legacy_users')

    def clean_user_data(self):
        legacy_users_df = self.read.copy()
        
        legacy_users_df['join_date'] = pd.to_datetime(legacy_users_df['join_date'], errors='coerce')
        legacy_users_df['date_of_birth'] = pd.to_datetime(legacy_users_df['date_of_birth'], errors='coerce')
        
        legacy_users_df = legacy_users_df.dropna(subset=['join_date', 'date_of_birth'])
        
        legacy_users_df['join_date'] = legacy_users_df['join_date'].dt.date
        legacy_users_df['date_of_birth'] = legacy_users_df['date_of_birth'].dt.date
        
        return legacy_users_df
