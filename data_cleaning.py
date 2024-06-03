import pandas as pd
from data_extraction import DataExtractor
import re

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

    def clean_card_data(self, card_dfs):
        card_dfs = card_dfs.dropna(how='all')
        card_dfs = card_dfs.drop_duplicates()
        card_dfs['date_payment_confirmed'] = pd.to_datetime(card_dfs['date_payment_confirmed'], errors='coerce')
        card_dfs = card_dfs.dropna(subset=['date_payment_confirmed'])
        card_dfs['date_payment_confirmed'] = card_dfs['date_payment_confirmed'].dt.date

        expiry_date_pattern = re.compile(r'^(0[1-9]|1[0-2])/([0-9]{2})$')
        valid_expiry_dates = card_dfs['expiry_date'].apply(lambda x: bool(expiry_date_pattern.match(x)))
        card_dfs = card_dfs[valid_expiry_dates]

        return card_dfs

    def clean_store_data(self, store_df):
        store_df = store_df.dropna(how='all')
        store_df = store_df.drop_duplicates()
        return store_df

