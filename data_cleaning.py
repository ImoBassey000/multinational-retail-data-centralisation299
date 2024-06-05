import pandas as pd
import re
from data_extraction import DataExtractor


class DataCleaning:
    def __init__(self, db_creds):
        self.extractor = DataExtractor(db_creds)
        self.read = self.extractor.read_rds_table('legacy_users')

    def clean_user_data(self):
        legacy_users_df = self.read.copy()

        legacy_users_df['join_date'] = pd.to_datetime(legacy_users_df['join_date'], errors='coerce').dt.date
        legacy_users_df['date_of_birth'] = pd.to_datetime(legacy_users_df['date_of_birth'], errors='coerce').dt.date

        legacy_users_df = legacy_users_df.dropna(subset=['join_date', 'date_of_birth'])

        return legacy_users_df

    def clean_card_data(self, card_df):
        card_df = card_df.dropna(how='all').drop_duplicates()
        card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], errors='coerce').dt.date
        card_df = card_df.dropna(subset=['date_payment_confirmed'])

        expiry_date_pattern = re.compile(r'^(0[1-9]|1[0-2])/([0-9]{2})$')
        card_df = card_df[card_df['expiry_date'].apply(lambda x: bool(expiry_date_pattern.match(x)))]

        return card_df

    def clean_store_data(self, store_df):
        return store_df.dropna(how='all').drop_duplicates()

    def convert_product_weights(self, products_df):
        def convert_weight(value):
            if pd.isnull(value):
                return value
            value = value.lower().replace(' ', '')
            try:
                if 'kg' in value:
                    return float(value.replace('kg', ''))
                if 'g' in value:
                    return float(value.replace('g', '')) / 1000
                if 'ml' in value:
                    return float(value.replace('ml', '')) / 1000
                if 'l' in value:
                    return float(value.replace('l', ''))
            except ValueError:
                return None
            return None

        products_df['weight'] = products_df['weight'].apply(convert_weight)
        return products_df

    def clean_products_data(self, products_df):
        return products_df.dropna(how='all').drop_duplicates()

    def clean_orders_data(self, orders_df):
        return orders_df.drop(columns=['first_name', 'last_name', '1'])

    def clean_date_times_data(self, date_times_df):
        date_times_df['date_time'] = pd.to_datetime(date_times_df['date_time'], errors='coerce')
        return date_times_df.dropna(subset=['date_time'])
