import pandas as pd
import re
import numpy as np
from data_extraction import DataExtractor


class DataCleaning:

    def clean_user_data(self, legacy_users_df):
        legacy_users_df['join_date'] = pd.to_datetime(legacy_users_df['join_date'], errors='coerce').dt.date
        legacy_users_df['date_of_birth'] = pd.to_datetime(legacy_users_df['date_of_birth'], errors='coerce').dt.date
        legacy_users_df = legacy_users_df.dropna(subset=['join_date', 'date_of_birth'])
        return legacy_users_df

    def clean_card_data(self, card_df):
# drop any NANs and duplicates
        card_df = card_df.dropna(how='all').drop_duplicates()
        card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], errors='coerce').dt.date
        card_df = card_df.dropna(subset=['date_payment_confirmed'])
        expiry_date_pattern = re.compile(r'^(0[1-9]|1[0-2])/([0-9]{2})$')
        card_df = card_df[card_df['expiry_date'].apply(lambda x: bool(expiry_date_pattern.match(x)))]
        return card_df

    def clean_store_data(self, stores_df):
#  Correct the 'continent' column
        stores_df['continent'] = stores_df['continent'].replace({'eeEurope': 'Europe'})
        valid_continents = ['Europe', 'America']
        stores_df['continent'] = stores_df['continent'].apply(lambda x: x if x in valid_continents else np.nan)
# Removing rows with non-numeric longitude and latitude
        stores_df['longitude'] = pd.to_numeric(stores_df['longitude'], errors='coerce')
        stores_df['latitude'] = pd.to_numeric(stores_df['latitude'], errors='coerce')
#  Handle missing values (fill None with np.nan)
        stores_df = stores_df.replace('N/A', np.nan).replace('', np.nan)
        stores_df = stores_df.applymap(lambda x: np.nan if x in ['K0ODETRLS3', 'K8CXLZDP07', 'UXMWDMX1LC', '3VHFDNP8ET', '9D4LK7X4LZ', 'D23PCWSM6S', '36IIMAQD58', 'NN04B3F6UQ', 'JZP8MIJTPZ', 'B3EH2ZGQAV', '1WZB1TE1HL'] else x)
#  Ensure correct data types
        stores_df['staff_numbers'] = pd.to_numeric(stores_df['staff_numbers'], errors='coerce')
        stores_df['opening_date'] = pd.to_datetime(stores_df['opening_date'], errors='coerce')
#  Drop rows with all NaNs in critical columns
        stores_df = stores_df.dropna(subset=['address', 'longitude', 'latitude', 'locality', 'store_code', 'staff_numbers', 'opening_date', 'store_type', 'country_code', 'continent'])
        return stores_df
    


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
        date_times_df.dropna(how='all').drop_duplicates()
        return date_times_df