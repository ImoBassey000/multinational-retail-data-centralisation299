import pandas as pd
import re
from data_extraction import DataExtractor

class DataCleaning:
    def __init__(self, db_creds):
        self.extractor = DataExtractor(db_creds)
        self.read = self.extractor.read_rds_table('legacy_users')

    def clean_user_data(self):
        """Clean user data from the 'legacy_users' table."""
        legacy_users_df = self.read.copy()

        # Convert join_date and date_of_birth to datetime, dropping any invalid dates
        legacy_users_df['join_date'] = pd.to_datetime(legacy_users_df['join_date'], errors='coerce')
        legacy_users_df['date_of_birth'] = pd.to_datetime(legacy_users_df['date_of_birth'], errors='coerce')

        # Drop rows with NaN values in join_date or date_of_birth
        legacy_users_df = legacy_users_df.dropna(subset=['join_date', 'date_of_birth'])

        # Convert datetime columns to date
        legacy_users_df['join_date'] = legacy_users_df['join_date'].dt.date
        legacy_users_df['date_of_birth'] = legacy_users_df['date_of_birth'].dt.date

        return legacy_users_df

    def clean_card_data(self, card_df):
        """Clean card data from a DataFrame."""
        # Drop rows with all NaN values and remove duplicates
        card_df = card_df.dropna(how='all').drop_duplicates()

        # Convert date_payment_confirmed to datetime, dropping any invalid dates
        card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], errors='coerce')
        card_df = card_df.dropna(subset=['date_payment_confirmed'])

        # Convert datetime column to date
        card_df['date_payment_confirmed'] = card_df['date_payment_confirmed'].dt.date

        # Validate expiry_date format
        expiry_date_pattern = re.compile(r'^(0[1-9]|1[0-2])/([0-9]{2})$')
        valid_expiry_dates = card_df['expiry_date'].apply(lambda x: bool(expiry_date_pattern.match(x)))
        card_df = card_df[valid_expiry_dates]

        return card_df

    def clean_store_data(self, store_df):
        """Clean store data from a DataFrame."""
        # Drop rows with all NaN values and remove duplicates
        store_df = store_df.dropna(how='all').drop_duplicates()
        return store_df

    def convert_product_weights(self, products_df):
        """Convert product weights to a uniform unit (kg)."""

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
                # Handle cases where the string cannot be converted to float
                return None
            return None

        products_df['weight'] = products_df['weight'].apply(convert_weight)
        return products_df

    def clean_products_data(self, products_df):
        """Clean product data from a DataFrame."""
        # Drop rows with all NaN values and remove duplicates
        products_df = products_df.dropna(how='all').drop_duplicates()
        return products_df

    def clean_orders_data(self, orders_df):
        """Clean orders data from a DataFrame."""
        # Remove unnecessary columns
        orders_df = orders_df.drop(columns=['first_name', 'last_name', '1'])
        return orders_df

    def clean_date_times_data(self, date_times_df):
        """Clean date and time data from a DataFrame."""
        # Convert date_time to datetime, dropping any invalid dates
        date_times_df['date_time'] = pd.to_datetime(date_times_df['date_time'], errors='coerce')
        date_times_df = date_times_df.dropna(subset=['date_time'])
        return date_times_df
