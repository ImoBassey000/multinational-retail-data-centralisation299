SELECT 
    MAX(LENGTH(card_number::text)) AS max_card_number_length,
    MAX(LENGTH(store_code::text)) AS max_store_code_length,
    MAX(LENGTH(product_code::text)) AS max_product_code_length
FROM orders_table;

ALTER TABLE orders_table
  ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
  ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
  ALTER COLUMN card_number TYPE VARCHAR(19),
  ALTER COLUMN store_code TYPE VARCHAR(12),
  ALTER COLUMN product_code TYPE VARCHAR(11),
  ALTER COLUMN product_quantity TYPE SMALLINT;

SELECT MAX(LENGTH(country_code)) AS max_country_code
FROM dim_users;

ALTER TABLE dim_users
  ALTER COLUMN first_name TYPE VARCHAR(255),
  ALTER COLUMN last_name TYPE VARCHAR(255),
  ALTER COLUMN date_of_birth TYPE DATE,
  ALTER COLUMN country_code TYPE VARCHAR(3),
  ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
  ALTER COLUMN join_date TYPE DATE;