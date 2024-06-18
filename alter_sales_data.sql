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

SELECT 
MAX(LENGTH(store_code::text)) AS max_storecode,
MAX(LENGTH(country_code)) AS country_code_max
FROM dim_store_details;

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT USING latitude::float,
ALTER COLUMN lat TYPE FLOAT USING lat::float;
UPDATE dim_store_details
SET latitude = COALESCE(latitude, lat);
ALTER TABLE dim_store_details
DROP COLUMN lat;


ALTER TABLE dim_store_details
  ALTER COLUMN longitude TYPE FLOAT USING longitude::float,
  ALTER COLUMN locality TYPE VARCHAR(255),
  ALTER COLUMN store_code TYPE VARCHAR(12),
  ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
  ALTER COLUMN opening_date TYPE DATE,
  ALTER COLUMN store_type TYPE VARCHAR(255),
  ALTER COLUMN latitude TYPE FLOAT USING latitude::float,
  ALTER COLUMN country_code TYPE VARCHAR(3),
  ALTER COLUMN continent TYPE VARCHAR(255);

-- Remove £ character from product_price
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

-- Add weight_class column based on weight ranges
ALTER TABLE dim_products 
ADD COLUMN weight_class VARCHAR(20);

UPDATE TABLE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
    ELSE NULL  
END;
;

UPDATE dim_products
SET Still_available = CASE
    WHEN Still_available = 'Still_available' THEN 'Yes'
    WHEN Still_available = 'Removed' THEN 'No'
    ELSE Still_available
END;

ALTER TABLE dim_products
  ALTER COLUMN product_price TYPE FLOAT,
  ALTER COLUMN weight TYPE FLOAT,
  ALTER COLUMN EAN TYPE VARCHAR(17),
  ALTER COLUMN product_code TYPE VARCHAR(11),
  ALTER COLUMN date_added TYPE DATE,
  ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
  ALTER COLUMN still_available TYPE BOOL USING still_available::boolean,
  ALTER COLUMN weight_class TYPE VARCHAR(14);

SELECT 
	MAX(LENGTH(month::text)) AS max_month,
	MAX(LENGTH(year::text)) AS max_year,
	MAX(LENGTH(day::text)) AS max_day,
	MAX(LENGTH(time_period::text)) AS max_time_period
FROM dim_date_times;


ALTER TABLE dim_date_times
  ALTER COLUMN month TYPE VARCHAR(2),
  ALTER COLUMN year TYPE VARCHAR(4),
  ALTER COLUMN day TYPE VARCHAR(2),
  ALTER COLUMN time_period TYPE VARCHAR(10),
  ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;


SELECT 
	MAX(LENGTH(card_number::text)) AS max_card_number,
	MAX(LENGTH(expiry_date::text)) AS max_expiry_date
FROM dim_card_details;

ALTER TABLE dim_card_details
  ALTER COLUMN card_number TYPE VARCHAR(25),
  ALTER COLUMN expiry_date TYPE VARCHAR(5),
  ALTER COLUMN date_payment_confirmed TYPE DATE
;

--SELECT
--  table_name,
--  constraint_type,
--  constraint_name
--FROM information_schema.table_constraints
--WHERE table_name = 'orders_table';

ALTER TABLE dim_card_details
ADD CONSTRAINT pk_dim_card_details PRIMARY KEY (card_number);
ALTER TABLE dim_date_times
ADD CONSTRAINT pk_dim_date_times PRIMARY KEY (date_uuid);
ALTER TABLE dim_products
ADD CONSTRAINT pk_dim_products PRIMARY KEY (product_code);
ALTER TABLE dim_store_details
ADD CONSTRAINT pk_dim_store_details PRIMARY KEY (store_code);
ALTER TABLE dim_users
ADD CONSTRAINT pk_dim_users PRIMARY KEY (user_uuid);


-- Insert missing product_code values
INSERT INTO dim_products (product_code)
SELECT DISTINCT product_code
FROM orders_table
WHERE product_code NOT IN (SELECT product_code FROM dim_products);

-- Add foreign key constraint on card_number
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_card_number
FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);
-- Add foreign key constraint on date_uuid
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_date_uuid
FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);
-- Add foreign key constraint on product_code
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_product_code
FOREIGN KEY (product_code) REFERENCES dim_products(product_code);
-- Add foreign key constraint on store_code
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_store_code
FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);
-- Add foreign key constraint on user_uuid
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_user_uuid
FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);