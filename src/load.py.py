import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    return create_engine(os.getenv("DATABASE_URL"))


MAIN_TABLE = """
                    CREATE TABLE IF NOT EXISTS dim_date(
                        order_date TIMESTAMP, 
                        date_key BIGINT PRIMARY KEY, 
                        day BIGINT, 
                        month BIGINT,  
                        year BIGINT, 
                        is_weekend BOOLEAN
                        );

                    CREATE TABLE IF NOT EXISTS dim_geolocation(
                        geolocation_zip_code_prefix VARCHAR PRIMARY KEY,	
                        geolocation_lat	FLOAT, 
                        geolocation_lng FLOAT, 
                        geolocation_city TEXT,
                        geolocation_state TEXT
                        );

                    CREATE TABLE IF NOT EXISTS dim_product_name(
                        product_category_name TEXT PRIMARY KEY,
                        product_category_name_english TEXT
                        );
                    
                    CREATE TABLE IF NOT EXISTS dim_products(
                        product_id VARCHAR PRIMARY KEY, 
                        product_category_name TEXT,
                        product_name_lenght	FLOAT,
                        product_description_lenght FLOAT,	
                        product_photos_qty FLOAT,
                        product_weight_g FLOAT,
                        product_length_cm FLOAT,	
                        product_height_cm FLOAT, 
                        product_width_cm FLOAT,
                        product_category_name_english TEXT
                        );
                    
                    CREATE TABLE IF NOT EXISTS dim_customers(
                        customer_id	VARCHAR PRIMARY KEY,
                        customer_unique_id VARCHAR, 
                        customer_zip_code_prefix VARCHAR,
                        customer_city TEXT,
                        customer_state TEXT
                        );

                    CREATE TABLE IF NOT EXISTS dim_sellers(
                        seller_id VARCHAR PRIMARY KEY,
                        seller_zip_code_prefix VARCHAR,
                        seller_city	TEXT,
                        seller_state TEXT
                        );

                    CREATE TABLE IF NOT EXISTS dim_orders(
                        order_id VARCHAR PRIMARY KEY,
                        customer_id	VARCHAR REFERENCES dim_customers(customer_id),
                        order_status TEXT,
                        order_purchase_timestamp TIMESTAMP,	
                        order_approved_at TIMESTAMP,
                        order_delivered_carrier_date TIMESTAMP,	
                        order_delivered_customer_date TIMESTAMP,
                        order_estimated_delivery_date TIMESTAMP,
                        delivery_time_days INT
                        );

                    CREATE TABLE IF NOT EXISTS dim_orders_details(
                        review_id VARCHAR,
                        order_id VARCHAR NOT NULL REFERENCES dim_orders(order_id),
                        review_score INT,
                        review_comment_title TEXT,
                        review_comment_message TEXT,
                        review_creation_date TIMESTAMP,
                        review_answer_timestamp TIMESTAMP,
                        PRIMARY KEY (review_id, order_id),
                        UNIQUE (review_id)
                        );

                    CREATE TABLE IF NOT EXISTS dim_order_payments(
                        order_id VARCHAR REFERENCES dim_orders(order_id),
                        payment_sequential BIGINT, 
                        payment_type TEXT,
                        payment_installments BIGINT, 
                        payment_value FLOAT,
                        PRIMARY KEY (order_id, payment_sequential)
                        );
                    
                    CREATE TABLE IF NOT EXISTS fact_order_items(
                        order_id VARCHAR REFERENCES dim_orders(order_id),
                        order_item_id BIGINT,
                        product_id VARCHAR REFERENCES dim_products(product_id),
                        seller_id VARCHAR REFERENCES dim_sellers(seller_id),
                        review_id VARCHAR REFERENCES dim_orders_details(review_id),
                        shipping_limit_date TIMESTAMP,	
                        price FLOAT,
                        freight_value FLOAT,
                        total_item_cost FLOAT,
                        order_purchase_timestamp TIMESTAMP,
                        date_key BIGINT,
                        PRIMARY KEY (order_id, order_item_id)
                        );
"""

STAGING_TABLE = """
                    CREATE TABLE IF NOT EXISTS stage_date(
                        order_date TIMESTAMP, 
                        date_key BIGINT, 
                        day BIGINT, 
                        month BIGINT,  
                        year BIGINT, 
                        is_weekend BOOLEAN
                        );

                    CREATE TABLE IF NOT EXISTS stage_geolocation(
                        geolocation_zip_code_prefix VARCHAR,	
                        geolocation_lat	FLOAT, 
                        geolocation_lng FLOAT, 
                        geolocation_city TEXT,
                        geolocation_state TEXT
                        );

                    CREATE TABLE IF NOT EXISTS stage_product_name(
                        product_category_name TEXT,
                        product_category_name_english TEXT
                        );
                    
                    CREATE TABLE IF NOT EXISTS stage_products(
                        product_id VARCHAR, 
                        product_category_name TEXT,
                        product_name_lenght	FLOAT,
                        product_description_lenght FLOAT,	
                        product_photos_qty FLOAT,
                        product_weight_g FLOAT,
                        product_length_cm FLOAT,	
                        product_height_cm FLOAT, 
                        product_width_cm FLOAT,
                        product_category_name_english TEXT
                        );
                    
                    CREATE TABLE IF NOT EXISTS stage_customers(
                        customer_id	VARCHAR,
                        customer_unique_id VARCHAR, 
                        customer_zip_code_prefix VARCHAR,
                        customer_city TEXT,
                        customer_state TEXT
                        );

                    CREATE TABLE IF NOT EXISTS stage_sellers(
                        seller_id VARCHAR,
                        seller_zip_code_prefix VARCHAR,
                        seller_city	TEXT,
                        seller_state TEXT
                        );

                    CREATE TABLE IF NOT EXISTS stage_orders(
                        order_id VARCHAR PRIMARY KEY,
                        customer_id	VARCHAR,
                        order_status TEXT,
                        order_purchase_timestamp TIMESTAMP,	
                        order_approved_at TIMESTAMP,
                        order_delivered_carrier_date TIMESTAMP,	
                        order_delivered_customer_date TIMESTAMP,
                        order_estimated_delivery_date TIMESTAMP,
                        delivery_time_days INT
                        );
                    
                    CREATE TABLE IF NOT EXISTS stage_orders_details(
                        review_id VARCHAR,
                        order_id VARCHAR,
                        review_score BIGINT,
                        review_comment_title TEXT,
                        review_comment_message TEXT,	
                        review_creation_date TIMESTAMP,
                        review_answer_timestamp TIMESTAMP
                        );

                    CREATE TABLE IF NOT EXISTS stage_order_payments(
                        order_id VARCHAR,
                        payment_sequential BIGINT, 
                        payment_type TEXT,
                        payment_installments BIGINT, 
                        payment_value FLOAT
                        );
                    
                    CREATE TABLE IF NOT EXISTS stage_order_items(
                        order_id VARCHAR,
                        order_item_id BIGINT,
                        product_id VARCHAR,
                        seller_id VARCHAR,
                        review_id VARCHAR,
                        shipping_limit_date TIMESTAMP,	
                        price FLOAT,
                        freight_value FLOAT,
                        total_item_cost FLOAT,
                        order_purchase_timestamp TIMESTAMP,	
                        date_key BIGINT
                        );

"""

merge_dim = """
                INSERT INTO dim_date(order_date, date_key, day, month, year, is_weekend)
                SELECT order_date, date_key, day, month, year, is_weekend 
                FROM stage_date
                ON CONFLICT (date_key)
                DO NOTHING;

                
                INSERT INTO dim_geolocation(geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city ,geolocation_state)
                SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city ,geolocation_state 
                FROM stage_geolocation
                ON CONFLICT (geolocation_zip_code_prefix) 
                DO NOTHING;

                
                INSERT INTO dim_product_name(product_category_name, product_category_name_english)
                SELECT product_category_name, product_category_name_english 
                FROM stage_product_name
                ON CONFLICT (product_category_name)
                DO NOTHING;

                
                INSERT INTO dim_products(product_id, 
                            product_category_name,
                            product_name_lenght,
                            product_description_lenght,	
                            product_photos_qty,
                            product_weight_g,
                            product_length_cm,	
                            product_height_cm, 
                            product_width_cm,
                            product_category_name_english)
                SELECT DISTINCT ON (product_id) product_id,
                            product_category_name,
                            product_name_lenght,
                            product_description_lenght,	
                            product_photos_qty,
                            product_weight_g,
                            product_length_cm,	
                            product_height_cm, 
                            product_width_cm,
                            product_category_name_english 
                            FROM stage_products
                ON CONFLICT (product_id)
                DO UPDATE SET 
                product_category_name = EXCLUDED.product_category_name,
                product_name_lenght = EXCLUDED.product_name_lenght,
                product_description_lenght = EXCLUDED.product_description_lenght,
                product_photos_qty = EXCLUDED.product_photos_qty,
                product_weight_g = EXCLUDED.product_weight_g,
                product_length_cm = EXCLUDED.product_length_cm,
                product_height_cm = EXCLUDED.product_height_cm,
                product_width_cm = EXCLUDED.product_width_cm,
                product_category_name_english = EXCLUDED.product_category_name_english;


                INSERT INTO dim_customers(customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
                SELECT DISTINCT ON (customer_id) customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state 
                FROM stage_customers
                ON CONFLICT (customer_id)
                DO UPDATE SET 
                customer_zip_code_prefix = EXCLUDED.customer_zip_code_prefix,
                customer_city = EXCLUDED.customer_city,
                customer_state = EXCLUDED.customer_state;


                INSERT INTO dim_sellers(seller_id,
                            seller_zip_code_prefix,
                            seller_city,
                            seller_state)
                SELECT DISTINCT ON (seller_id)
                            seller_id,
                            seller_zip_code_prefix,
                            seller_city,
                            seller_state FROM stage_sellers
                ON CONFLICT (seller_id)
                DO UPDATE SET
                seller_zip_code_prefix = EXCLUDED.seller_zip_code_prefix,
                seller_city = EXCLUDED.seller_city,
                seller_state = EXCLUDED.seller_state;


                INSERT INTO dim_orders(
                    order_id, customer_id, order_status, 
                    order_purchase_timestamp, order_approved_at, 
                    order_delivered_carrier_date, order_delivered_customer_date, 
                    order_estimated_delivery_date
                )
                SELECT 
                    order_id, customer_id, order_status, 
                    order_purchase_timestamp::timestamp,
                    order_approved_at::timestamp,
                    order_delivered_carrier_date::timestamp, 
                    order_delivered_customer_date::timestamp, 
                    order_estimated_delivery_date::timestamp 
                FROM stage_orders
                ON CONFLICT (order_id)
                DO UPDATE SET order_status = EXCLUDED.order_status;

                
                INSERT INTO dim_orders_details(
                            review_id,
                            order_id,
                            review_score,
                            review_comment_title,
                            review_comment_message,	
                            review_creation_date,
                            review_answer_timestamp)
                SELECT review_id, 
                            order_id, 
                            review_score, 
                            review_comment_title, 
                            review_comment_message, 
                            review_creation_date::timestamp, 
                            review_answer_timestamp::timestamp
                FROM stage_orders_details
                ON CONFLICT (review_id, order_id)
                DO UPDATE SET
                            review_score = EXCLUDED.review_score,
                            review_comment_title = EXCLUDED.review_comment_title,
                            review_comment_message = EXCLUDED.review_comment_message,
                            review_creation_date = EXCLUDED.review_creation_date,
                            review_answer_timestamp = EXCLUDED.review_answer_timestamp;

                            
                INSERT INTO dim_order_payments(order_id,
                            payment_sequential, 
                            payment_type,
                            payment_installments, 
                            payment_value)
                SELECT order_id,
                            payment_sequential, 
                            payment_type,
                            payment_installments, 
                            payment_value FROM stage_order_payments
                            ON CONFLICT (order_id, payment_sequential)
                            DO NOTHING;     
    """


FACT_MERGING = """                        
                    INSERT INTO fact_order_items(
                            order_id,
                            order_item_id,
                            product_id,
                            seller_id,
                            shipping_limit_date,
                            price,
                            freight_value,
                            total_item_cost,	
                            order_purchase_timestamp,	
                            date_key)
                    SELECT order_id,
                            order_item_id,
                            product_id,
                            seller_id,
                            shipping_limit_date,	
                            price,
                            freight_value,	
                            total_item_cost,	
                            order_purchase_timestamp::timestamp,	
                            date_key
                    FROM stage_order_items
                    ON CONFLICT (order_id, order_item_id)
                    DO NOTHING;
"""

CLEARING_STAGE = """
        TRUNCATE TABLE stage_date, 
        stage_geolocation, 
        stage_product_name, 
        stage_products, 
        stage_customers, 
        stage_sellers, 
        stage_orders, 
        stage_orders_details, 
        stage_order_payments, 
        stage_order_items;
""" 

