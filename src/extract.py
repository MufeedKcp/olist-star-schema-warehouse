import pandas as pd
import os
import logging 
import time
from sqlalchemy import create_engine
from sqlalchemy import text
from dotenv import load_dotenv
load_dotenv()

"""calling my database connection"""
DATABASE_URL = os.getenv("DATABASE_URL")

"""connecting to PostgreSQL"""
engine = create_engine(DATABASE_URL)

"""mapping the file and folder"""
DATA_FOLDER = 'brazil_sales_datasets'
INGESTING_FILE = {
    'olist_customers_dataset.csv': 'customers', 
    'olist_geolocation_dataset.csv': 'geolocation', 
    'olist_order_items_dataset.csv': 'order_items', 
    'olist_order_payments_dataset.csv': 'order_payment', 
    'olist_order_reviews_dataset.csv': 'order_review', 
    'olist_orders_dataset.csv': 'orders', 
    'olist_products_dataset.csv': 'products', 
    'olist_sellers_dataset.csv': 'sellers', 
    'product_category_name_translation.csv': 'category'
}

logging.basicConfig(filename='log.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s - %(name)s')


def extract_raw_data(data_folder, file_mapping):
    """re-usable extract function"""
    extracted_data = {}
    """EVERY FILE I OPEN SHOULD GO INTO extracted_data = {}"""

    for file_name, table_name in file_mapping.items():
        file_path = os.path.join(data_folder, file_name)

        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
        
                if not df.empty:
                    extracted_data[table_name] = df
                    logging.info(f'succesfully extracted {len(df)} rows')
                
                else:
                    logging.warning(f'{file_name} is empty')
        
            except Exception as e:
                logging.error(f'Error extracting {file_name}: {e}')
        else:
            logging.error(f'File {file_name} not found')
    return extracted_data

"""created a transformation functions for each file"""
def transformer_geolocation(geo_df):
    logging.info('Cleaning Geolocation data.......')

    geo_df['geolocation_city'] = geo_df['geolocation_city'].str.normalize('NFKD')\
        .str.encode('ascii', errors='ignore').str.decode('utf-8').str.strip().str.lower()

    cleaned_geo = geo_df.groupby(['geolocation_zip_code_prefix']).agg({
        'geolocation_lat': 'mean', 
        'geolocation_lng': 'mean', 
        'geolocation_city': 'first', 
        'geolocation_state': 'first'}).reset_index()

    logging.warning(f"Geolocation cleaned. Rows reduced from {len(geo_df)} to {len(cleaned_geo)}")
    return cleaned_geo


def transformer_customers(cust_df):
    logging.info('Cleaning Customer data.......')

    cust_df['customer_zip_code_prefix'] = cust_df['customer_zip_code_prefix'].astype(str)
    cust_df['customer_state'] = cust_df['customer_state'].str.strip().str.upper()
    cust_df['customer_city'] = cust_df['customer_city'].str.normalize('NFKD')\
        .str.encode('ascii', errors='ignore').str.decode('utf-8').str.strip().str.lower()
    cust_clean = cust_df.dropna(subset=['customer_unique_id'])

    logging.info(f"Customers cleaned: Zip codes preserved, cities standardized. Total: {len(cust_df)} rows.")
    return cust_clean.drop_duplicates(subset=['customer_id'])


def tranformer_products(pro_df):
    initial_pro_count = len(pro_df)
    logging.info('Cleaning Product data.......')

    pro_df['product_category_name'] = pro_df['product_category_name'].fillna('unknown')
    pro_df[
        ['product_name_lenght', 
         'product_description_lenght', 
         'product_photos_qty', 
         'product_weight_g', 
         'product_length_cm', 
         'product_height_cm', 
         'product_width_cm']] = pro_df[
                  ['product_name_lenght', 
                   'product_description_lenght',
                   'product_photos_qty', 
                   'product_weight_g', 
                   'product_length_cm', 
                   'product_height_cm', 
                   'product_width_cm']
                   ].astype(float)
    
    cat_grp = pro_df.groupby(['product_category_name'])    
    filt = cat_grp[['product_name_lenght', 
                    'product_description_lenght', 
                    'product_photos_qty', 
                    'product_weight_g', 
                    'product_length_cm', 
                    'product_height_cm', 
                    'product_width_cm']].median(numeric_only=True)
    cleaned_pro_df = pro_df.fillna(filt)
    
    logging.warning(f"Product dropped {initial_pro_count - len(cat_grp)} rows with grouping product_category.\
 Products cleaned: Missing categories set to 'unknown'. Dimensions converted to float. Total: {len(pro_df)} rows.")
    return cleaned_pro_df.drop_duplicates(subset=['product_id'])


def transformer_seller(seller_df):
    logging.info('Cleaning Seller data.......')
    
    seller_df['seller_zip_code_prefix'] = seller_df['seller_zip_code_prefix'].astype(str)
    seller_df['seller_city'] = seller_df['seller_city'].str.normalize('NFKD').str.encode('ascii', errors='ignore')\
        .str.decode('utf-8').str.strip().str.lower()
    seller_df['seller_state'] = seller_df['seller_state'].str.upper()
    cleaned_seller_df = seller_df

    logging.info(f"Sellers cleaned: Zip codes cast to string and states standardized. Total: {len(seller_df)} rows.")
    return cleaned_seller_df.drop_duplicates(subset=['seller_id'])


def transformer_order_review(review_df):
    logging.info('Cleaning Order review data.......')

    review_df = review_df.copy()
    review_df = review_df.drop_duplicates(subset=['review_id'], keep='first')
    review_df['review_answer_timestamp'] = pd.to_datetime(review_df['review_answer_timestamp'])
    review_df['review_creation_date'] = pd.to_datetime(review_df['review_creation_date'])
    review_df[['review_comment_title', 'review_comment_message']] = review_df[['review_comment_title', 'review_comment_message']].fillna('no comment provided')
    cleaned_review = review_df
    
    logging.info(f"Order Reviews cleaned: Missing comments filled with 'No comment'. Total: {len(review_df)} rows.")
    return cleaned_review


def transformer_payment(pay_df):
    logging.info('Cleaning order payment data.......')
    
    pay_df['payment_type'] = pay_df['payment_type'].str.lower()
    cleaned_payment = pay_df
    
    logging.info(f"Order Payments cleaned: Payment types lowercased and types cast to float. Total: {len(pay_df)} rows.")
    return cleaned_payment


def transformer_items(item_df):
    logging.info('Cleaning Order items data.......')
    
    item_df['shipping_limit_date'] = pd.to_datetime(item_df['shipping_limit_date'])
    item_df['total_item_cost'] = (item_df['price'] + item_df['freight_value'])
    clean_order_item = item_df.dropna(subset=['order_id', 'product_id'])

    logging.info(f"Order Items cleaned: Prices verified and 'total_item_cost' calculated. Total: {len(item_df)} rows.")

    return clean_order_item


def transformer_orders(order_df):
    
    logging.info('Cleaning Order data.......')

    
    order_df['order_purchase_timestamp'] = pd.to_datetime(order_df['order_purchase_timestamp'], format='ISO8601')
    order_df['order_delivered_carrier_date'] = pd.to_datetime(order_df['order_delivered_carrier_date'], format='ISO8601')
    order_df['order_delivered_customer_date'] = pd.to_datetime(order_df['order_delivered_customer_date'], format='ISO8601')
    order_df['order_delivered_customer_date'] = pd.to_datetime(order_df['order_delivered_customer_date'], format='ISO8601')
    order_df['order_estimated_delivery_date'] = pd.to_datetime(order_df['order_estimated_delivery_date'], format='ISO8601')
    order_df['delivery_time_days'] = (order_df['order_delivered_customer_date'] - order_df['order_purchase_timestamp']).dt.days

    cleaned_order_df = order_df

    logging.info(f"Orders cleaned: 5 date columns converted to datetime. Total: {len(order_df)} rows.")
    logging.info(f"Orders: 'delivery_time_days' metric generated.")
    return cleaned_order_df.drop_duplicates(subset=['order_id'])

def transformer_category_name(name_df):
    logging.info('Cleaning category name data.......')

    name_df['product_category_name_english'] = name_df['product_category_name_english'].str.lower().str.strip()
    name_df['product_category_name'] = name_df['product_category_name'].str.lower().str.strip()
    cleaned_name = name_df

    logging.info(f"Translations cleaned: Category names normalized for joining. Total: {len(name_df)} rows.")

    return cleaned_name



if __name__ == "__main__":

    print("Starting Extraction...")

    TRANSFORM_MAP = {
        'geolocation': transformer_geolocation,
        'customers': transformer_customers,
        'products': tranformer_products,
        'category': transformer_category_name,
        'order_items': transformer_items,
        'order_review': transformer_order_review,
        'orders': transformer_orders,
        'order_payment': transformer_payment,
        'sellers': transformer_seller
}
    
    data_dict = extract_raw_data(DATA_FOLDER, INGESTING_FILE)

    if data_dict:

        for table, func in TRANSFORM_MAP.items():
            data_dict[table] = func(data_dict[table])
            print(f'Cleaning {table}.....')
    else:
        logging.error(f"Skipping transformation: not extracted.")

        print('Done! Data is now cleaned')
        print(f"Extraction complete! Loaded {len(data_dict)} tables.")
        print(f"Tables loaded: {list(data_dict.keys())}")


    
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

    with engine.begin() as conn:
        conn.execute(text(MAIN_TABLE))
        conn.execute(text(STAGING_TABLE))

    print("Building Star schema Warehouse Models.....")

    fact_order_items = transformer_items(data_dict['order_items'])

    """creating dimensionnal table"""
    dim_customers = transformer_customers(data_dict['customers'])
    dim_product = tranformer_products(data_dict['products'])
    dim_sellers = transformer_seller(data_dict['sellers'])
    dim_orders_details = transformer_order_review(data_dict['order_review'])
    dim_geolocation = transformer_geolocation(data_dict['geolocation'])
    dim_order_payment = transformer_payment(data_dict['order_payment'])
    dim_product_name = transformer_category_name(data_dict['category'])
    dim_order = transformer_orders(data_dict['orders'])

    dim_product = pd.merge(dim_product, dim_product_name, on='product_category_name', how='left')

    """creating dim_date table"""
    dim_date = dim_order[['order_purchase_timestamp']].drop_duplicates().copy()
    dim_date.columns = ['order_date']

    """creating primary key date_key"""
    dim_date['date_key'] = dim_date['order_date'].dt.strftime('%Y%m%d').astype(int)

    dim_date['day'] = pd.to_datetime(dim_date['order_date']).dt.day
    dim_date['month'] = pd.to_datetime(dim_date['order_date']).dt.month
    dim_date['year'] = pd.to_datetime(dim_date['order_date']).dt.year
    dim_date['is_weekend'] = pd.to_datetime(dim_date['order_date']).dt.dayofweek >= 5

    """creating final fact table"""
    fact_order_items = pd.merge(fact_order_items, dim_order[['order_id', 'order_purchase_timestamp']], on='order_id', how='left') 
    fact_order_items['date_key'] = fact_order_items['order_purchase_timestamp'].dt.strftime('%Y%m%d').astype(int)

    print("Success! Star Schema is ready.")
    logging.info(f"Warehouse modeling complete. Fact table contains {len(fact_order_items)} rows.")


    MAPPING_LOAD = {
        'stage_date': dim_date,
        'stage_geolocation': dim_geolocation,
        'stage_product_name': dim_product_name,
        'stage_products': dim_product,
        'stage_customers': dim_customers,
        'stage_sellers': dim_sellers,
        'stage_orders': dim_order,
        'stage_orders_details': dim_orders_details,
        'stage_order_payments': dim_order_payment,
        'stage_order_items': fact_order_items
    }

    print('Loading data into warehouse......')
    loading_start_time = time.time() 

    for table_name, df in MAPPING_LOAD.items():
        start_time = time.time()
        logging.info(f"Loading into {table_name}...")

        try:
            df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=5000, method='multi')
            logging.info(f'{table_name}: {len(df)} Rows Loaded...')

            end_time = time.time()
            time_taken = round(end_time - start_time, 2)

            logging.info(f'{table_name} loaded in {time_taken} seconds')
            print(f'SUCCESFULLY LOADED: {table_name}')

        except Exception:
            elapsed = round(time.time() - start_time, 2)
            logging.exception(f"{table_name} failed after {elapsed}s")
            print(f"FAILED: {table_name}")
        

    total_loading_time = time.time()
    logging.info(f'TOTAL LOADING TIME: {round(total_loading_time - loading_start_time, 2)} seconds')
    print(f'SUCCESS: DONE LOADING')



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
    
    try:
        
        with engine.begin() as conn:
            print(f'Executing dimension Merging...')
            conn.execute(text(merge_dim))
            print('Executing Fact Merging...')
            conn.execute(text(FACT_MERGING))
            print('Clearing Stage_table...')
            conn.execute(text(CLEARING_STAGE))

        logging.info("ETL PIPELINE SUCCESSFULLY completed and staging table is clean.")
        print('ETL Pipeline is Succesfully completed...')

    except Exception as e:
        logging.error(f'Pipeline Failed {e}')
        print(f'LOADING PHASE FAILED: check log')

else:
    print("Extraction failed. Check pipeline.log for details")







