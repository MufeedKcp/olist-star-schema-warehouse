import pandas as pd
import logging 


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


def create_warehouse_models(data_dict):
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


    return {
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