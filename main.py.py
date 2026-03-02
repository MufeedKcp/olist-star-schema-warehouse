import time
import logging
from A import extract_raw_data
from B import create_warehouse_models
from C import get_engine, MAIN_TABLE, STAGING_TABLE, CLEARING_STAGE, FACT_MERGING, merge_dim
from sqlalchemy import text


DATA_FOLDER = r'olist-ETL-Warehouse\brazil_sales_datasets'
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

def run_pipeline():
    logging.basicConfig(filename='report.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s - %(name)s')
    engine = get_engine()

    data_dict = extract_raw_data(DATA_FOLDER, INGESTING_FILE)
    MAPPING_LOAD = create_warehouse_models(data_dict)

    with engine.begin() as conn:
        conn.execute(text(MAIN_TABLE))
        conn.execute(text(STAGING_TABLE))


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


if __name__ == "__main__":
    run_pipeline()


