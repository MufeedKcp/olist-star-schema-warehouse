import pandas as pd
import os
import logging 

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
