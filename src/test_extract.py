from extract import extract_raw_data
from extract import DATA_FOLDER, INGESTING_FILE

def test_does_data_loaded_into_dict():
    data = extract_raw_data(DATA_FOLDER, INGESTING_FILE)
    assert len(data) == 9
