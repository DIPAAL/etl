from typing import List
import pandas

CSV_EXTENSION = '.csv'

def clean_data(ais_file_path: str) -> List[str]:
    if ais_file_path.endswith(CSV_EXTENSION):
        return _clean_csv_data(ais_file_path)


def _clean_csv_data(ais_file_path_csv: str) -> List[str]:
    # Read into pandas
    # Keep dictionary of known entities (mimicking staging DB)
    # Coarse cleaning
    #   - Remove where draught >= 28.5 (keep nulls/none)
    #   - Remove where width >= 75
    #   - Remove where length >= 488
    #   - Remove where 99999999 =< MMSI >= 990000000
    #   - Remove where 112000000 < MMSI > 111000000
    #   - Remove where not with geometri of Danish_waters
    dirty_frame = pandas.read_csv(ais_file_path_csv)
    for row in dirty_frame.iterrows():
        stop1 = "Stop"
        print(row)
    stop = "Stop"
    pass


if __name__ == "__main__":
    clean_data('C:\\Users\\Alex skole\\Downloads\\aisdk-2021-09-07_reduced.csv')