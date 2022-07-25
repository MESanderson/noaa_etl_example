from noaa_etls.etls.raw_to_staging import split_hourly_data_into_categories, raw_split_file_to_db
from noaa_etls.lib.noaa_csv_lib import load_csv_raw
from sqlalchemy import create_engine
import logging

logger = logging.getLogger("noaa_csv_to_staging")


def load_csv(csv_path, output_path, db_conn_uri, year):
    db_conn = create_engine(db_conn_uri)
    main_df = load_csv_raw(csv_path, year)
    split_hourly_data_into_categories(main_df, output_path)
    raw_split_file_to_db(db_conn, output_path)



