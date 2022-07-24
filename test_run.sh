export CSV_PATH="test_data/3033900.csv"
export OUTPUT_PATH="initial_transform/"
export DB_USERNAME="noaa_etl"
export DB_PSWD="secret"
export DB_HOST="localhost"
export DB_PORT="5432"
export DB_NAME="noaa_etl"

python main.py init_db
python main.py load_csv -i $CSV_PATH -o $OUTPUT_PATH -u $DB_USERNAME -P $DB_PSWD -H $DB_HOST -p $DB_PORT -d $DB_NAME
python main.py upsert_staging_data -u $DB_USERNAME -P $DB_PSWD -H $DB_HOST -p $DB_PORT -d $DB_NAME