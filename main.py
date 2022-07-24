from noaa_etls.etls.csv_load import load_csv
from noaa_etls.etls.staging_upsert import run_upsert
from initial_db_setup import init_db
import argparse
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler(stream=sys.stdout)]
)
logger = logging.getLogger("noaa_entry_point")


def build_db_uri(args):
    return f"postgresql://{args.username}:{args.password}@{args.host}:{args.port}/{args.database}"


def execute_init_db(_args):
    logger.info("Initializing DB")
    init_db()


def execute_load_csv(args):
    logger.info("loading_csv")
    db_uri = build_db_uri(args)
    load_csv(args.input_path, args.output_location, db_uri)


def execute_upsert(args):
    logger.info("Upserting staging data")
    db_uri = build_db_uri(args)
    run_upsert(db_uri)


def parse_args(args):

    parser = argparse.ArgumentParser(description="NOAA Hourly data ETL")
    subparsers = parser.add_subparsers()

    init_db_parser = subparsers.add_parser("init_db", help="initialize database")
    init_db_parser.set_defaults(func=execute_init_db)

    load_csv_parser = subparsers.add_parser("load_csv", help="Load CSV data into staging")
    load_csv_parser.add_argument("-i", "--input_path")
    load_csv_parser.add_argument("-o", "--output_location")
    load_csv_parser.add_argument("-u", "--username", help="db user name", default="noaa_etl")
    load_csv_parser.add_argument("-P", "--password", help="db password", default="secret")
    load_csv_parser.add_argument("-H", "--host", help="db host", default="localhost")
    load_csv_parser.add_argument("-p", "--port", help="db port", default="5432")
    load_csv_parser.add_argument("-d", "--database", help="db to connect to", default="noaa_etl")
    load_csv_parser.set_defaults(func=execute_load_csv)

    upsert_staging_parser = subparsers.add_parser("upsert_staging_data", help="Upsert staging_data")
    upsert_staging_parser.add_argument("-u", "--username", help="db user name", default="noaa_etl")
    upsert_staging_parser.add_argument("-P", "--password", help="db password", default="secret")
    upsert_staging_parser.add_argument("-H", "--host", help="db host", default="localhost")
    upsert_staging_parser.add_argument("-p", "--port", help="db port", default="54323")
    upsert_staging_parser.add_argument("-d", "--database", help="db to connect to", default="noaa_etl")
    upsert_staging_parser.set_defaults(func=execute_upsert)

    return parser.parse_args(args)


if __name__ == "__main__":
    parsed_args = parse_args(sys.argv[1:])
    parsed_args.func(parsed_args)
