import os
import sys
import pathlib
import logging
import datetime
import pandas
from sqlalchemy import inspect

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler(stream=sys.stdout)]
)
logger = logging.getLogger("noaa_raw_to_staging")


def extract_station_info(main_df):
    columns_to_extract = [
        "station",
        "name",
        "latitude",
        "longitude",
        "elevation"
    ]
    station_df = main_df[columns_to_extract].drop_duplicates()
    return station_df


def extract_temperature_df(main_df):
    columns_to_extract = [
        "station",
        "date_time",
        "hourly_temperature_tenth_percentile",
        "hourly_temperature_tenth_percentile_completeness",
        "hourly_temperature_ninetieth_percentile",
        "hourly_temperature_ninetieth_percentile_completeness",
        "hourly_temperature_mean",
        "hourly_temperature_mean_completeness",
        "hourly_cooling_degree_hours_mean",
        "hourly_cooling_degree_hours_mean_completeness",
        "hourly_heating_degree_hours_mean",
        "hourly_heating_degree_hours_mean_completeness",
        "hourly_heat_index_mean",
        "hourly_heat_index_mean_completeness"
    ]
    temperature_df = main_df[columns_to_extract]
    return temperature_df


def extract_dewpoint_df(main_df):
    columns_to_extract = [
        "station",
        "date_time",
        "hourly_dew_point_tenth_percentile",
        "hourly_dew_point_tenth_percentile_completeness",
        "hourly_dew_point_ninetieth_percentile",
        "hourly_dew_point_ninetieth_percentile_completeness",
        "hourly_dew_point_mean",
        "hourly_dew_point_mean_completeness"
    ]
    dewpoint_df = main_df[columns_to_extract]
    return dewpoint_df


def extract_pressure_df(main_df):
    columns_to_extract = [
        "station",
        "date_time",
        "hourly_sea_level_pressure_tenth_percentile",
        "hourly_sea_level_pressure_tenth_percentile_completeness",
        "hourly_sea_level_pressure_ninetieth_percentile",
        "hourly_sea_level_pressure_ninetieth_percentile_completeness",
        "hourly_sea_level_pressure_mean",
        "hourly_sea_level_pressure_mean_completeness"
    ]
    pressure_df = main_df[columns_to_extract]
    return pressure_df


def extract_wind_df(main_df):
    columns_to_extract = [
        "station",
        "date_time",
        "hourly_wind_chill_mean",
        "hourly_wind_chill_mean_completeness",
        "hourly_wind_prevailing_direction",
        "hourly_wind_prevailing_direction_completeness",
        "hourly_wind_prevailing_percentage",
        "hourly_wind_prevailing_percentage_completeness",
        "hourly_wind_secondary_direction",
        "hourly_wind_secondary_direction_completeness",
        "hourly_wind_secondary_percentage",
        "hourly_wind_secondary_percentage_completeness",
        "hourly_wind_average_speed",
        "hourly_wind_average_speed_completeness",
        "hourly_wind_percentage_calm",
        "hourly_wind_percentage_calm_completeness",
        "hourly_wind_mean_vector_direction",
        "hourly_wind_mean_vector_direction_completeness",
        "hourly_wind_mean_vector_magnitude",
        "hourly_wind_mean_vector_magnitude_completeness"
    ]
    wind_df = main_df[columns_to_extract]
    return wind_df


def write_df(df, output_path):
    if os.path.exists(output_path):
        logger.info("Output already exists. Skipping write")
    else:
        df.to_parquet(pathlib.Path(output_path).with_suffix(".parquet"))


def split_hourly_data_into_categories(main_df, output_path):
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    station_df = extract_station_info(main_df)
    temp_df = extract_temperature_df(main_df)
    dewpt_df = extract_dewpoint_df(main_df)
    pressure_df = extract_pressure_df(main_df)
    wind_df = extract_wind_df(main_df)

    write_df(station_df, str(pathlib.Path(output_path, "stations")))
    write_df(temp_df, str(pathlib.Path(output_path, "temp")))
    write_df(dewpt_df, str(pathlib.Path(output_path, "dewpt")))
    write_df(pressure_df, str(pathlib.Path(output_path, "pressure")))
    write_df(wind_df, str(pathlib.Path(output_path, "wind")))


def get_raw_load_table_name(file):
    return f"{pathlib.Path(file).stem}_staging"


def raw_split_file_to_db(db_conn, raw_split_out):
    files = os.listdir(raw_split_out)
    for f in files:
        full_path = pathlib.Path(raw_split_out, f)
        df = pandas.read_parquet(str(full_path))
        df["load_path"] = raw_split_out
        df["load_time"] = datetime.datetime.now()

        if inspect(db_conn).has_table(get_raw_load_table_name(f)):
            # Selecting max from staging even though this ETL logic should only upload from a directory once
            # better safe than sorry
            prev_loads = db_conn.execute(f"""
            SELECT max(load_time)
            from {get_raw_load_table_name(f)} 
            where load_path = '{raw_split_out}';""").first()[0]
        else:
            prev_loads = None

        if prev_loads:
            logger.info(f"Previous load found from {prev_loads}. Skipping load to staging.")
        else:
            df.to_sql(get_raw_load_table_name(f), db_conn, if_exists="append", index=False)


