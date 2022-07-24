import sys
import logging
import datetime
import pandas
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler(stream=sys.stdout)]
)
logger = logging.getLogger("noaa_staging_upsert")


def get_new_load_rows(cur, staging_table):
    cur.execute(f"""INSERT INTO load_tracking (load_source, load_time)
            (select distinct load_path, load_time from {staging_table}
            except select load_source, load_time from load_tracking)""")


def delete_updated_rows_from_final(cur, staging_table, final_table):
    cur.execute(f"""
                       delete from {final_table}
                       where (station_id, date_time) in (select 
                       stations.id, {staging_table}.date_time from  {staging_table}
                       join stations on stations.station_id = {staging_table}.station
                       );
                       """)


def update_completeness_table(cur, staging_table, completness_table, columns):
    cols_string = ",\n".join(columns)
    cols_complete_string = ",\n".join(f"{c}_completeness" for c in columns)
    cur.execute(
        f"""
        with new_completeness as (
         select distinct {cols_complete_string}
        from {staging_table}
        except
        select  {cols_string}
            from {completness_table}
        )
        INSERT INTO {completness_table} (
        {cols_string}
        ) select {cols_complete_string}
        from new_completeness;
        """
    )


def insert_new_rows_to_final(cur, staging_table, final_table, completeness_table, columns):
    cols_string = ",\n".join(columns)
    cols_string_stg_prefix = ",\n".join(f"stg.{c}" for c in columns)
    comp_join_string = "\nAND ".join(f"comp.{c} = stg.{c}_completeness" for c in columns)

    cur.execute(
        f"""
        INSERT INTO {final_table}
        (station_id, date_time, 
        {cols_string}, 
        data_completeness_id, load_id)

        select 
        stations.id,
        stg.date_time,
        {cols_string_stg_prefix},
         comp.id,
         lt.id

        from {staging_table} stg
        join load_tracking lt on lt.load_time = stg.load_time
        and lt.load_source = stg.load_path
        join stations on stations.station_id = stg.station
        join {completeness_table} comp on
        {comp_join_string}
        """
    )


def delete_upserted_from_staging(cur, staging_table, final_table, columns):
    col_join = "\nOR ".join(f"stg.{c} <> final_table.{c}" for c in columns)
    cur.execute(f"""
               with new_data as (
               select stg.station,
               stg.date_time
               from {staging_table} stg
               left join stations st on st.station_id = stg.station
               left join {final_table} final_table
               on final_table.station_id = st.id 
               AND final_table.date_time = stg.date_time
               AND ( 
               {col_join}
               )
               left join load_tracking on load_tracking.id = final_table.load_id
               where load_tracking.load_time is null or load_tracking.load_time < stg.load_time
               )
               delete from {staging_table}
               where (station, date_time) not in ( select station, date_time from new_data);
               """)


def upsert_stations(db_conn):
    with db_conn.begin() as cur:
        # Find only where data has changed
        cur.execute("""
        with new_station_data as (
       select stg.station
        from stations_staging stg
        left join stations
        on stations.station_id = stg.station 
        left join load_tracking on load_tracking.id = stations.load_id
        where load_tracking.load_time is null
        or stations.station_id is null
        or
           ( ( stg.name <> stations.name
            OR stg.latitude <> stations.latitude
            OR stg.longitude <> stations.longitude
            OR stg.elevation <> stations.elevation
            )
         AND load_tracking.load_time < stg.load_time)
        )
        delete from stations_staging
        where station not in ( select station from new_station_data);
        """)
        cur.execute("""
                delete from stations
                where station_id in (select station from  stations_staging);
                """)

        get_new_load_rows(cur, "stations_staging")

        cur.execute(
            """
            INSERT INTO stations
            (station_id, name, latitude, longitude, elevation, load_id)
            
            select station, name, latitude, longitude, elevation, lt.id
            from stations_staging
            join load_tracking lt on lt.load_time = stations_staging.load_time
            and lt.load_source = stations_staging.load_path;
            """
        )


def upsert_dewpt(db_conn):
    logger.info("Upserting dewpt")

    staging_table = "dewpt_staging"
    final_table = "hourly_dew_point_data"
    completeness_table = "hourly_dew_point_data_completeness"
    columns = [
        "hourly_dew_point_tenth_percentile",
        "hourly_dew_point_ninetieth_percentile",
        "hourly_dew_point_mean"
    ]

    with db_conn.begin() as cur:
        delete_upserted_from_staging(cur, staging_table, final_table, columns)

        delete_updated_rows_from_final(cur, staging_table, final_table)
        get_new_load_rows(cur, staging_table)

        # add in new completeness
        update_completeness_table(cur, staging_table, completeness_table, columns)

        insert_new_rows_to_final(cur, staging_table, final_table, completeness_table, columns)
        cur.execute(f"truncate {staging_table};")


def upsert_pressure(db_conn):
    logger.info("Upserting pressure")

    staging_table = "pressure_staging"
    final_table = "hourly_pressure_data"
    completeness_table = "hourly_pressure_data_completeness"
    columns = [
              "hourly_sea_level_pressure_tenth_percentile",
              "hourly_sea_level_pressure_ninetieth_percentile",
              "hourly_sea_level_pressure_mean"
          ]

    with db_conn.begin() as cur:
        delete_upserted_from_staging(cur, staging_table, final_table, columns)

        delete_updated_rows_from_final(cur, staging_table, final_table)
        get_new_load_rows(cur, staging_table)

        # add in new completeness
        update_completeness_table(cur, staging_table, completeness_table, columns)

        insert_new_rows_to_final(cur, staging_table, final_table, completeness_table, columns)
        cur.execute(f"truncate {staging_table};")


def upsert_temp(db_conn):
    logger.info("Upserting temp")

    staging_table = "temp_staging"
    final_table = "hourly_temperature_data"
    completeness_table = "hourly_temperature_data_completeness"
    columns = [
        "hourly_temperature_tenth_percentile",
        "hourly_temperature_ninetieth_percentile",
        "hourly_temperature_mean",
        "hourly_cooling_degree_hours_mean",
        "hourly_heating_degree_hours_mean",
        "hourly_heat_index_mean"
    ]

    with db_conn.begin() as cur:

        delete_upserted_from_staging(cur, staging_table, final_table, columns)

        delete_updated_rows_from_final(cur, staging_table, final_table)
        get_new_load_rows(cur, staging_table)

        # add in new completeness
        update_completeness_table(cur, staging_table, completeness_table, columns)

        insert_new_rows_to_final(cur, staging_table, final_table, completeness_table, columns)
        cur.execute(f"truncate {staging_table};")


def upsert_wind(db_conn):
    logger.info("Upserting wind")

    staging_table = "wind_staging"
    final_table = "hourly_wind_data"
    completeness_table = "hourly_wind_data_completeness"
    columns = [
        "hourly_wind_chill_mean",
        "hourly_wind_prevailing_direction",
        "hourly_wind_prevailing_percentage",
        "hourly_wind_secondary_direction",
        "hourly_wind_secondary_percentage",
        "hourly_wind_average_speed",
        "hourly_wind_percentage_calm",
        "hourly_wind_mean_vector_direction",
        "hourly_wind_mean_vector_magnitude"
    ]

    with db_conn.begin() as cur:
        delete_upserted_from_staging(cur, staging_table, final_table, columns)

        delete_updated_rows_from_final(cur, staging_table, final_table)
        get_new_load_rows(cur, staging_table)

        # add in new completeness
        update_completeness_table(cur, staging_table, completeness_table, columns)

        insert_new_rows_to_final(cur, staging_table, final_table, completeness_table, columns)
        cur.execute(f"truncate {staging_table};")


def run_upsert(db_uri):
    db_conn = create_engine(db_uri)
    upsert_stations(db_conn)
    upsert_dewpt(db_conn)
    upsert_pressure(db_conn)
    upsert_temp(db_conn)
    upsert_wind(db_conn)

