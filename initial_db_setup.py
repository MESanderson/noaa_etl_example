from sqlalchemy import create_engine


def init_db():
    admin_db_conn = create_engine("postgresql://noaa_etl:secret@localhost:5432/postgres", isolation_level="AUTOCOMMIT")
    dbs = [x[0] for x in
           admin_db_conn.execute("select datname from pg_database;").fetchall()]
    if "noaa_etl" not in dbs:
        admin_db_conn.execute("CREATE DATABASE noaa_etl;")

    db_conn = create_engine("postgresql://noaa_etl:secret@localhost:5432/noaa_etl", isolation_level="AUTOCOMMIT")

    db_conn.execute("""CREATE TABLE if not exists LOAD_TRACKING (
                    id serial primary key,
                    load_source text,
                    load_time timestamp
                    );""")

    db_conn.execute("""
        CREATE TABLE if not exists STATIONS (
            id serial primary key,
            station_id text,
            name text,
            latitude numeric(8, 5),
            longitude numeric(8, 5),
            elevation numeric(8, 2),
            load_id integer references LOAD_TRACKING(id)
        );
    """)

    db_conn.execute("""
    CREATE TABLE if not exists HOURLY_DEW_POINT_DATA_COMPLETENESS (
        id serial primary key,
        hourly_dew_point_tenth_percentile varchar(1),
        hourly_dew_point_ninetieth_percentile varchar(1),
        hourly_dew_point_mean varchar(1)
    );
    CREATE TABLE if not exists HOURLY_DEW_POINT_DATA (
        id serial primary key,
        station_id integer references STATIONS(id),
        date_time timestamp,
        hourly_dew_point_tenth_percentile real,
        hourly_dew_point_ninetieth_percentile real,
        hourly_dew_point_mean real,
        data_completeness_id integer references HOURLY_DEW_POINT_DATA_COMPLETENESS(id),
        load_id integer references LOAD_TRACKING(id)
    );
    """)

    db_conn.execute("""
    CREATE TABLE if not exists HOURLY_PRESSURE_DATA_COMPLETENESS (
        id serial primary key,
        hourly_sea_level_pressure_tenth_percentile varchar(1),
        hourly_sea_level_pressure_ninetieth_percentile varchar(1),
        hourly_sea_level_pressure_mean varchar(1)
    );
    CREATE TABLE if not exists HOURLY_PRESSURE_DATA (
        id serial primary key,
        station_id integer references STATIONS(id),
        date_time timestamp,
        hourly_sea_level_pressure_tenth_percentile real,
        hourly_sea_level_pressure_ninetieth_percentile real,
        hourly_sea_level_pressure_mean real,
        data_completeness_id integer references HOURLY_PRESSURE_DATA_COMPLETENESS(id),
        load_id integer references LOAD_TRACKING(id)
    );
    """)

    db_conn.execute("""
    CREATE TABLE if not exists HOURLY_TEMPERATURE_DATA_COMPLETENESS (
        id serial primary key,
        hourly_temperature_tenth_percentile varchar(1),
        hourly_temperature_ninetieth_percentile varchar(1),
        hourly_temperature_mean varchar(1),
        hourly_cooling_degree_hours_mean varchar(1),
        hourly_heating_degree_hours_mean varchar(1),
        hourly_heat_index_mean varchar(1)
    );
    CREATE TABLE if not exists HOURLY_TEMPERATURE_DATA (
        id serial primary key,
        station_id integer references STATIONS(id),
        date_time timestamp,
        hourly_temperature_tenth_percentile real,
        hourly_temperature_ninetieth_percentile real,
        hourly_temperature_mean real,
        hourly_cooling_degree_hours_mean real,
        hourly_heating_degree_hours_mean real,
        hourly_heat_index_mean real,
        data_completeness_id integer references HOURLY_TEMPERATURE_DATA_COMPLETENESS(id),
        load_id integer references LOAD_TRACKING(id)
    );
    """)

    db_conn.execute("""
    CREATE TABLE if not exists HOURLY_WIND_DATA_COMPLETENESS (
        id serial primary key,
        hourly_wind_chill_mean varchar(1),
        hourly_wind_prevailing_direction varchar(1),
        hourly_wind_prevailing_percentage varchar(1),
        hourly_wind_secondary_direction varchar(1),
        hourly_wind_secondary_percentage varchar(1),
        hourly_wind_average_speed varchar(1),
        hourly_wind_percentage_calm varchar(1),
        hourly_wind_mean_vector_direction varchar(1),
        hourly_wind_mean_vector_magnitude varchar(1)
    );
    CREATE TABLE if not exists HOURLY_WIND_DATA (
        id serial primary key,
        station_id integer references STATIONS(id),
        date_time timestamp,
        hourly_wind_chill_mean real,
        hourly_wind_prevailing_direction real,
        hourly_wind_prevailing_percentage real,
        hourly_wind_secondary_direction real,
        hourly_wind_secondary_percentage real,
        hourly_wind_average_speed real,
        hourly_wind_percentage_calm real,
        hourly_wind_mean_vector_direction real,
        hourly_wind_mean_vector_magnitude real,
        data_completeness_id integer references HOURLY_WIND_DATA_COMPLETENESS(id),
        load_id integer references LOAD_TRACKING(id)
    );
    """)