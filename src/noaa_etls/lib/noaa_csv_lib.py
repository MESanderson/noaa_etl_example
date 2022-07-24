import pandas as pd

# Naming convention found here: https://www.ncei.noaa.gov/pub/data/cdo/documentation/NORMAL_HLY_documentation.pdf
COL_NAME_TRANSLATOR = {
    "HLY": "HOURLY",
    "CLDH": "COOLING_DEGREE_HOURS",
    "CLOD": "CLOUDS",
    "PCTBKN": "BROKEN_PERCENTAGE",
    "PCTFEW": "FEW_PERCENTAGE",
    "PCTOVC": "OVERCAST_PERCENTAGE",
    "PCTSCT": "SCATTERED_PERCENTAGE",
    "DEWP": "DEW_POINT",
    "10PCTL": "TENTH_PERCENTILE",
    "90PCTL": "NINETIETH_PERCENTILE",
    "NORMAL": "MEAN",
    "HIDX": "HEAT_INDEX",
    "HTDH": "HEATING_DEGREE_HOURS",
    "PRES": "SEA_LEVEL_PRESSURE",
    "TEMP": "TEMPERATURE",
    "WCHL": "WIND_CHILL",
    "WIND": "WIND",
    "1STDIR": "PREVAILING_DIRECTION",
    "1STPCT": "PREVAILING_PERCENTAGE",
    "2NDDIR": "SECONDARY_DIRECTION",
    "2NDPCT": "SECONDARY_PERCENTAGE",
    "AVGSPD": "AVERAGE_SPEED",
    "PCTCLM": "PERCENTAGE_CALM",
    "VCTDIR": "MEAN_VECTOR_DIRECTION",
    "VCTSPD": "MEAN_VECTOR_MAGNITUDE",
    "ATTRIBUTES": "COMPLETENESS",
    "DATE": "DATE_TIME"
}


def col_translator(col_name):
    col_name_pieces = col_name.replace("_", "-").split("-")
    return "_".join(COL_NAME_TRANSLATOR.get(x, x).lower() for x in col_name_pieces)


def load_csv_raw(csv_path, year):
    raw_df = pd.read_csv(csv_path)
    renamed_df = raw_df.rename(mapper=col_translator, axis="columns")
    renamed_df["date_time"] = pd.to_datetime(renamed_df["date_time"].map(lambda x: f"{year}-{x}"))
    return renamed_df

