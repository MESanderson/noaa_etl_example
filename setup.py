from setuptools import setup

setup(
    name="noaa_hourly_etl",
    version="0.0.1",
    install_requires=[
        "requests",
        "pandas",
        "pyarrow",
        "fastparquet",
        "sqlalchemy",
        "psycopg2"
    ]
)