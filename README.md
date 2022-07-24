# NOAA ETL library

This is an example ETL for loading NOAA hourly weather data into a database.
THIS IS NOT A REAL LIBRARY/REAL PROJECT. This could be improved
in a hundred ways. I knocked it out over a weekend so it is just
the minimum to create a pipeline that splits up a CSV into parts
that make sense and allow it to run multiple times in a row 
with the same arguments without polluting the database.

## Quick start
start up postgres database with `docker-compose up`

run all steps with `./test_run.sh`