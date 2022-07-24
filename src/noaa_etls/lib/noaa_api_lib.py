import itertools
import os
import requests

TOKEN = os.environ.get("NOAA_TOKEN")

BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/"


def get_header():
    return {
        "token": TOKEN
    }


def get_data_sets(
        datatypeid=None,
        locationid=None,
        stationid=None,
        startdate=None,
        enddate=None,
        sortfield=None,
        sortorder=None,
        limit=None,
        offset=None
):
    params_raw = {
        "datatypeid": datatypeid,
        "locationid": locationid,
        "stationid": stationid,
        "startdate": startdate,
        "enddate": enddate,
        "sortfield": sortfield,
        "sortorder": sortorder,
        "limit": limit,
        "offset": offset
    }
    params = {k: v for k, v in params_raw.items() if v is not None}
    r = requests.get(BASE_URL + "datasets", headers=get_header(), params=params)
    r.raise_for_status()
    return r.json()["results"]


def get_data_categories(
        datasetid=None,
        locationid=None,
        stationid=None,
        startdate=None,
        enddate=None,
        sortfield=None,
        sortorder=None,
        limit=None,
        offset=None
):
    params_raw = {
        "datasetid": datasetid,
        "locationid": locationid,
        "stationid": stationid,
        "startdate": startdate,
        "enddate": enddate,
        "sortfield": sortfield,
        "sortorder": sortorder,
        "limit": limit,
        "offset": offset
    }
    params = {k: v for k, v in params_raw.items() if v is not None}
    r = requests.get(BASE_URL + "datacategories", headers=get_header(), params=params)
    r.raise_for_status()
    return r.json()["results"]


def get_location_categories(
        datasetid=None,
        startdate=None,
        enddate=None,
        sortfield=None,
        sortorder=None,
        limit=None,
        offset=None
):
    params_raw = {
        "datasetid": datasetid,
        "startdate": startdate,
        "enddate": enddate,
        "sortfield": sortfield,
        "sortorder": sortorder,
        "limit": limit,
        "offset": offset
    }
    params = {k: v for k, v in params_raw.items() if v is not None}
    r = requests.get(BASE_URL + "locationcategories", headers=get_header(), params=params)
    r.raise_for_status()
    return r.json()["results"]


def get_locations(
        datasetid=None,
        locationcategoryid=None,
        datacategoryid=None,
        startdate=None,
        enddate=None,
        sortfield=None,
        sortorder=None,
        limit=None,
        offset=None
):
    params_raw = {
        "datasetid": datasetid,
        "locationcategoryid": locationcategoryid,
        "datacategoryid": datacategoryid,
        "startdate": startdate,
        "enddate": enddate,
        "sortfield": sortfield,
        "sortorder": sortorder,
        "limit": limit,
        "offset": offset
    }
    params = {k: v for k, v in params_raw.items() if v is not None}
    r = requests.get(BASE_URL + "locations", headers=get_header(), params=params)
    r.raise_for_status()
    return r.json()["results"]


def get_stations(
        datasetid=None,
        locationid=None,
        datacategoryid=None,
        datatypeid=None,
        extent=None,
        startdate=None,
        enddate=None,
        sortfield=None,
        sortorder=None,
        genLimit=100,
        limit=25,
        offset=None
):
    params_raw = {
        "datasetid": datasetid,
        "locationid": locationid,
        "datacategoryid": datacategoryid,
        "datatypeid": datatypeid,
        "extent": extent,
        "startdate": startdate,
        "enddate": enddate,
        "sortfield": sortfield,
        "sortorder": sortorder,
        "limit": limit,
        "offset": offset
    }
    params = {k: v for k, v in params_raw.items() if v is not None}
    r = requests.get(BASE_URL + "stations", headers=get_header(), params=params)
    r.raise_for_status()

    metadata = r.json()["metadata"]
    resultset = metadata["resultset"]

    station_list = r.json()["results"]

    newOffset = params.pop("offset", 1) + limit
    station_iter = itertools.chain(station_list, get_stations(genLimit=genLimit, offset=newOffset, **params))

    for i in itertools.islice(station_iter, genLimit):
        yield i


def get_data(
        datasetid=None,
        datatypeid=None,
        locationid=None,
        stationid=None,
        startdate=None,
        enddate=None,
        units="standard",
        sortfield=None,
        sortorder=None,
        limit=None,
        offset=None,
        inlucdemetadata=False
):
    params_raw = {
        "datasetid": datasetid,
        "locationid": locationid,
        "stationid": stationid,
        "datatypeid": datatypeid,
        "units": units,
        "startdate": startdate,
        "enddate": enddate,
        "sortfield": sortfield,
        "sortorder": sortorder,
        "limit": limit,
        "offset": offset,
        #"inlucdemetadata": inlucdemetadata
    }
    params = {k: v for k, v in params_raw.items() if v is not None}

    r = requests.get(BASE_URL + "data", headers=get_header(), params=params)
    r.raise_for_status()
    return r.json()["results"]


# def do_a_test():
#     dat = get_data(stationid="GHCND:USW00023036", limit=100, startdate="2009-07-01", enddate="2010-06-30", datasetid="NORMAL_HLY")
#     d = get_data_sets()
#     dc = get_data_categories()
#     l = get_locations()
#     lc = get_location_categories()
#     s = get_stations(datasetid="NORMAL_HLY", sortfield="name")
#
# do_a_test()

