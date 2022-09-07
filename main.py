import os
from urllib import response
import shutil
import time

import requests
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

def download_file(url):
    print(f'downloading {url}')
    with requests.get(url, stream=True) as r:
        r.raw.decode_content = True # seems like they are all gzipped
        with open('output.csv', 'wb') as f:
            shutil.copyfileobj(r.raw, f)

def prep_open_data_download(url):
    print(f'preparing new download for {url}')
    r = requests.post(url, json=dict(spatialRefId=4326, format="csv",where="1=1"))
    print(r.status_code)
    r.raise_for_status()
    return True

def check_status(url):
    r = requests.get(f'{url}?spatialRefId=4326&formats=csv&where=1%3D1')
    print(r.status_code)
    r.raise_for_status()
    response_data = r.json()
    status = response_data['data'][0]['attributes']['status'] 
    print(status) 
    return status

def sync_db(data, DB_URL):
    try:
        con = create_engine(
            DB_URL,
            executemany_mode="values",
            executemany_values_page_size=1000
        )
        data.to_sql(
            'requests',
            con=con,
            index=False,
            if_exists='replace'
        )
    except OperationalError as e:
        print(e)
        time.sleep(5)
        sync_db(data, DB_URL)

if __name__ == "__main__":
    
    id = "06a34732b59142418f9b490f24a942b9_14"
    url = f'https://opendata.arcgis.com/api/v3/datasets/{id}/downloads'

    if not os.path.exists('output.csv'): # for local testing
        prep_open_data_download(url) # only trigger this once to prep for sometime later
        download_file(f'{url}/data?spatialRefId=4326&format=csv&where=1%3D1')

    # spatial join smd columns to 311 data
    print('perform spatial joins')
    smd = gpd.read_file('data/smd2013.geojson')
    df = pd.read_csv('output.csv')
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.X, df.Y), crs=smd.crs)
    joined = gdf.sjoin(smd, how="inner", predicate='within')
    data = pd.DataFrame(joined.drop(columns=['geometry', 'index_right']))

    # data cleaning
    print('clean the data')
    data["SERVICETYPECODEDESCRIPTION"] = data["SERVICETYPECODEDESCRIPTION"].str.replace("Admistration", "Administration")
    data["ADDDATE"] = pd.to_datetime(data["ADDDATE"])
    data["RESOLUTIONDATE"] = pd.to_datetime(data["RESOLUTIONDATE"])
    data["SERVICEDUEDATE"] = pd.to_datetime(data["SERVICEDUEDATE"])
    data["SERVICEORDERDATE"] = pd.to_datetime(data["SERVICEORDERDATE"])
    
    # temp save
    # data.to_csv('data.csv')

    # write to db
    print('write to db')
    DB_URL = os.getenv('MB_DB_CONNECTION_URI')
    DB_URL = DB_URL.replace("postgres://", "postgresql://")
    sync_db(data, DB_URL)
