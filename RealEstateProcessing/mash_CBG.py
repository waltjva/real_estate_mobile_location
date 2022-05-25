import numpy as np
import pandas as pd
import os
import geopandas as gpd
from geopy.geocoders import Nominatim
from pyspark.sql import SparkSession

# start spark session
spark = SparkSession.builder \
        .master("local") \
        .appName("cbg_for_mash") \
        .getOrCreate()

sc = spark.sparkContext

# geom = gpd.read_file('../Census_Data/cbg.geojson')

def get_coordinates(x):
    geolocator = Nominatim(user_agent="txx3ej@virginia.edu")
    try:
        location = geolocator.geocode(x, timeout=120)
        return (location.longitude, location.latitude)
    except:
        return (0,0)

def process_maps(file):
    # types = {'venue_name':'str','venue_category':'str','name':'str','poi_place_id':'str','speed':'float', 'types':'str', 'vicinity':'str','publisher_id':'str'}
    f = '/'.join(['origin',file])
    data_orig= pd.read_csv(f)#,dtype=types)
    # data_orig = data_orig[:2].copy()
    
    # grab coordinates
    data_orig[['Location']] = data_orig['PREMISEADD_fixed'].apply(get_coordinates)
    data_orig['Longitude'] = data_orig['Location'].apply(lambda x: x[0])
    data_orig['Latitude'] = data_orig['Location'].apply(lambda x: x[1])
    data_orig.drop('Location', axis=1, inplace=True)
    
    # grab CBG
    
    # read cbg.geojson as 'geom'
    geom = gpd.read_file('../Census_Data/cbg.geojson')
    
    # take out CensusBlockGroup from the original dataframe
    data_orig.drop('CensusBlockGroup',axis=1,inplace=True)
    
    # turn data_orig into a GeoDataFrame
    gpd_data_orig = gpd.GeoDataFrame(data_orig, geometry = gpd.points_from_xy(data_orig.Longitude,data_orig.Latitude))
    
    # one-to-one inner join
    gpd_sjoin = gpd.sjoin(gpd_data_orig,geom,predicate='within')
    
    # drop all but unique identifier and desired CensusBlockGroup
    cbg_frame = gpd_sjoin[['id_address','CensusBlockGroup']].copy()

    if not cbg_frame.empty:
        data_final = data_orig.merge(cbg_frame, on='id_address',how='left')
    else:
        data_orig.insert(len(data_orig.columns),'CensusBlockGroup',None,allow_duplicates=True)
        data_final = data_orig

    
    cwd = os.getcwd()
    path = cwd + "/destination/new_" + file
    data_final.to_csv(path, index=False)


def spark_process_maps(x):
    return [process_maps(entry) for entry in x]

files = os.listdir(os.getcwd() + '/origin')
files = [i for i in files]
# files.pop(8)
# files = [files[0]]

rdd =sc.parallelize(files).mapPartitions(spark_process_maps).collect()