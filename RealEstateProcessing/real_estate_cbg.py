import numpy as np
import pandas as pd
import os
import folium
from folium import plugins
import pygeohash as geo
from datetime import datetime
import calendar
from datetime import timezone
import datetime
import pytz
import geopandas as gpd

from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .master("local") \
        .appName("cbg_for_real_estate") \
        .getOrCreate()

sc = spark.sparkContext

# geom = gpd.read_file('../Census_Data/cbg.geojson')

def process_maps(file):
    # types = {'venue_name':'str','venue_category':'str','name':'str','poi_place_id':'str','speed':'float', 'types':'str', 'vicinity':'str','publisher_id':'str'}
    f = '/'.join(['origin',file])
    data_orig= pd.read_csv(f)#,dtype=types)
    #     data_orig = data_orig[:10].copy()
    
    geom = gpd.read_file('../Census_Data/cbg.geojson')
    data_orig = gpd.GeoDataFrame(data_orig, geometry = gpd.points_from_xy(data_orig.Longitude,data_orig.Latitude))
    data_orig = gpd.sjoin(data_orig,geom,predicate='within')
    data_orig = data_orig[['INTERNALID','Latitude','Longitude','CensusBlockGroup']].copy()
    
    cwd = os.getcwd()
    path = cwd + "/destination/new_" + file
    data_orig.to_csv(path)

def spark_process_maps(x):
    return [process_maps(entry) for entry in x]

files = os.listdir(os.getcwd() + '/origin')
files = [i for i in files]
# files = [files[0]]

rdd =sc.parallelize(files).mapPartitions(spark_process_maps).collect()