import pandas as pd
import numpy as np
import geopandas as gp
import geog
import shapely.geometry
import warnings
warnings.filterwarnings("ignore")
import datetime

#shapely.__version__

location = pd.read_csv('deduped_merged_data.csv')
real_estate = pd.read_csv('realestate_data/Arlington/_mobility_only_/DC_Arl_1.csv')
real_estate = real_estate[(real_estate.RESCOMM == 'residential') & (real_estate.Longitude != 0) & (real_estate['2019ASSESSMENT'] > 50000)].copy()
real_estate = real_estate.sample(10000,random_state=42)

#location.head()

#len(real_estate)

class ConnectMobileWithRealEstate():
    def __init__(self, mobile_df, count_once_per_day=False):
        self.mobile_df = mobile_df
        self.count_once_per_day = count_once_per_day #counted only once per day if they remain in the same stop
        
    def calculate_stats(self, mobile_df):
        
        mobile_df = mobile_df[mobile_df['dwell_start'].notnull() & mobile_df['dwell_end'].notnull()].reset_index(drop=True)
        
        mobile_df['total_commuting'] = 0
        mobile_df.loc[mobile_df.place_id==-1,'total_commuting'] = 1
        
        mobile_df['residents'] = 0
        mobile_df.loc[mobile_df.census_block_group==mobile_df.census_block_group_stop, 'residents'] = 1
        
        mobile_df['people_in_area'] = 1
        
        mobile_df['dwell_estimated'] = mobile_df.dwell_end - mobile_df.dwell_start
        
        mobile_df.loc[mobile_df.place_id_latitude.isna(), 'place_id_latitude'] = mobile_df.loc[mobile_df.place_id_latitude.isna(), 'latitude']
        mobile_df.loc[mobile_df.place_id_longitude.isna(), 'place_id_longitude'] = mobile_df.loc[mobile_df.place_id_longitude.isna(), 'longitude']

        return mobile_df        
        
        
    def timerange(self, x, time_unit='H'):
        r = pd.date_range(x.dwell_start, x.dwell_end,freq=time_unit)
        return [i for i in r]
        
    def group_by_time(self):
        
        mobile_df = self.calculate_stats(self.mobile_df)
        
        mobile_df['dwell_start'] = pd.to_datetime(mobile_df['dwell_start'],unit='s', errors='coerce')
        mobile_df['dwell_start'] = mobile_df['dwell_start'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

        mobile_df['dwell_end']= pd.to_datetime(mobile_df['dwell_end'],unit='s', errors='coerce')
        mobile_df['dwell_end'] = mobile_df['dwell_end'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
                
        mobile_df['time_list'] = mobile_df.apply(lambda x: self.timerange(x), axis=1)
                
        if self.count_once_per_day: # and (self.group_by == 'day_of_week')
            mobile_df.drop_duplicates(['advertiser_id','dwell_start','time_unit'], inplace=True, ignore_index=True)
                        
        mobile_df = mobile_df.drop(['census_block_group', 'census_block_group_stop','latitude', 'longitude', 'dwell_start', 'dwell_end', 'place_id'], axis=1)
        self.mobile_df = gp.GeoDataFrame(mobile_df, geometry=gp.points_from_xy(mobile_df.place_id_longitude, mobile_df.place_id_latitude), crs = 'epsg:4326')

        
    def merge_datasets(self, real_estate_df, distance=1000, time_group = 'day_of_week', only_non_residents=True, empty_area_impute_strat = 'mean'):
        
        real_estate_df = real_estate_df.copy()
        gmobile = self.mobile_df.copy()
        
        min_lat = gmobile.place_id_latitude.min()
        max_lat = gmobile.place_id_latitude.max()
        min_lon = gmobile.place_id_longitude.min()
        max_lon = gmobile.place_id_longitude.max()
        
        real_estate_df = real_estate_df.loc[(real_estate_df.Latitude>= min_lat) & (real_estate_df.Latitude<= max_lat) & (real_estate_df.Longitude >= min_lon) & (real_estate_df.Longitude <= max_lon)].reset_index(drop=True)
        
        if only_non_residents:
        
            columns_toNull = [i for i in gmobile.columns if i not in ['advertiser_id','place_id_latitude','place_id_longitude','residents', 'hour_of_week', 'hour_of_day', 'day_of_week','people_in_area', 'time_list','geometry']]
            gmobile.loc[gmobile.residents==1, columns_toNull] = np.nan 
        
        gmobile.reset_index(inplace=True)
        gmobile.rename({'index':'mobile_index'}, axis=1, inplace=True)
        #gmobile = gp.GeoDataFrame(mobile_df, geometry=gp.points_from_xy(self.mobile_df.place_id_longitude, self.mobile_df.place_id_latitude), crs = 'epsg:4326')
        gmobile.drop(['place_id_latitude','place_id_longitude'], axis=1, inplace=True)
        
        num_points = 50
        radius = distance  #radius in meters
        angles = np.linspace(0, 360, num_points)
        
        real_estate_df.rename({'CensusBlockGroup':'census_block_group'}, axis=1, inplace=True)
        real_estate_df = real_estate_df.reset_index()
        greal = gp.GeoDataFrame(real_estate_df, geometry=gp.points_from_xy(real_estate_df.Longitude, real_estate_df.Latitude), crs = 'epsg:4326')
        
        for idx, point in greal.geometry.items():
            polygon_array = geog.propagate(point, angles, radius)
            circle = shapely.geometry.Polygon(polygon_array)
            greal.loc[idx, 'geometry'] = circle
        #print('done with loop')
        
        property_columns = [i for i in greal.columns if i != 'geometry']
        column_operations = {i:'mean' for i in gmobile.columns if i not in ['census_block_group', 'hour_of_week', 'hour_of_day', 'day_of_week','day_of_year','people_in_area','index_right','geometry', 'advertiser_id', 'time_list','mobile_index','residents']}
        column_operations = {**column_operations, **{'people_in_area':'sum', 'residents':'sum'}}
        
        merged = gp.sjoin(greal[['geometry','index']], gmobile[['mobile_index','geometry']], predicate='contains', how='inner').drop(['index_right','geometry'], axis=1).reset_index(drop=True)
        #could weight by dwell in case of both or group by days 
        #print('done with merge')
        merged = merged.merge(real_estate_df, on = 'index').merge(gmobile, on='mobile_index')
        merged.drop('mobile_index', axis=1, inplace=True)
        
        merged = merged.explode('time_list')        
        
        if len(merged)==0:
            return merged
            
        merged['day_of_week'] = merged.time_list.dt.dayofweek
        merged['hour_of_day'] = merged.time_list.dt.hour
        merged['hour_of_week'] = merged.day_of_week * 24 + merged.hour_of_day
        merged['day_of_year'] = merged.time_list.dt.dayofyear
        
        merged.drop('time_list', axis=1, inplace=True)
        
        #print('done with explode')
        
        if time_group == 'day_of_week':
            merged.people_in_area = merged.people_in_area / merged.groupby(property_columns + ['day_of_year'] +['advertiser_id']).people_in_area.transform('sum')
            merged.residents = merged.residents / merged.groupby(property_columns + ['day_of_year'] +['advertiser_id']).residents.transform('sum')

        merged.drop('advertiser_id', axis=1, inplace=True)
        
        merged = merged.groupby(property_columns + [time_group]).agg(column_operations).reset_index()
        final = merged[property_columns].drop_duplicates().set_index('index')
        
        for i in [x for x in merged[time_group].unique()]:
            sub = merged.loc[merged[time_group]==i, [i for i in merged.columns if ((i not in property_columns) | (i=='index'))]].drop(time_group, axis=1).set_index('index')
            sub.columns = [str(i) + '__' + x for x in sub.columns if x not in property_columns]
            final = final.join(sub)
        
        mobility_columns = [i for i in final.columns if '__' in i]
        final.reset_index(inplace=True)
        final.rename({'index':'property_id'}, axis =1, inplace=True)
        
        #residents should maybe be averaged?
        people_in_area_columns = [i for i in mobility_columns if i.split('__')[1] in ['residents','people_in_area']]
        final[people_in_area_columns] = final[people_in_area_columns].fillna(0)
    
        if empty_area_impute_strat == 'mean':
            new = final.copy()
        
            for i in mobility_columns:
                new.loc[final[i].isna(), i] = final.loc[final[i].isna(), [x for x in mobility_columns if x.split('__')[1]==i.split('__')[1]]].mean(axis=1)

        if empty_area_impute_strat == 'zero':
            new = final.fillna(0) 
            
        else:
            new = final.copy()
            
        return new
            

features_not_to_use = ['dwell_delta', 'horizontal_accuracy',
       'location_at', 'venue_name', 'venue_category', 'speed', 'publisher_id',
       'dwell_time', 'batch_id_x', 'geometry', 'index_right',
       'home_long', 'home_lat', 'batch_id_y', 'State',
       'County', 'White alone', 'Black or African American alone',
       'American Indian and Alaska Native alone', 'Asian alone',
       'Native Hawaiian and Other Pacific Islander alone',
       'Some other race alone', 'Two or more races', 'Males', 'Females',
       'Some college 1 or more years no degree', "Associate's degree",
       "Bachelor's degree", "Master's degree", 'Professional school degree',
       'Doctorate degree','owner_occupied_units',
       'nonvet_povertylvl_18to64', 'vet_povertylvl_18to64',
       'nonvet_povertylvl_65&over', 'vet_povertylvl_65&over',
       'owner_occupied_1.01to1.5_PeoplePerRoom',
       'owner_occupied_1.51to2_PeoplePerRoom',
       'owner_occupied_2.01orMore_PeoplePerRoom',
       'renter_occupied_1.01to1.5_PeoplePerRoom',
       'renter_occupied_1.5to2_PeoplePerRoom',
       'renter_occupied_2.01orMore_PeoplePerRoom','renter_occupied_units']

# real_estate.drop(['Longitude','Latitude'],axis=1, inplace=True)

location.drop(features_not_to_use, axis=1, inplace=True)

ob = ConnectMobileWithRealEstate(location)

ob.group_by_time()

#ob.mobile_df.head(5)

sections = [len(real_estate)//13]*13+[len(real_estate)%13]

from tqdm import tqdm

df = pd.DataFrame()
tally = 0

for i in tqdm(sections):
    temp = ob.merge_datasets(real_estate.iloc[tally:tally+i,:], distance =500, empty_area_impute_strat='none')
    if len(temp) != 0:
        df = pd.concat([df,temp], ignore_index=True)
    tally = tally + i
    #print(tally)

#310
#df
#mobility_distance_500_mean_impute = ob.merge_datasets(real_estate, distance =500)
#mobility_distance_500_mean_impute

df.to_csv('MobilityDistance/tax_residential_mobility_distance_500_no_impute.csv', index=False)