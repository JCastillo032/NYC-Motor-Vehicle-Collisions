import pandas as pd
from datetime import date
from utilities import getConnString as gc
import numpy as np

# upload data from csv to SQL Server
# NYC Motor Vehicle Collisions data for NYC
# -https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95

# 3 views:
# -vDetail: Cleaned, detailed table of csv
# -vNYMVCInjuries: Summary of types involved and injuries/deaths
# -vNYMVCVehicles: Total Vehicles involved

# Parameters:
#   -fL: File location of downloaded csv file
#   -fNMVC: File name of NY MVC downloaded csv file
#   -svrN: Server Name to upload csv file to
#   -dBN: Database name to upload csv file to


# file loc and name
fL = ''
fNMVC = 'Motor_Vehicle_Collisions_-_Crashes.csv'

# Server DB Name
svrN = 'HOMEPC'
dBN = 'NY'

# Date range to remove older data, only keep last 5 years
begdt = date(date.today().year-5,1,1)

#MVC
# Get csv data
rptNYMVC = pd.read_csv(fL + fNMVC,dtype={'ZIP CODE':'str'})

# convert header names to _
rptNYMVC.columns = rptNYMVC.columns.str.replace(' ','_')

# create datetime
rptNYMVC[['CRASH_HR','CRASH_MM']] = rptNYMVC['CRASH_TIME'].str.split(':',expand=True)
rptNYMVC['CRASH_DATETIME'] = pd.to_datetime(rptNYMVC['CRASH_DATE'] + ' '
    +rptNYMVC['CRASH_HR'].str.zfill(2) + ':'
    +rptNYMVC['CRASH_MM'].str.zfill(2),
    format='%m/%d/%Y %H:%M')

# filter datetime
rptNYMVC = rptNYMVC.query("CRASH_DATETIME >=  @begdt")

# clean up latitude and longitude errors
minlon = rptNYMVC['LONGITUDE'].mean()-3
maxlon = rptNYMVC['LONGITUDE'].mean()+3
minlat = rptNYMVC['LATITUDE'].mean()-3
maxlat = rptNYMVC['LATITUDE'].mean()+3
rptNYMVC['LONGITUDE'] = rptNYMVC['LONGITUDE'].replace({0:np.nan})
rptNYMVC['LONGITUDE'] = np.where(rptNYMVC['LONGITUDE']<minlon,
    np.nan,rptNYMVC['LONGITUDE'])
rptNYMVC['LONGITUDE'] = np.where(rptNYMVC['LONGITUDE']>maxlon,
    np.nan,rptNYMVC['LONGITUDE'])
rptNYMVC['LATITUDE'] = rptNYMVC['LATITUDE'].replace({0:np.nan})
rptNYMVC['LATITUDE'] = np.where(rptNYMVC['LATITUDE']<minlat,
    np.nan,rptNYMVC['LATITUDE'])
rptNYMVC['LATITUDE'] = np.where(rptNYMVC['LATITUDE']>maxlat,
    np.nan,rptNYMVC['LATITUDE'])

# clean vehicle groups
for c in ['1','2','3','4','5']:
    rptNYMVC['VEHICLE_TYPE_CODE_' + c] = rptNYMVC['VEHICLE_TYPE_CODE_' + c].replace(
        {'SPORT UTILITY / STATION WAGON':'Station Wagon/Sport Utility Vehicle', 
        '4 dr sedan':'Sedan'})
    rptNYMVC['VEHICLE_TYPE_CODE_' + c] = rptNYMVC['VEHICLE_TYPE_CODE_' + c].str.upper()
    rptNYMVC['VEHICLE_TYPE_CODE_' + c] = rptNYMVC['VEHICLE_TYPE_CODE_' + c].astype('category')

# clean contributing factor groups
for f in ['1','2','3','4','5']:
    rptNYMVC['CONTRIBUTING_FACTOR_VEHICLE_' + f] = rptNYMVC['CONTRIBUTING_FACTOR_VEHICLE_' + f].replace(
        {'':np.nan,'ILLNES':'ILLNESS'})
    rptNYMVC['CONTRIBUTING_FACTOR_VEHICLE_' + f] = rptNYMVC['CONTRIBUTING_FACTOR_VEHICLE_' + f].str.upper()
    rptNYMVC['CONTRIBUTING_FACTOR_VEHICLE_' + f] = rptNYMVC['CONTRIBUTING_FACTOR_VEHICLE_' + f].astype('category')

# Add flag for if vehicle or injury
rptNYMVC['SEROUS_INCIDENT'] = (rptNYMVC['NUMBER_OF_PERSONS_INJURED']
    + rptNYMVC['NUMBER_OF_PERSONS_KILLED']
    + rptNYMVC['NUMBER_OF_PEDESTRIANS_INJURED']
    + rptNYMVC['NUMBER_OF_PEDESTRIANS_KILLED']
    + rptNYMVC['NUMBER_OF_CYCLIST_INJURED']
    + rptNYMVC['NUMBER_OF_CYCLIST_KILLED']
    + rptNYMVC['NUMBER_OF_MOTORIST_INJURED']
    + rptNYMVC['NUMBER_OF_MOTORIST_KILLED'])

# transpose vehicle type columns to rows
vNYMVCVehicles = pd.DataFrame()
for i in ['1','2','3','4','5']:
    tempVTDF = rptNYMVC.copy()
    tempVTDF = tempVTDF[['CRASH_DATETIME','LATITUDE','LONGITUDE', 'VEHICLE_TYPE_CODE_' + i]]
    tempVTDF = tempVTDF.query(f"VEHICLE_TYPE_CODE_{i} == VEHICLE_TYPE_CODE_{i}")
    tempVTDF.columns = ['CRASH_DATETIME','LATITUDE','LONGITUDE','VEHICLE_TYPE']
    vNYMVCVehicles =vNYMVCVehicles.append(tempVTDF)

# remove fields 
rptNYMVC = rptNYMVC.drop(['LOCATION','COLLISION_ID','CRASH_DATE','CRASH_TIME','CRASH_HR','CRASH_MM'],axis=1)

# tbl of injuries by date and location
vNYMVCInjuries = rptNYMVC.copy()
vNYMVCInjuries = vNYMVCInjuries.groupby([
    'CRASH_DATETIME',
    'ZIP_CODE',
    'LATITUDE',
    'LONGITUDE']).agg(
    {'NUMBER_OF_PERSONS_INJURED':'sum',
    'NUMBER_OF_PERSONS_KILLED':'sum',
    'NUMBER_OF_PEDESTRIANS_INJURED':'sum',
    'NUMBER_OF_PEDESTRIANS_KILLED':'sum',
    'NUMBER_OF_CYCLIST_INJURED':'sum',
    'NUMBER_OF_CYCLIST_KILLED':'sum',
    'NUMBER_OF_MOTORIST_INJURED':'sum',
    'NUMBER_OF_MOTORIST_KILLED':'sum'}).reset_index()

# save tbls in sql server
cnNYMVCInj = gc(svrN,dBN,'write')
rptNYMVC.to_sql('NYMVCDetail',cnNYMVCInj,if_exists='replace',index=False)
vNYMVCInjuries.to_sql('NYMVCInjuries',cnNYMVCInj,if_exists='replace',index=False)
vNYMVCVehicles.to_sql('NYMVCVehicles',cnNYMVCInj,if_exists='replace',index=False)