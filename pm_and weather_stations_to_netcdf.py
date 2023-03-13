
#we have some line that are temp line and wil be remove changes  21-02-2023

import xarray as xr
from affine import Affine
import os
import glob
import pandas as pd
from glob import glob
import rasterio
import rioxarray
import cftime
import dask.array as da
import numpy as np
import glob
import netCDF4    
import numpy as np
import datetime as dt
from netCDF4 import date2num,num2date
from datetime import datetime
import pandas as pd
cwd = os.getcwd()

#this code in a loop wil enter the data for the netcdf file
#the basice  netcdf file is the AOT for one tile
#the csv data of pm and weather data is in this fromat
#metdata of the of the variable - PM10_h21v05_MET.csv  
#data of the of the variable    -'PM10_h21v05.csv'
#csv name is "met"+"_"+ tile_name +"_"var_name

#we have -
#

##parameters
loop_on_file_netcdf = ["h21v05_10yar_aot_.nc","10yar_aot_h20v05.nc","10yar_aot_h20v05.nc"] #this netcdf are in the main dir
nc_name =  'h21v05'
dir_of_project_file = 'C:/PM2_5/REDY2'
netcdf = 'D:/test/TEST_NEW_AO2/10yar.nc'
METADATA_of_data = "PM10-NET-H20V05.csv"
station_data = "Data_PM10.csv"
var_name = "PM2.5"
output_file_netcdf = 'D:/test/REDY2023/10yar_PM10_3H_RESMPLE.nc'
date_range_start = '2010-01-01'
date_range_end = '2020-01-02'
loop_on_file = ["PM10_data","PM25_data","RH_data_.csv","temp_data_.csv","WS_data_.csv"]
#dir of metdata of csv file to use = 'D:/test/REDY2023/10yar_PM10_3H_RESMPLE.nc'
netcdf_f =[]
##parameters


os.chdir(dir_of_project_file)



#h21v06 -ELIT
#h21v05 -DEAD SEA
#h20v05 -MIAN

csv_file = ["PM10_data","PM25_data","RH_data_.csv","temp_data_.csv","WS_data_.csv"]

#start by loop over tile nc (we have 3)
for nc in netcdf_f:
  nc_id = nc[0:6]#get tile name
  ds = xr.open_mfdataset(nc, combine='by_coords')

  ds['AOT'] = ds['AOT'] * np.nan
    
  list_of_time = ds.coords['time'].values
  #now we loop over all the csv of this tile

  for csv in csv_file:
   print(csv)
   station_data = csv
   name_of_var = station_data.split("_")
   METADATA_of_data = name_of_var[0]+"_"+nc_name+"_MET.csv"
   time_range_start = []
   time_range_end = []
    #ds = xr.open_dataset(netcdf)
   ds = xr.open_mfdataset(netcdf, combine='by_coords')
   ds['AOT'] = ds['AOT'] * np.nan
   list_of_time = ds.coords['time'].values
    #my_list = len(list_of_time)
    #T=list_of_time[0]
   time_list_from_netcdf = []
   Timestamp_from_netcdf = []
   for time in list_of_time:
        Timestamp_from_netcdf.append (pd.Timestamp(np.datetime64(time)))
        time_list_from_netcdf.append( datetime.utcfromtimestamp(time.tolist()/1e9))

   my_list = list(range(0, 11540)) # to fix 
   df = pd.DataFrame(list(zip(time_list_from_netcdf, my_list)),
                   columns =['date', 'val'])
   netcdf_time = df.set_index('date')
   df_m = pd.read_csv (METADATA_of_data)#METADATA csv All the stations 
   if len(df_m) == 0:
        print("no need to work and make netcdf")
   else:    
    Station_code=df_m['Station_code'].tolist()#GE
    ITMx=df_m["ITMx"].tolist()
    ITMy=df_m["ITMy"].tolist()
    #y shload be first 
    Station_name=df_m["Station_name"].tolist()
    Station_type=df_m["Station_type"].tolist()
    station_number=df_m["station_number"].tolist()
    len(Station_code)
    df_main= pd.read_csv (station_data)#main data set after we clean,and put only stations that are in the METADATA
    x = pd.date_range(start='2000-01-01 00:00:00', end='2022-01-01 00:00:00', freq='30min')#len of date is 385728
    x= x[0:385728]# to fix 
    df_main["date"] = x #this is 32592
    df_main = df_main.set_index('date')
    df_main['Date of the stations'] = df_main.index
    #this resmple igonre nana vale so if we have [6,np.NAN,105] the mean wil be  8 -igonre the nan
    df_main = df_main.resample('3H').mean()
    #df_main = df_main

    df_main = df_main.truncate(before=pd.Timestamp(date_range_start),after=pd.Timestamp(date_range_end))
    df_main['date'] = df_main.index
    df_main.drop('date', inplace=True, axis=1)


    df_merged = pd.merge_asof(
        netcdf_time,
        df_main,
        on='date'

    )
    #now entar the data to the netcdf file 
    
    count =0
    #vale_to_netcdf = list(range(1, 11541))
    ds = xr.open_dataset(netcdf)
    Station_code = Station_code
    #Station_code = ["AMI","AFU"]
    for index, Station_code_str in enumerate(Station_code):
        x = df_merged[str(Station_code_str)] #df-main is all of the data with colwm
        x = pd.DataFrame(x)#convrt to df
        x.columns.values[0] = "temp"#GIVW name for the clowm with the pooltingse
        vale_to_netcdf = x["temp"].tolist()
        ITMx_to_use = ITMx[index]
        ITMy_to_use = ITMy[index]

        vale_to_netcdf = vale_to_netcdf

        xx=ds.x
        yy=ds.y

        loc = ds.sel(x=ITMx[index], y=ITMy[index],method='nearest')
        y = loc.y.to_numpy()
        x = loc.x.to_numpy()
        print(x)
        print("Station_code---------------------------------")
        print (Station_code_str)
        print("Station_code---------------------------------")
        ds.AOT.loc[dict(time=slice(None),y=y,x=x)] = vale_to_netcdf
    ds= ds.rename({'AOT':  name_of_var[0]})
    ds.to_netcdf(output_file_netcdf)
    


    #this is to add band /chnge band name /chnge to nan
    #ds= ds.rename({'AOT': 'PM2_5'})
    #ds.to_netcdf(output_file_netcdf)
    #ds = ds.isel(time=0)
    #test file


    nc = xr.open_dataset('D:/test/REDY2023/10yar_PM2_5.nc')
    loc_plot = nc.sel(x=x, y=y,method='nearest')
    loc_plot = ds.sel(x=x, y=y,method='nearest')
    loc_plot["PM2_5"].plot()


    nc = xr.open_dataset('D:/test/REDY2023/10yar_PM2_5_3H_mean.nc')
    nc = nc.isel(time=0)
    nc=nc.transpose( 'y', 'x')
    nc.rio.write_crs('epsg:2039', inplace=True)#set esgpe of iseral

    nc.rio.to_raster('D:/test/REDY2023/10yar_PM2_5_3H_mean.tif')


    nc = xr.open_dataset('D:/test/REDY2023/10yar_AOT.nc')
    nc = nc.isel(time=0)
    nc=nc.transpose( 'y', 'x')
    nc.rio.write_crs('epsg:2039', inplace=True)#set esgpe of iseral
    nc.rio.to_raster('D:/test/REDY2023/10yar_AOTNEW.tif')
    #nc = nc.where(nc['new'] != 10000)#chnge vale to nan
    #nc['new']=xr.where((nc['new']> -9999999),-9950,nc['new'])

    

 



    
    
    
    
























  

