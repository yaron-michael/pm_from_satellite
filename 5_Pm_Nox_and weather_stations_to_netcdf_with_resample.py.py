


import xarray as xr
import os
import pandas as pd
import rioxarray
import cftime
import dask.array as da
import numpy as np  
import datetime as dt
from datetime import datetime
import glob



#this code in a loop wil enter the data for the netcdf file
#the basice  netcdf file is the AOT for one tile
#the csv data of pm and weather data is in this fromat
#metdata of the of the variable - PM10_h21v05_MET.csv  
#data of the of the variable    -'PM10_h21v05.csv'
#csv name is "met"+"_"+ tile_name +"_"var_name


##parameters
dir_of_project_file = 'C:/PM2_5/data'
output_file_netcdf_dir = "D:/test/resample/"

date_range_start = '2010-01-01'
date_range_end = '2020-01-02'

netcdf_to_loop = ['D:/test/h20v05/h20v05_AOT.nc','D:/test/h21v06/h21v06_AOT.nc','D:/test/h21v05/h21v05_AOT.nc']#netcdf are tile


csv_file = ["Ws","Temp","Rain","RH","pm10","pm25","NOx" ]#var of csv file 

##parameters



os.chdir(dir_of_project_file)

path_csv=glob.glob("*.csv")

for nc in netcdf_to_loop:
  full_name_with_Dir = nc
  nc = os.path.basename(nc)  
  

  nc_id = nc[0:6]#get tile name


 
  #now we loop over all the csv of this tile

  for csv in csv_file:
   print(csv)
   station_data = csv
   name_of_var = station_data.split("_")
  
   #METADATA_of_data = name_of_var[0]+"_"+nc_name+"_MET.csv"
   METADATA_of_data   =   name_of_var[0]+"_"+ nc_id +"_met.csv" 
   print(METADATA_of_data)

   time_range_start = []
   time_range_end = []

   ds = xr.open_mfdataset(full_name_with_Dir, combine='by_coords', chunks="auto")
   list_of_time = ds.coords['time'].values
   ds['AOT'] = ds['AOT'] * np.nan
   list_of_time = ds.coords['time'].values
  
   time_list_from_netcdf = []
   Timestamp_from_netcdf = []
   for time in list_of_time:
        Timestamp_from_netcdf.append (pd.Timestamp(np.datetime64(time)))
        time_list_from_netcdf.append( datetime.utcfromtimestamp(time.tolist()/1e9))

   my_list = list(range(0, len(list_of_time))) 
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
    df_main= pd.read_csv ("Data"+"_"+name_of_var[0]+".csv")#main data set after we clean,and put only stations that are in the METADATA
    temp_index=len(df_main)
    x = pd.date_range(start='2000-01-01 00:00:00', end='2022-01-01 00:00:00', freq='30min')#len of date is 385728\
        
     
    x= x[0:len(df_main)]
    df_main["date"] = x #this is 32592
    df_main = df_main.set_index('date')
    df_main['Date of the stations'] = df_main.index
    #this resmple igonre nana vale so if we have [6,np.NAN,105] the mean wil be  8 -igonre the nan
    df_main = df_main.resample('3H').mean()

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
    Station_code = Station_code

    for index, Station_code_str in enumerate(Station_code):
        x = df_merged[str(Station_code_str)] 
        x = pd.DataFrame(x)#convrt to df
        x.columns.values[0] = "temp"
        vale_to_netcdf = x["temp"].tolist()
        ITMx_to_use = ITMx[index]
        ITMy_to_use = ITMy[index]

        vale_to_netcdf = vale_to_netcdf

        xx=ds.x
        yy=ds.y

        loc = ds.sel(x=ITMx[index], y=ITMy[index],method='nearest')
        y = loc.y.to_numpy()
        x = loc.x.to_numpy()
     
        ds.AOT.loc[dict(time=slice(None),y=y,x=x)] = vale_to_netcdf
    ds= ds.rename({'AOT':  name_of_var[0]})
    print(nc_id+name_of_var[0])
    output_file_netcdf_name_and_dir = output_file_netcdf_dir +nc_id+name_of_var[0]+"resample" + ".nc"
    ds.to_netcdf(output_file_netcdf_name_and_dir)


 



























  

