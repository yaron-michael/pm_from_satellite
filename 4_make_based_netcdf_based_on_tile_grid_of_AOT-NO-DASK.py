# -*- coding: utf-8 -*-
"""
Created on Sat Mar 18 13:23:00 2023
-
@author: YARON
"""

from dask.distributed import Client
import dask.array as da
from dask.diagnostics import ProgressBar
import xarray as xr
import os
import pandas as pd
import glob
import rioxarray
import numpy as np
from datetime import datetime



##parameters
var_name = "AOT"
dir_of_geotif_tile_file  = ['D:/test/h20v05','D:/test/h21v06','D:/test/h21v05']
#the output netcdf wil be in the dir of the tile
##parameters

for dir_of_geotif  in dir_of_geotif_tile_file:
  os.chdir(dir_of_geotif)
  
 
  print(dir_of_geotif)

  ds_list = []
  geotiff_list = glob.glob("*.tif")
  filenames_netcdf =geotiff_list[0][0:6]
  orbit_date_datetime_list = []
  for tif in geotiff_list:
    
    #print(tif)
    orbit_date =  tif[18:29]
    orbit_date_datetime = datetime.strptime(orbit_date, '%Y%j%H%M')
    orbit_date_datetime_list.append(orbit_date_datetime)

    geotiffs_temp = rioxarray.open_rasterio(tif,masked=True)
    geotiffs_ds = geotiffs_temp.to_dataset('band')
    count_band = len(geotiffs_ds.count())
    count = list(range(count_band))
 
    geotiffs_ds = geotiffs_ds.rename({1: var_name})
 
    x = list(range(1, 1+len(count), 1))
    x.pop(0) 

    geotiffs_ds = geotiffs_ds.drop_vars(x)
    geotiffs_ds.rio.write_crs('epsg:2039', inplace=True)#set esgpe of iseral 

    ds_list.append(geotiffs_ds) 

  time_var = xr.Variable('time', orbit_date_datetime_list)
  geotiffs_da = xr.concat(ds_list, dim=time_var) 
  geotiffs_da.rio.write_crs('epsg:2039', inplace=True)#set esgpe of iseral 
  geotiffs_da.to_netcdf(filenames_netcdf+"_"+var_name+".nc")
  del(geotiffs_da)
  print("end one netcdf")
