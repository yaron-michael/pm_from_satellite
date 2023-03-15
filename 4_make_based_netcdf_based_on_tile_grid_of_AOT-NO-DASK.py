

#14-3-2023 now in testing 

from dask.distributed import Client
import dask.array as da
from dask.diagnostics import ProgressBar
import xarray as xr
from affine import Affine
import os
import glob
import pandas as pd
from glob import glob
import rasterio
import rioxarray
import cftime
import numpy as np
import glob
import netCDF4    
import datetime as dt
from netCDF4 import date2num,num2date
from datetime import datetime
import salem
import geopandas as gpd
from shapely.geometry import box


##parameters
var_name = "AOT"
dir_of_geotif_tile_file  = ['D:/test/h20v05','D:/test/h21v06','D:/test/h21v05']
#the output netcdf wil be in the dir of the tile
##parameters

for dir_of_geotif  in dir_of_geotif_tile_file:
 os.chdir(dir_of_geotif)
 filenames = glob.glob('*.tif')
 TIF =xr.open_rasterio(filenames[0])
 print(filenames[0][0:6])
 filenames_netcdf =filenames[0][0:6]
 geotiffs_da = TIF.to_dataset('band')

 X =geotiffs_da["x"]
 X = X.shape[0]
 Y = geotiffs_da["y"]
 Y= Y.shape[0]
 x_array= geotiffs_da.x
 x_c = x_array.data
 y_array= geotiffs_da.y
 y_c = y_array.data
 
 
 filenames = glob.glob('*.tif')
 
 for tif in filenames:  

  tiff_file = rasterio.open(tif)
  tiff_file = tiff_file.read(1)

  orbit_date =  tif[18:29]
  orbit_date_datetime = datetime.strptime(orbit_date, '%Y%j%H%M')
  ncfile = netCDF4.Dataset(str(tif)+".nc",mode='w',format='NETCDF4_CLASSIC') 
  lat_dim = ncfile.createDimension('y', Y)     # latitude axis lat
  lon_dim = ncfile.createDimension('x', X)    # longitude axis lon
  time_dim = ncfile.createDimension('time',  1) 
  lat = ncfile.createVariable('y', np.float32, ('y',))
  lat.units = 'meters'
  lat.long_name = 'Y'
  lon = ncfile.createVariable('x', np.float32, ('x',))
  lon.units = 'meters'
  lon.long_name = 'X'
  time = ncfile.createVariable('time', np.float64, ('time',))
  time.units = 'hours since 1800-01-01'
  time.long_name = 'time'
# Define a 3D variable to hold the data
  temp = ncfile.createVariable('AOT',np.float64,('time','x','y')) # note: unlimited dimension is leftmost
  temp.units = 'AOT' # unit here
  temp.standard_name = var_name # this is a CF standard name
 
  nlats = len(lat_dim); nlons = len(lon_dim); ntimes = 1
  # Write latitudes, longitudes.
  lat[:] = y_c
  lon[:] = x_c
  temp[0,:,:] = tiff_file  # Appends data along unlimited dimension
  times_arr = time[:]
  dates = [orbit_date_datetime]#need to take time from the file name
  times = date2num(dates, time.units)
  time[:] = times
  ncfile.close(); print('Dataset is closed!')

 file = glob.glob( '*.nc' )

 new_ds = xr.open_mfdataset(file, combine='by_coords')
 new_ds.to_netcdf(filenames_netcdf+var_name+".nc")
