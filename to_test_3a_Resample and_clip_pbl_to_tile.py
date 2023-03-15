# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 05:28:13 2023

#waringe this code use alot of RAM -the h21v05 can even not run sometime in a pc with 32 GB 
#one way to solve it to spilt the netcdf of the PBL of one year to for 6 months -comibne netcdf after 
#this can be done with tool that can work with big netcdf even with 8 gb ram
#var to add if you want to run also the h21v05 tile
#'C:/grid_file/h21v05_crs2039.shp' 
#C:/grid_file/h21v05.tif'
#'D:/pbl/h21v05/'
@author: YARON
"""




from dask.distributed import Client
import dask.array as da
from dask.diagnostics import ProgressBar
import numpy as np
import xarray as xr
import rioxarray
import geopandas
from shapely.geometry import box
from pathlib import Path    
import os
import glob

#parameters
tile_list_shp = ['C:/grid_file/h20v05_crs2039.shp','C:/grid_file/h21v06_crs2039.shp']
geotif_of_tile = ['C:/grid_file/h20v05.tif','C:/grid_file/h21v06.tif'] 
dir_to_put_redy_pbl_file = ['D:/pbl/h20v05/','D:/pbl/h21v06/']
#parameters
pbl_file =['D:/pbl/2010pbl.nc','D:/pbl/2011pbl.nc','D:/pbl/2012pbl.nc','D:/pbl/2013pbl.nc','D:/pbl/2014pbl.nc','D:/pbl/2015pbl.nc','D:/pbl/2016pbl.nc','D:/pbl/2017pbl.nc','D:/pbl/2018pbl.nc','D:/pbl/2019pbl.nc','D:/pbl/2020pbl.nc']


for tile, tile_grid,savedir in zip(tile_list_shp, geotif_of_tile,dir_to_put_redy_pbl_file):
    print(savedir)

    geodf = geopandas.read_file(tile)
    temp_list = []
    temp_name = []
    temp_match =[]
    tile_name = tile[7:12]#to fix -take file name not all pathch
    
    
    for pbl in pbl_file:
        print(pbl)
        xds1 = xds = xr.open_dataset(pbl)#land use
        xds1.rio.write_crs(4326, inplace=True)
        name_of_var = Path(pbl).name#the frist 4 letter of the pbl file are the year
        #we clip the raster of the land use
        clipped = xds1.rio.clip(geodf.geometry.values, geodf.crs, drop=False, invert=False)#clip the raster 
        clipped = clipped.rename({'longitude': 'x','latitude': 'y'})#need to chnge the dim to x and y 
        to_match = rioxarray.open_rasterio(tile_grid)#raster to math the 
        xds_repr_match = clipped.rio.reproject_match(to_match)
        xds_repr_match.to_netcdf(savedir + name_of_var[0:4]+"pbl.nc")#dir of path
