# -*- coding: utf-8 -*-
"""
Created on Wed Feb 22 02:27:11 2023

@author: yaron.michael
"""

#https://zia207.github.io/geospatial-python.io/lesson_06_working-with-raster-data.html
#SEE IN Convert Point Data to Raster

from geocube.api.core import make_geocube
import rasterstats
import pandas as pd
import geopandas as gpd
import numpy
import rioxarray
import xarray
import geopandas as gpd
import rasterstats
import os
import glob
from shapely.geometry import mapping

gdf = gpd.read_file('/content/to_test.shp')
gdf = gpd.read_file('/content/930_2039.shp')
raster = "/content/LAND_USE_2014.tif"
raster = "/content/TEST4.tif"
dic= rasterstats.zonal_stats(vectors=gdf['geometry'], raster=raster, categorical=True)#the code igonre nan vale like sea
gdf_list_v = list(range(0, len(gdf)))
gdf["id"] = gdf_list_v
#make index for later use

#ADD ID
list_loop = [0,1,2,3]
list_v =[]
gdf_list_v
for v in gdf_list_v:
  
  df = pd.DataFrame([dic[v]])
  list_v.append(df)
  

result = pd.concat(list_v)
result

temp = result.sum(1)#get the sum of pixcal in each polygon
result["sum"]= temp.tolist()

result["gdf_list_v"]= gdf_list_v

#now get the Percent for each land use
result["land_1"] = 100 * result[1]  / result["sum"]
result["land_2"] = 100 * result[2]  / result["sum"]
result["land_3"] = 100 * result[3]  / result["sum"]
result["land_4"] = 100 * result[4]  / result["sum"]



gdf_redy = gdf.merge(result, left_on='id', right_on='gdf_list_v')
#now make 4 raster ,one raster for each land use
resolution_X = 900
resolution_Y = 900
land_use_list = ["land_1","land_2","land_3","land_4"]
for land in land_use_list:
 out_grid= make_geocube(vector_data=gdf_redy, measurements=[land], resolution=(900, 900)) #for most crs negative comes first in resolution
 out_grid[land].rio.to_raster(land+".tif")