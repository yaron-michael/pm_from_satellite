# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 00:02:22 2023

@author: YARON
"""

from geocube.api.core import make_geocube
import rasterstats
import pandas as pd
import geopandas as gpd
import numpy
import rioxarray
import xarray
import rasterstats
import os
import glob
from shapely.geometry import mapping
import scipy
import itertools
import rasterio
from shapely.geometry import box

##parameters
raster_to_grid = 'C:/land_use_and_road/ISERAL_ALL_2039.tif'
grid_temp = 'C:/land_use_and_road/temp_grid.shp'
raster = 'C:/land_use_and_road/LAND_USE_2014_4CLASS_2039_.tif' #LAND USE RASTER
output_dir = 'C:/land_use_and_road/output//'
resolution_X = 930
resolution_Y = 930
#parameters





with rasterio.open(raster_to_grid) as dataset:  
    data = dataset.read(1)

    t = dataset.transform

    move_x = t[0]
    move_y = t[4]

    height = dataset.height
    width = dataset.width 

    polygons = []
    indices = list(itertools.product(range(width), range(height)))
    for x,y in indices:
        x_min, y_max = t * (x,y)
        x_max = x_min + move_x
        y_min = y_max + move_y
        polygons.append(box(x_min, y_min, x_max, y_max))

data_list = []
for x,y in indices:
    data_list.append(data[y,x])

gdf = gpd.GeoDataFrame(data=data_list, crs={'init':'epsg:2039'}, geometry=polygons, columns=['value'])
gdf.to_file(grid_temp, driver='ESRI Shapefile')

gdf = gpd.read_file(grid_temp)
dic= rasterstats.zonal_stats(vectors=gdf['geometry'], raster=raster, categorical=True)#the code igonre nan vale like sea

gdf_list_v = list(range(0, len(gdf)))
gdf["id"] = gdf_list_v
list_v = list(range(0, len(dic)))
list_v =[]
gdf_list_v
for v in gdf_list_v:
  
  df = pd.DataFrame([dic[v]])
  list_v.append(df)
  
result = pd.concat(list_v)


temp = result.sum(1)#get the sum of pixcal in each polygon
result["sum"]= temp.tolist()
result["gdf_list_v"]= gdf_list_v
#now get the Percent for each land use
result["residence"] = 100 * result[1]  / result["sum"]#residence
result["industry"] = 100 * result[2]  / result["sum"]#industry
result["agriculture"] = 100 * result[3]  / result["sum"]#agriculture
result["Open_space"] = 100 * result[4]  / result["sum"]#Open space
result
gdf_redy = gdf.merge(result, left_on='id', right_on='gdf_list_v')
gdf_redy
#now make 4 raster ,one raster for each land use

land_use_list = ["residence","industry","agriculture","Open_space"]
for land in land_use_list:
 out_grid= make_geocube(vector_data=gdf_redy, measurements=[land], resolution=(resolution_X, resolution_Y)) 
 out_grid[land].rio.to_raster(output_dir+land+".tif")