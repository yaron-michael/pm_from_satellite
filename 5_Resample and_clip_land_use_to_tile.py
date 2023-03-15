import numpy as np
import xarray as xr
import rioxarray
import geopandas
from shapely.geometry import box
from pathlib import Path    
import os


#parameters
tile_list_shp = ['C:/land_use_and_road/h20v05_crs2039.shp','C:/land_use_and_road/h21v05_crs2039.shp','C:/land_use_and_road/h21v06_crs2039.shp']
geotif_of_tile = ['C:/land_use_and_road/h20v05.tif','C:/land_use_and_road/h21v05.tif','C:/land_use_and_road/h21v06.tif']
#geotif_land_use all of israel
geotif_land_use = ['C:/land_use_and_road/temp/residence.tif','C:/land_use_and_road/temp/industry.tif','C:/land_use_and_road/temp/agriculture.tif','C:/land_use_and_road/temp/Open_space.tif']
output_dir = 'C:/land_use_and_road/output//'
#parameters
for tile, tile_grid in zip(tile_list_shp, geotif_of_tile):
    geodf = geopandas.read_file(tile)
    temp_list = []
    temp_name = []
    temp_match =[]
    #now  we take file name 5 first letter-its the tile ID
    tile_name = Path(tile).name 
  
    tile_name = tile_name[0:6]  
    # we now have the  tile ID
    print(tile_name)
    for land in geotif_land_use:
        xds1 = rioxarray.open_rasterio(land)#land use
        name_of_var = Path(land).name
        #we clip the raster of the land use
        clipped = xds1.rio.clip(geodf.geometry.values, geodf.crs, drop=False, invert=False)#clip the raster  
     
        to_match = rioxarray.open_rasterio(tile_grid)#raster to math the 
        xds_repr_match = clipped.rio.reproject_match(to_match)
        temp_list.append(xds_repr_match)
        temp_name.append(name_of_var[:-4])
    temp_list[0]       
    #now enter the data to netcdf
    ds = temp_list[0].to_dataset(name = temp_name[0])
    ds[temp_name[1]] = temp_list[1]
    ds[temp_name[2]] = temp_list[2]
    ds[temp_name[3]] = temp_list[3]
    #now combibe  all for one netcdf
    ds.to_netcdf(output_dir+tile_name+"_land__use.nc")

