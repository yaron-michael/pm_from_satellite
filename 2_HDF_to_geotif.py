from osgeo import gdal
from pathlib import Path
import glob
import numpy as np
import pandas as pd 
import os
import rasterio as rio


#Scale Factor of  MCD19A2 v006 is 0.001 
#https://lpdaac.usgs.gov/products/mcd19a2v006/


#parameters
geo_tif_loc_to_save_in_dir_for_each_hdf_id = 'C:/REDY_TIF/' #OUTPUT dir
hdf_id_list =["h20v05","h21v05","h21v06"]#tile to work on dir
dir_of_hdf_file = 'C:/ALL_AOT_HDF-2010-2020/'#hdf location  for the code
clip_shp_file_CENTR = 'C:/pm/h20v05.shp'
clip_shp_file_SOTUTH = 'C:/pm/h21v06.shp'
clip_shp_file_EAST = 'C:/pm/h21v05.shp'
#parameters


#if we have tif file  dir_of_hdf_file we wil del thoes file
folders = os.listdir(dir_of_hdf_file)

for (dirname, dirs, files) in os.walk(dir_of_hdf_file):
   for file in files:
      if file.endswith('.tif'):
          source_file = os.path.join(dirname, file)
          os.remove(source_file)



hdf_id_dir = hdf_id_list
for hdf_id in hdf_id_dir:
    if not os.path.exists(geo_tif_loc_to_save_in_dir_for_each_hdf_id+ hdf_id):
     os.makedirs(geo_tif_loc_to_save_in_dir_for_each_hdf_id + hdf_id)


rootdir = dir_of_hdf_file
os.chdir(dir_of_hdf_file)
sub_dir = glob.glob('*/')

for d in sub_dir:
    wokrking_dir = dir_of_hdf_file+'/'+d[:-1]
    os.chdir(wokrking_dir)
    dir_raster = []
    path = Path(wokrking_dir)
    hdf_files = list(path.iterdir())

    for hdf in hdf_files:
        hdf_name = os.path.basename(hdf)
        hdf_id =  hdf_name[17:23]
        #extart the hdf image -each image have 4 band 
        dataset = gdal.Open(str(hdf),gdal.GA_ReadOnly)
        sdsdict = dataset.GetMetadata()#get metdata

        Orbit_amount = sdsdict["Orbit_amount"]
        Orbit_time_stamp = sdsdict["Orbit_time_stamp"]
        subdataset =  gdal.Open(dataset.GetSubDatasets()[0][0], gdal.GA_ReadOnly)
        kwargs = {'format': 'GTiff', 'dstSRS': 'EPSG:2039'}
        #save the data
        hdf = str(hdf)
        hdf=hdf[:-3]
        Orbit_amount = sdsdict["Orbit_amount"]
        Orbit_time_stamp = sdsdict["Orbit_time_stamp"]
        Orbit_time_stamp=Orbit_time_stamp.rsplit(' ')
        Orbit_time_stamp_list = [item for item in Orbit_time_stamp if item != '']
        thisdict = { Orbit_amount: Orbit_time_stamp_list}
        df_MET_AOT = pd.DataFrame(thisdict)
        #temp is a raster with all the band for the same day
        ds = gdal.Warp(destNameOrDestDS=str(hdf)+"TEMP"+".tif",srcDSOrSrcDSTab=subdataset, **kwargs)    
        band_num_list = list(range(1, len(df_MET_AOT)+1))
     
        for band_num in band_num_list:
          Orbit_time_stamp[band_num-1]
          print(Orbit_time_stamp)
          metadata = rio.open(str(hdf)+"TEMP"+".tif")
          #loop over band in save each one
          with rio.open(str(hdf)+"TEMP"+".tif") as src:
            kwargs = src.meta
            band = src.read(band_num)
            print(Orbit_time_stamp[band_num-1])
            band_data = "Orbit_time-"+Orbit_time_stamp_list[band_num-1]+"-"
            with rio.open(str(hdf)+band_data+".tif", 'w', **kwargs) as dst:
              dst.write_band(1,band)
            src.closed
              #NOW WE clip the data
            print(hdf_id)
            if hdf_id ==  "h20v05":
             clip_shp_file = clip_shp_file_CENTR
             print("h20v05")
            if hdf_id ==  "h21v06":
             clip_shp_file = clip_shp_file_SOTUTH
             print("h21v06")
            if hdf_id ==  "h21v05":
             clip_shp_file = clip_shp_file_EAST
             print("h21v05")
          print(geo_tif_loc_to_save_in_dir_for_each_hdf_id + hdf_id  +"/"+band_data+"clip"+".tif")
          OutTile = gdal.Warp(geo_tif_loc_to_save_in_dir_for_each_hdf_id + hdf_id +"/"+hdf_id+"_"+band_data+"clip"+".tif", 
                    str(hdf)+band_data+".tif", 
                    cutlineDSName=clip_shp_file,
                    cropToCutline=True,
                    dstNodata = 0)
        OutTile = None 
         # close dataset
        ds = None
        os.remove(str(hdf)+band_data+".tif")

print("end")
     
   
   

        