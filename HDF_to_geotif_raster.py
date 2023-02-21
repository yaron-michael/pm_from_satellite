from osgeo import gdal
from pathlib import Path
import glob
import pandas as pd
import numpy as np
from rasterio.merge import merge
import rasterio as rio
import rioxarray
import pandas as pd 
import os
import shutil

#now we have all the hdf in one dir for all of the year

#do for all the hdf in a loop
#can del other hdf file if no code is use
#Scale Factor of  MCD19A2 v006 is 0.001 
#https://lpdaac.usgs.gov/products/mcd19a2v006/
#parameters
#hdf_id dir
geo_tif_loc_to_save_in_dir_for_each_hdf_id = 'D:/data_geo_2014/' #OUTPUT
hdf_id_list =["h20v05","h21v05","h21v06"]
dir_of_hdf_file = 'D:/AOT2014/'
clip_shp_file = 'C:/PM2_5/REDY/shp2.shp'#use shp of all iseral to test
hdf_id = "h20v05"
#parameters
hdf_id_dir = hdf_id_list
for hdf_id in hdf_id_dir:
    if not os.path.exists(geo_tif_loc_to_save_in_dir_for_each_hdf_id+ hdf_id):
     os.makedirs(geo_tif_loc_to_save_in_dir_for_each_hdf_id + hdf_id)


rootdir = dir_of_hdf_file
os.chdir(dir_of_hdf_file)
sub_dir = glob.glob('*/')

for d in sub_dir:
    wokrking_dir = dir_of_hdf_file+'/'+d[:-1]
    print(d[:-1])
    os.chdir(wokrking_dir)
    dir_raster = []
    path = Path(wokrking_dir)
    hdf_files = list(path.iterdir())
    for hdf_id in hdf_id_list:
     print(hdf_id)
    #open the hdf file
     for hdf in hdf_files:
       #print(hdf)
       print(hdf)

       hdf_name = os.path.basename(hdf)
       if hdf_id ==  hdf_name[17:23]:
        print(hdf_id)   
        #extart the hdf image -each image have 4 band 
        dataset = gdal.Open(str(hdf),gdal.GA_ReadOnly)
        #print(hdf)
        sdsdict = dataset.GetMetadata()#get metdata
        #print(sdsdict)
        Orbit_amount = sdsdict["Orbit_amount"]
       # print(Orbit_amount)
        Orbit_time_stamp = sdsdict["Orbit_time_stamp"]
        subdataset =  gdal.Open(dataset.GetSubDatasets()[0][0], gdal.GA_ReadOnly)
        kwargs = {'format': 'GTiff', 'dstSRS': 'EPSG:2039'}
        #save the data
        hdf = str(hdf)
        hdf=hdf[:-3]
        #print(hdf)
        Orbit_amount = sdsdict["Orbit_amount"]
        Orbit_time_stamp = sdsdict["Orbit_time_stamp"]
        Orbit_time_stamp=Orbit_time_stamp.rsplit(' ')
        Orbit_time_stamp_list = [item for item in Orbit_time_stamp if item != '']
        thisdict = { Orbit_amount: Orbit_time_stamp_list}
        df_MET_AOT = pd.DataFrame(thisdict)
       
        #df_MET_AOT.to_csv(str(hdf)+"csv",index=False)#save metdata to csv fro later use
        #by usinge the TEMP we can del find the tif file and del later
        ds = gdal.Warp(destNameOrDestDS=str(hdf)+"TEMP"+".tif",srcDSOrSrcDSTab=subdataset, **kwargs)    
        band_num_list = list(range(1, len(df_MET_AOT)+1))
        #loop over band in save each one
        for band_num in band_num_list:
         Orbit_time_stamp[band_num-1]
         #print(Orbit_time_stamp)
         metadata = rio.open(str(hdf)+"TEMP"+".tif")
         with rio.open(str(hdf)+"TEMP"+".tif") as src:
            kwargs = src.meta
            band = src.read(band_num)
            print(Orbit_time_stamp[band_num-1])
            band_data = "Orbit_time-"+Orbit_time_stamp_list[band_num-1]+"-"
            with rio.open(str(hdf)+band_data+".tif", 'w', **kwargs) as dst:
              dst.write_band(1,band)
            src.closed
         #NOW WE clip the data
         #print (geo_tif_loc_to_save_in_dir_for_each_hdf_id)
        # print (hdf_id)
         #print(band_data)
         print(geo_tif_loc_to_save_in_dir_for_each_hdf_id + hdf_id  +"/"+band_data+"clip"+".tif")
         OutTile = gdal.Warp(geo_tif_loc_to_save_in_dir_for_each_hdf_id + hdf_id +"/"+hdf_id+"_"+band_data+"clip"+".tif", 
                    str(hdf)+band_data+".tif", 
                    cutlineDSName=clip_shp_file,
                    cropToCutline=True,
                    dstNodata = 0)
         OutTile = None 
         #del the old raster
         #str(hdf)+band_data+".tif"
         os.remove(str(hdf)+band_data+".tif")

         

#os.remove(str(hdf)+band_data+".tif")        
   
#to del file
#PATH = 'C:/AOT-3'
#EXT = "*.tif"
#all_csv_files = [file
#                 for path, subdir, files in os.walk(PATH)
#                 for file in glob(os.path.join(path, EXT))]
#print(all_csv_files)