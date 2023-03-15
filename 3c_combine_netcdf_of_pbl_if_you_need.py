# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 05:28:13 2023

@author: YARON
"""




from dask.distributed import Client
import dask.array as da
from dask.diagnostics import ProgressBar
import xarray as xr
import os
import glob
#parameters

#the list of tile geotif tif and dir shold be in the same order
dir_pbl = ['D:/pbl/h20v05/','D:/pbl/h21v06/']#'D:/pbl/h21v05/'
#parameters


for pbl in dir_pbl:
    print(pbl)
    os.chdir(pbl)
    pbl_file = glob.glob('*.nc')
    
    tilename = os.path.basename(os.path.normpath(pbl))
    new_pbl= xr.open_mfdataset(pbl_file, combine='by_coords')    
   
    out_file = tilename +"all_year.nc"
    write_job = new_pbl.to_netcdf(out_file, compute=False)
    with ProgressBar():
        print(f"Writing to {out_file}")
        write_job.compute()    
        
    
    
    
    




        
        

