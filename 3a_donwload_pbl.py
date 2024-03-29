# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 19:31:32 2023

@author: YARON
"""


import os
import cdsapi




##parameters
dir_to_download = 'D:/pbl'
uid = ""
apikey = ""
#Sub-region extraction 
North = 34
South = 29
East = 36
West = 33
year_to_download = ["2010","2011","2012","2013","2014","2015","2016","2017","2018","2019","2020"]
##parameters
os.chdir(dir_to_download) 

c = cdsapi.Client(key=f"{uid}:{apikey}", url="https://cds.climate.copernicus.eu/api/v2")

for year in year_to_download:
  print(year)
  c.retrieve(
    'reanalysis-era5-single-levels',
    {
        'product_type': 'reanalysis',
        'variable': 'boundary_layer_height',
        'year': year,
        'month': [
            '01', '02', '03',
            '04', '05', '06',
            '07', '08', '09',
            '10', '11', '12',
        ],
        'day': [
            '01', '02', '03',
            '04', '05', '06',
            '07', '08', '09',
            '10', '11', '12',
            '13', '14', '15',
            '16', '17', '18',
            '19', '20', '21',
            '22', '23', '24',
            '25', '26', '27',
            '28', '29', '30',
            '31',
        ],
        'time': [
            '00:00', '01:00', '02:00',
            '03:00', '04:00', '05:00',
            '06:00', '07:00', '08:00',
            '09:00', '10:00', '11:00',
            '12:00', '13:00', '14:00',
            '15:00', '16:00', '17:00',
            '18:00', '19:00', '20:00',
            '21:00', '22:00', '23:00',
        ],
        'area': [
            North, West, South,
            East,
        ],
        'format': 'netcdf',
    },
    year+"pbl"+".nc")
print("donwload end")
