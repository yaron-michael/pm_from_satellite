#each modeis tiil have in the aot VALE 4 band -each band is a diifndt hour
#to deonloas pbl https://herbie.readthedocs.io/en/latest/index.html
#need to add the ablity to retry download 
from modis_tools.auth import ModisSession
from modis_tools.resources import CollectionApi, GranuleApi
from modis_tools.granule_handler import GranuleHandler
import datetime
from datetime import timedelta 
import os
#parameters
AOT = [33, 28 , 38,36.00] #bounding_box
download_data_to ='C:/2019/'
username = "yaron1205"
password = "RFEx3LKsWX1z"
start_date = datetime.date(2019, 1, 1)
end_date   = datetime.date(2020, 1, 1)


#parameters

def alt_elem(list, index=2):
    for i, elem in enumerate(list, start=1):
        if not i % index:
           yield tuple(list[i-index:i])

dates_list = [ start_date + datetime.timedelta(n) for n in range(int ((end_date - start_date).days))]
print(dates_list)
#we need to have list with two date for each loop
dates_to_loop = []


for date_vale in dates_list:
    a = date_vale 
    b = date_vale + timedelta(days = 1) 

   
# Authenticate a session
    session = ModisSession(username=username, password=password)
# Query the MODIS catalog for collections
    collection_client = CollectionApi(session=session)
    collections = collection_client.query(short_name="MCD19A2", version="006")

# Query the selected collection for granules
    granule_client = GranuleApi.from_collection(collections[0], session=session)
   # print(dates_to_loop[i])
    #a = dates_to_loop[i]
    #b = dates_to_loop[i + 1]
    #print(dates_to_loop[i + 1])
    print(a)
    a1 = a.strftime('%Y-%m-%d')
    b1 = b.strftime('%Y-%m-%d')

    
#Israel_granules = granule_client.query(start_date="2017-01-01", end_date="2017-01-02", bounding_box=AOT)
    Israel_granules = granule_client.query(start_date=a1, end_date=b1, bounding_box=AOT)
    # Download the granules
    
    name_of_dir = date_vale.strftime('%d-%m-%Y')
    os.mkdir(download_data_to+name_of_dir)

  
    GranuleHandler.download_from_granules(Israel_granules, session,path=download_data_to+name_of_dir)#each wil day have is own dir with 4 subdir 
    







