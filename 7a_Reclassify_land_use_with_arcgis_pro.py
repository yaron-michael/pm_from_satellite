#MAKE LAND USE RASTER ON ARCGIS PRO
#parameter
raster_in = "C:\\land_use_and_road\\land_use2014g.img"
raster_out =r"C:\land_use_and_road\LAND_USE_2014_4CLASS.tif"
#parameter
out_raster = arcpy.sa.Reclassify(raster_in, "Value", "1 1;2 1;3 1;4 1;5 1;6 1;7 1;8 1;9 1;10 2;11 2;12 2;13 2;14 2;15 2;16 2;17 2;18 2;20 1;21 1;22 2;23 2;24 2;25 2;26 2;27 2;28 2;29 2;31 2;32 4;33 4;34 4;35 3;36 3;37 4;38 4;39 4", "DATA"); out_raster.save(raster_out)

