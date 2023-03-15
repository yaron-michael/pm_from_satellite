#THIS CODE work on arcgis pro 2.9 with data of bantal

#parameter
#input 
iseral_grid = "C:\\land_use_and_road\\iseral_grid.tif"
railway_line = "C:\\land_use_and_road\\data.gdb\\railroads_l"
major_road_line = "C:\\land_use_and_road\\data.gdb\\major_roads_l"
highways_line =  "C:\\land_use_and_road\\data.gdb\\highways_l"
all_road = "C:\\land_use_and_road\\data.gdb\\all_road" 
save_temp_data_in_gdb ="C:\\land_use_and_road\\data.gdb\\"
#output
output_dir = "C:\\land_use_and_road\\output\\"
output_railroads = output_dir+"railroads.tif"
output_major_road = output_dir+ "major_road.tif"
output_highways= output_dir+"highways_road.tif"
output_road_density_raster = output_dir+"road_density_raster.tif"
#end of parameter


arcpy.conversion.RasterToPoint(iseral_grid,save_temp_data_in_gdb+"P_railroads", "Value")
arcpy.conversion.RasterToPoint(iseral_grid, save_temp_data_in_gdb+"P_major_road", "Value")
arcpy.conversion.RasterToPoint(iseral_grid, save_temp_data_in_gdb+"P_highways", "Value")

#THIS IS near for railway  
arcpy.analysis.Near(save_temp_data_in_gdb+"P_railroads", railway_line, None, "NO_LOCATION", "NO_ANGLE", "PLANAR", "NEAR_FID NEAR_FID;NEAR_DIST NEAR_DIST")
arcpy.conversion.PointToRaster(save_temp_data_in_gdb+"P_railroads", "NEAR_DIST", output_railroads, "MEAN", "NONE", tile, "BUILD")
#THIS IS near for major roads 
arcpy.analysis.Near(save_temp_data_in_gdb+"P_major_road", major_road_line, None, "NO_LOCATION", "NO_ANGLE", "PLANAR", "NEAR_FID NEAR_FID;NEAR_DIST NEAR_DIST")
arcpy.conversion.PointToRaster(save_temp_data_in_gdb+"P_major_road", "NEAR_DIST", output_major_road, "MEAN", "NONE", tile, "BUILD")
#THIS IS near for highways 
arcpy.analysis.Near(save_temp_data_in_gdb+"P_highways", highways_line, None, "NO_LOCATION", "NO_ANGLE", "PLANAR", "NEAR_FID NEAR_FID;NEAR_DIST NEAR_DIST")
arcpy.conversion.PointToRaster(save_temp_data_in_gdb+"P_highways", "NEAR_DIST", output_major_road, "MEAN", "NONE", tile, "BUILD")
#road Density all
out_raster = arcpy.sa.LineDensity(all_road, "NONE", 930, 465, "SQUARE_METERS"); out_raster.save(output_road_density_raster)

  
