'''
takes in 
    WUMI CSV with subfires, 
    WUMI dir (with internal dirs per year, per fire), 
    MTBS sev raster dir (with internal dirs for year, each year has 1 full CA tif)
    start_year, end_year
    output_dir to save extracted, clipped severity raster + burn boundary shp file

filters WUMI CSV to get WUMI fireid's for:
    CA
    start_year <= fire year <= end_year


extracts MTBS sev raster for each subfire (clipped to subfire boundary)
checks that >20% of pixels in the burn boundary are marked as burned --> writes to error log otherwise (but continues processing)

creates new dir with structure data/recovery_maps/{wumi_fireid}/spatialinfo/ containing shapefiles of fire boundaries and clipped severity rasters
the severity raster and shapefiles should be called {wumi_fireid}_burnbndy.shp and {wumi_fireid}_sevraster.tif
'''