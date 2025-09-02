'''
This exploration is on pause as I consider using the MODIS landcover maps.
'''

import pandas as pd
import numpy as np
import geopandas as gpd
import xarray as xr
import rioxarray as rxr
import gc, glob, sys
from shapely.geometry import Point

sys.path.append("workflow/utils/")
from geo_utils import get_crs

sampled_pts_f = '/u/project/eordway/shared/surp_cd/fire_recovery/data/test_data/test_ROIS_pts/evt_rap_RF_sampled_points.shp' #sys.argv[1]
evt_rxr_f = '/u/project/eordway/shared/surp_cd/timeseries_data/data/CA_wide_readonly/cawide_testing/annual_evt_groupings_fullCA500m_withagrdevmask.nc' #sys.argv[2]
evt_vat_f =  '/u/project/eordway/shared/surp_cd/timeseries_data/data/CA_wide_readonly/cawide_testing/annual_evt_groupings_fullCA500m_withagrdevmask.tif.vat.dbf' #sys.arvg[3]
rap_rxr_dir = '/u/project/eordway/shared/surp_cd/fire_recovery/data/baselayers/temp/RAP/' #sys.argv[4]


def generate_sample_points(evt_rxr_f, sampled_pts_f, samples_per_year=600):
    # Create random sample of N points per year
    evt_rxr = xr.open_dataset(evt_rxr_f)
    print(evt_rxr_f)
    evt_crs = get_crs(evt_rxr_f)
    print(evt_crs)
    evt_years = evt_rxr.time.dt.year.values
    num_years = len(evt_years)
    x_coords = np.random.choice(evt_rxr.x.values, num_years*samples_per_year)
    y_coords = np.random.choice(evt_rxr.y.values, num_years*samples_per_year)
    points = gpd.GeoDataFrame(
        {
        'id': range(num_years*samples_per_year),
        'year': np.repeat(evt_years, samples_per_year),
        'geometry': [Point(x, y) for x, y in zip(x_coords, y_coords)]
        },
        crs = evt_crs
    )
    points.to_file(sampled_pts_f)

    del evt_rxr
    gc.collect()

    return points


def extract_evt_points(evt_rxr_f, points):
    evt_rxr = xr.open_dataset(evt_rxr_f)
    point_ids = points['id']
    x_coords_evt = points.geometry.x.values
    y_coords_evt = points.geometry.y.values

    # extract sample points
    evt_rxr_pts = (
        evt_rxr.sel(
            x=xr.DataArray(x_coords_evt, dims='points', coords={'points': point_ids}), 
            y=xr.DataArray(y_coords_evt, dims='points', coords={'points': point_ids}), 
            method='nearest'
        )['evt']
        .transpose('points','time')
    )
    years = evt_rxr_pts.time.dt.year.values
    point_ids = evt_rxr_pts.points.values
    rows = evt_rxr_pts.data

    del evt_rxr
    gc.collect()

    # Create df with format | point_id | year | EVT |
    df = pd.DataFrame(rows, columns=years)
    df['point_id'] = point_ids
    df = pd.melt(
        df,
        id_vars=['point_id'], 
        var_name='year', 
        value_name='EVT'
        )

    return df, years


def extract_rap_points(rap_rxr_dir, points, evt_yrs, veg_types = ['annual_forb_grass', 'perennial_forb_grass', 'shrub', 'tree']):
    # create template df with format | point_id | year | all_veg | forb_grass | shrub | tree
    df = pd.DataFrame(columns=['point_id', 'year', 'all_veg', 'forb_grass', 'shrub', 'tree'])

    # extract data from all relevant rap years
    for year in evt_yrs:
        rap_f = glob.glob(f'{rap_rxr_dir}*-{year}.tif')[0]

        # reproject points to rap crs
        rap_crs = get_crs(rap_f)
        points_rap_crs = points.to_crs(rap_crs)
        x_coords_rap = points_rap_crs.geometry.x.values
        y_coords_rap = points_rap_crs.geometry.y.values

        rap_rxr = (
            rxr.open_rasterio(rap_f)
            .sel(
                x=xr.DataArray(x_coords_rap, dims='points', coords={'points': points_rap_crs['id']}), 
                y=xr.DataArray(y_coords_rap, dims='points', coords={'points': points_rap_crs['id']}), 
                method='nearest'
            )
            .transpose('points','band')
        )
        rap_ordered_bands = ['annual_forb_grass', 'bare_ground', 'litter', 'perennial_forb_grass', 'shrub', 'tree']
        rap_rxr['band'] = rap_ordered_bands 
        rap_rxr['forb_grass'] = rap_rxr.sel(band='annual_forb_grass') + rap_rxr.sel(band='perennial_forb_grass')
        rap_rxr['all_veg'] = rap_rxr['forb_grass'] + rap_rxr['shrub'] + rap_rxr['tree']
        
        point_ids = points_rap_crs.values
        all_veg = rap_rxr.sel(band='all_veg').data
        forb_grass = rap_rxr.sel(band='forb_grass').data
        shrub = rap_rxr.sel(band='shrub').data
        tree = rap_rxr.sel(band='tree').data

        df_temp = pd.DataFrame({
            'point_id': point_ids,
            'year': year,
            'all_veg': all_veg,
            'forb_grass': forb_grass,
            'shrub': shrub,
            'tree': tree
        })

        df = pd.concat([df, df_temp])

        # memory management
        del rap_rxr, df_temp
        gc.collect()

    return df

def extract_sample_points(sampled_pts_f, evt_rxr_f, rap_rxr_f):
    # generate random points
    points = generate_sample_points(evt_rxr_f, sampled_pts_f)
    
    # extract sampled points from evt and add the year
    evt_df, evt_years = extract_evt_points(evt_rxr_f, points)
    print(evt_df)
    
    # for years with evt data, sample the corresponding rap year from rap dir
    # create df
    rap_df = extract_rap_points(rap_rxr_dir, points, evt_yrs)
    print(rap_df)

    df = pd.merge(evt_df, rap_df, on=['point_id', 'year'])
    print(df)
    return df

def evt_cleanup(df, evt_vat_f):
    # merge with points df --> only keep sampled points that match the points year

    # read vat as dict

    # drop any rows where any of forb_grass, shrub, or tree are -127 (this makes the addition to create other cols invalid for that row)

    # convert to evt names

    # drop all non-dominant lifeform, nodata vals

    return df

if __name__ == '__main__':
    # Get dataframe w/ format:
    # | pt_id | evt_name | rap_all_veg | rap_forb_grass | rap_shrub | rap_tree
    df = extract_sample_points(sampled_pts_f, evt_rxr_f, rap_rxr_dir)

    # Data cleanup
    
    # Simple RF model to map RAP % cover onto EVT categories