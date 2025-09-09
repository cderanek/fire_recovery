import glob, sys, os
import xarray as xr
import rioxarray as rxr
import rasterio as rio
import numpy as np
import subprocess 

sys.path.append("workflow/utils")
from geo_utils import reproj_align_rasters

def merge_topo(topo_f_list, out_f):
    # open topo layers and get band names from file paths
    print(f'Opening all topo layers', flush=True)
    topo_f_list = [glob.glob(os.path.join(f,'clipped/*_clipped.tif'))[0] for f in topo_f_list if f!='']
    print(f'Layers are: {'\n'.join(topo_f_list)}', flush=True)
    band_names = [os.path.basename(f).split('_')[1] for f in topo_f_list]
    topo_rxr_layers = [rxr.open_rasterio(f) for f in topo_f_list]

    # reproject/align all layers
    print('Reprojecting all layers', flush=True)
    topo_rxr_layers = reproj_align_rasters('reproj_match', *topo_rxr_layers)

    print(f'Merging all topo and mask layers --> saving to {out_f}')
    topo_rxr_layers = [
        r.assign_coords(band=[band_name])
        .transpose('band','y','x')
        .fillna(-9999)
        .rio.set_nodata(-9999)
        .astype('int16') 
        for r, band_name in zip(topo_rxr_layers, band_names)
        ]

    out_rxr_merged = (
        xr.concat(topo_rxr_layers, dim=('band'))
        .rio.write_crs(topo_rxr_layers[0].rio.crs)
        .rio.set_nodata(-9999))
    out_rxr_merged.to_netcdf(out_f)
    print(f'Saved to {out_f}', flush=True)

    # save printout to summary txt file
    with open(out_f.replace('.nc', '_summary.txt'), 'w') as f:
        print(out_rxr_merged, file=f)

    return True

if __name__ == '__main__':
    print(f'Running make_topo.py with arguments {'\n'.join(sys.argv)}\n')
    topo_f_list = sys.argv[1:-2]
    out_f = sys.argv[-2]
    done_flag = sys.argv[-1]

    merge_topo(topo_f_list, out_f)
    
    subprocess.run(['touch', done_flag])