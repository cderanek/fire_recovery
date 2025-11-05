nodata_value=-128

# Define band information for output merged tif
band_info = {
    'matched_recovery_time': {
        'description': f'Recovery time (recovery in seasons; nodata={nodata_value}, never recovered=-1)',
        'nodata': nodata_value,
        'dtype': np.int8,
        'units': 'seasons'
    },
    'matched_recovery_status': {
        'description': f'Recovery status (never recovered=0, recovered=1, nodata={nodata_value})',
        'nodata': nodata_value,
        'dtype': np.int8,
        'units': 'dimensionless'
    },
    'prefire_baseline_recovery_time': {
        'description': f'Recovery time (recovery in seasons; nodata={nodata_value}, never recovered=-1)',
        'nodata': nodata_value,
        'dtype': np.int8,
        'units': 'seasons'
    },
    'prefire_baseline_recovery_status': {
        'description': f'Recovery status (never recovered=0, recovered=1, nodata={nodata_value})',
        'nodata': nodata_value,
        'dtype': np.int8,
        'units': 'dimensionless'
    },
    'vegetation_type': {
        'description': 'Vegetation type; according to vegetation_type_dict exported.',
        'nodata': nodata_value,
        'dtype': np.int8,
        'units': 'dimensionless'
    },
    'UID_h': {
        'description': 'UID thousands/hundreds digit (e.g. for UID=812, UID_h=8; for UID=1127, UID_h=11)',
        'nodata': nodata_value,
        'dtype': np.int8,
        'units': 'dimensionless'
    },
    'UID_to': {
        'description': 'UID tens and ones digit (e.g. for UID=812, UID_to=12)',
        'nodata': nodata_value,
        'dtype': np.int8,
        'units': 'dimensionless'
    },
    'fire_yr': {
        'description': 'Fire year, counting from 1982. EX: fire_year=0 is for 1982; fire_year=5 is for 1987; ',
        'nodata': nodata_value,
        'dtype': np.int8,
        'units': 'years since 1982'
    },
    'severity': {
        'description': 'Fire severity',
        'nodata': nodata_value,
        'dtype': np.int8,
        'units': 'dimensionless'
    },
    'burn_bndy_dist': {
        'description': 'Distance, in hundreds of meters, from the burn boundary. Distance is reported rounded to the nearest 100 meters (ceiling). Distances >12,700m are reported as 127.',
        'nodata': nodata_value,
        'dtype': np.int8,
        'units': 'dimensionless'
    }
}

band_names = list(band_info.keys())
encoding = {'_FillValue': nodata_value, 'dtype': 'int8'}