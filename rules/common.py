import os 
import pandas as pd


# Helper fn to add prefix when in testing mode
def get_path(path, ROI_path):
    DATA_PREFIX = os.path.splitext(os.path.basename(ROI_path))[0]
    for path_start in ['data/', 'logs/', 'results/']:
        if path.startswith(path_start):
            return path.replace(path_start, f"{path_start}/{DATA_PREFIX}/")
    return path


def get_fireids(wumi_csv_path):
    return list(pd.read_csv(wumi_csv_path)['fireid'].values)