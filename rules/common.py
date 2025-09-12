import os 

# Helper fn to add prefix when in testing mode
def get_path(path, ROI_path):
    DATA_PREFIX = os.path.splitext(os.path.basename(ROI_path))[0]
    if path.startswith("data/"):
        return path.replace("data/", f"data/{DATA_PREFIX}/")
    if path.startswith("logs/"):
        return path.replace("logs/", f"logs/{DATA_PREFIX}/")
    return path