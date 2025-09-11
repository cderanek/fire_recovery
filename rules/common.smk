# Helper fn to add prefix when in testing mode
def get_path(path):
    if path.startswith("data/"):
        return path.replace("data/", f"data/{DATA_PREFIX}/")
    if path.startswith("logs/get_baselayers/"):
        return path.replace("logs/get_baselayers/", f"logs/get_baselayers/{DATA_PREFIX}/")
    return path