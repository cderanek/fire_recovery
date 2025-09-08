# Helper fn to add prefix when in testing mode
def get_path(path):
    if TESTING:
        if path.startswith("data/"):
            return path.replace("data/", f"{DATA_PREFIX}/", 1)
        if path.startswith("logs/"):
            return path.replace("logs/", f"{DATA_PREFIX}/logs/", 1)
    return path