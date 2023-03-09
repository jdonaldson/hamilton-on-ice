import os


def output_name(name, extension, params):
    if "hamilton_ice_cache_dir" in params:
        hamilton_ice_cache_dir = params["hamilton_ice_cache_dir"]
    else:
        hamilton_ice_cache_dir = os.environ["HAMILTON_ICE_DATA"]
    return os.path.join(hamilton_ice_cache_dir, name + "." + extension)
