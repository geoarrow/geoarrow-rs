# import geoarrow.rust.core as gars

# import geopandas as gpd
# import pandas as pd
# from geoarrow.rust.core import GeoTable

# gdf = gpd.read_file(gpd.datasets.get_path('nybb'))

# table = GeoTable.from_geopandas(gdf)
# exploded_table = table.explode()
# round_trip_gdf = exploded_table.to_geopandas()
# geopandas_impl = gdf.explode(index_parts=False)

# all(round_trip_gdf.geometry.reset_index(drop=True) == geopandas_impl.geometry.reset_index(drop=True))
# # True! Yay!

# ###
# # Perf testing
# large = pd.concat([gdf] * 1000)

# table = GeoTable.from_geopandas(large)
# %timeit exploded_table = table.explode()
# # 590 µs ± 33.2 µs per loop (mean ± std. dev. of 7 runs, 1,000 loops each)

# %timeit gdf_exp = large.explode(index_parts=False)
# # 262 ms ± 10.9 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)
