import geodatasets
import geopandas as gpd

nybb = gpd.read_file(geodatasets.get_path("nybb"))
nybb.to_parquet("nybb_wkb.parquet", geometry_encoding="WKB")
nybb.to_parquet(
    "nybb_wkb_covering.parquet", geometry_encoding="WKB", write_covering_bbox=True
)
nybb.to_parquet("nybb_geoarrow.parquet", geometry_encoding="geoarrow")
