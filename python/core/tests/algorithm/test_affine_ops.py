import geoarrow.rust.core as gars
import geodatasets
import geopandas as gpd
import shapely.geometry
from affine import Affine

nybb_path = geodatasets.get_path("nybb")


def test_affine_function():
    gdf = gpd.read_file(nybb_path)
    # shapely_area = gdf.geometry.area
    assert isinstance(gdf, gpd.GeoDataFrame)

    table = gars.from_geopandas(gdf)
    geom = table.geometry

    xoff = 10
    yoff = 20

    affine = Affine.translation(xoff, yoff)
    translated = gars.affine_transform(geom, affine)
    orig_geom = geom.chunk(0)[0]
    translated_geom = translated.chunk(0)[0]

    first_coord = shapely.geometry.shape(orig_geom).geoms[0].exterior.coords[0]
    first_coord_translated = (
        shapely.geometry.shape(translated_geom).geoms[0].exterior.coords[0]
    )

    assert first_coord_translated[0] - xoff == first_coord[0]
    assert first_coord_translated[1] - yoff == first_coord[1]
