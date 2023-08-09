
- `roads.geojson` from https://github.com/georust/gdal/blob/61d79f9e6c7c3c9dc7ba0206112ad8b03146fe59/fixtures/roads.geojson

### `nybb.arrow` (MultiPolygon)

```
wget https://www.nyc.gov/assets/planning/download/zip/data-maps/open-data/nybb_16a.zip
extract nybb_16a.zip
ogr2ogr nybb.arrow nybb_16a -lco GEOMETRY_ENCODING=GEOARROW -nlt PROMOTE_TO_MULTI
```
