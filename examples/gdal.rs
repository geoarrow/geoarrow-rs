use gdal::Dataset;
use geoarrow::error::GeoArrowError;
use geoarrow::io::gdal::read_gdal;
use std::path::Path;

fn run() -> Result<(), GeoArrowError> {
    // Open a dataset and access a layer
    let dataset = Dataset::open(Path::new("fixtures/roads.geojson"))?;
    let mut layer = dataset.layer(0)?;

    let _table = read_gdal(&mut layer, None)?;

    Ok(())
}

fn main() {
    run().unwrap()
}
