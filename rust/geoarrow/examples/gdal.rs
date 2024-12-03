use gdal::Dataset;
use geoarrow::error::GeoArrowError;
use geoarrow::io::gdal::read_gdal;
use geoarrow::table::Table;
use std::path::Path;

fn run() -> Result<(), GeoArrowError> {
    // Open a dataset and access a layer
    let dataset = Dataset::open(Path::new("fixtures/roads.geojson"))?;
    let mut layer = dataset.layer(0)?;

    let reader = read_gdal(&mut layer, None)?;
    let table: Table = reader.try_into()?;
    dbg!(&table.schema());

    Ok(())
}

fn main() {
    run().unwrap()
}
