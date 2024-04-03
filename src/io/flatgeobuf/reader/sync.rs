//! Reader for converting FlatGeobuf to GeoArrow tables
//!
//! FlatGeobuf implements
//! [`GeozeroDatasource`](https://docs.rs/geozero/latest/geozero/trait.GeozeroDatasource.html), so
//! it would be _possible_ to implement a fully-naive conversion, where our "GeoArrowTableBuilder"
//! struct has no idea in advance what the schema, geometry type, or number of rows is. But that's
//! inefficient, especially when the input file knows that information!
//!
//! Instead, this takes a hybrid approach. In this case where we _know_ the input format is
//! FlatGeobuf, we can use extra information from the file header to help us plan out the buffers
//! for the conversion. In particular, the header can tell us the number of features in the file
//! and the geometry type contained within. In the majority of cases where these two data points
//! are known, we can be considerably more efficient by instantiating the byte length ahead of
//! time.
//!
//! Additionally, having a known schema in advance makes the non-geometry conversion easier.
//!
//! However we don't re-implement all geometry conversion from scratch! We're able to re-use all
//! the GeomProcessor conversion from geozero, after initializing buffers with a better estimate of
//! the total length.

use crate::algorithm::native::DowncastTable;
use crate::array::*;
use crate::error::{GeoArrowError, Result};
use crate::io::flatgeobuf::reader::common::{infer_schema, FlatGeobufReaderOptions};
use crate::io::geozero::array::MixedGeometryStreamBuilder;
use crate::io::geozero::table::{GeoTableBuilder, GeoTableBuilderOptions};
use crate::table::Table;
use flatgeobuf::{FgbReader, GeometryType};
use std::io::{Read, Seek};
use std::sync::Arc;

/// Read a FlatGeobuf file to a Table
pub fn read_flatgeobuf<R: Read + Seek>(
    file: &mut R,
    options: FlatGeobufReaderOptions,
) -> Result<Table> {
    let reader = FgbReader::open(file)?;

    let header = reader.header();
    if header.has_m() | header.has_t() | header.has_tm() | header.has_z() {
        return Err(GeoArrowError::General(
            "Only XY dimensions are supported".to_string(),
        ));
    }

    let schema = infer_schema(header);
    let geometry_type = header.geometry_type();

    let mut selection = if let Some((min_x, min_y, max_x, max_y)) = options.bbox {
        reader.select_bbox(min_x, min_y, max_x, max_y)?
    } else {
        reader.select_all()?
    };

    let features_count = selection.features_count();

    // TODO: propagate CRS
    let options = GeoTableBuilderOptions::new(
        options.coord_type,
        true,
        options.batch_size,
        Some(Arc::new(schema.finish())),
        features_count,
        Default::default(),
    );

    match geometry_type {
        GeometryType::Point => {
            let mut builder = GeoTableBuilder::<PointBuilder>::new_with_options(options);
            selection.process_features(&mut builder)?;
            builder.finish()
        }
        GeometryType::LineString => {
            let mut builder = GeoTableBuilder::<LineStringBuilder<i32>>::new_with_options(options);
            selection.process_features(&mut builder)?;
            builder.finish()
        }
        GeometryType::Polygon => {
            let mut builder = GeoTableBuilder::<PolygonBuilder<i32>>::new_with_options(options);
            selection.process_features(&mut builder)?;
            builder.finish()
        }
        GeometryType::MultiPoint => {
            let mut builder = GeoTableBuilder::<MultiPointBuilder<i32>>::new_with_options(options);
            selection.process_features(&mut builder)?;
            builder.finish()
        }
        GeometryType::MultiLineString => {
            let mut builder =
                GeoTableBuilder::<MultiLineStringBuilder<i32>>::new_with_options(options);
            selection.process_features(&mut builder)?;
            builder.finish()
        }
        GeometryType::MultiPolygon => {
            let mut builder =
                GeoTableBuilder::<MultiPolygonBuilder<i32>>::new_with_options(options);
            selection.process_features(&mut builder)?;
            builder.finish()
        }
        GeometryType::Unknown => {
            let mut builder =
                GeoTableBuilder::<MixedGeometryStreamBuilder<i32>>::new_with_options(options);
            selection.process_features(&mut builder)?;
            let table = builder.finish()?;
            table.downcast(true)
        }
        // TODO: Parse into a GeometryCollection array and then downcast to a single-typed array if possible.
        geom_type => Err(GeoArrowError::NotYetImplemented(format!(
            "Parsing FlatGeobuf from {:?} geometry type not yet supported",
            geom_type
        ))),
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::BufReader;

    use super::*;

    #[test]
    fn test_countries() {
        let mut filein = BufReader::new(File::open("fixtures/flatgeobuf/countries.fgb").unwrap());
        let _table = read_flatgeobuf(&mut filein, Default::default()).unwrap();
    }

    #[test]
    fn test_nz_buildings() {
        let mut filein = BufReader::new(
            File::open("fixtures/flatgeobuf/nz-building-outlines-small.fgb").unwrap(),
        );
        let _table = read_flatgeobuf(&mut filein, Default::default()).unwrap();
    }
}
