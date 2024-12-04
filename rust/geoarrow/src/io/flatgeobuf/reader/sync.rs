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
use crate::datatypes::Dimension;
use crate::error::{GeoArrowError, Result};
use crate::io::flatgeobuf::reader::common::{infer_schema, FlatGeobufReaderOptions};
use crate::io::geozero::array::GeometryStreamBuilder;
use crate::io::geozero::table::{GeoTableBuilder, GeoTableBuilderOptions};
use crate::table::Table;
use flatgeobuf::{FallibleStreamingIterator, FgbReader, GeometryType};
use geozero::{FeatureProcessor, FeatureProperties};
use std::io::{Read, Seek};

/// Read a FlatGeobuf file to a Table
pub fn read_flatgeobuf<R: Read + Seek>(
    file: &mut R,
    options: FlatGeobufReaderOptions,
) -> Result<Table> {
    let reader = FgbReader::open(file)?;

    let header = reader.header();
    if header.has_m() | header.has_t() | header.has_tm() {
        return Err(GeoArrowError::General(
            "Only XY and XYZ dimensions are supported".to_string(),
        ));
    }
    let has_z = header.has_z();

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
        Some(schema),
        features_count,
        Default::default(),
    );

    macro_rules! impl_read {
        ($builder:ty, $dim:expr) => {{
            let mut builder = GeoTableBuilder::<$builder>::new_with_options($dim, options);
            while let Some(feature) = selection.next()? {
                feature.process_properties(&mut builder)?;
                builder.properties_end()?;

                builder.push_geometry(feature.geometry_trait()?.as_ref())?;

                builder.feature_end(0)?;
            }
            selection.process_features(&mut builder)?;
            builder.finish()
        }};
    }

    match (geometry_type, has_z) {
        (GeometryType::Point, false) => {
            impl_read!(PointBuilder, Dimension::XY)
        }
        (GeometryType::LineString, false) => {
            impl_read!(LineStringBuilder, Dimension::XY)
        }
        (GeometryType::Polygon, false) => {
            impl_read!(PolygonBuilder, Dimension::XY)
        }
        (GeometryType::MultiPoint, false) => {
            impl_read!(MultiPointBuilder, Dimension::XY)
        }
        (GeometryType::MultiLineString, false) => impl_read!(MultiLineStringBuilder, Dimension::XY),
        (GeometryType::MultiPolygon, false) => impl_read!(MultiPolygonBuilder, Dimension::XY),
        (GeometryType::Unknown, false) => {
            let mut builder = GeoTableBuilder::<GeometryStreamBuilder>::new_with_options(
                // Hack: this is unused
                Dimension::XY,
                options,
            );
            selection.process_features(&mut builder)?;
            let table = builder.finish()?;
            table.downcast()
        }
        (GeometryType::Point, true) => {
            impl_read!(PointBuilder, Dimension::XYZ)
        }
        (GeometryType::LineString, true) => {
            impl_read!(LineStringBuilder, Dimension::XYZ)
        }
        (GeometryType::Polygon, true) => {
            impl_read!(PolygonBuilder, Dimension::XYZ)
        }
        (GeometryType::MultiPoint, true) => {
            impl_read!(MultiPointBuilder, Dimension::XYZ)
        }
        (GeometryType::MultiLineString, true) => impl_read!(MultiLineStringBuilder, Dimension::XYZ),
        (GeometryType::MultiPolygon, true) => impl_read!(MultiPolygonBuilder, Dimension::XYZ),
        (GeometryType::Unknown, true) => {
            let mut builder = GeoTableBuilder::<GeometryStreamBuilder>::new_with_options(
                // Hack: this is unused
                Dimension::XYZ,
                options,
            );
            selection.process_features(&mut builder)?;
            let table = builder.finish()?;
            // TODO: 3d downcasting not implemented
            // table.downcast()
            Ok(table)
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

    use arrow_schema::DataType;

    use crate::datatypes::NativeType;

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

    #[test]
    fn test_poly() {
        let mut filein = BufReader::new(File::open("fixtures/flatgeobuf/poly00.fgb").unwrap());
        let table = read_flatgeobuf(&mut filein, Default::default()).unwrap();

        let geom_col = table.geometry_column(None).unwrap();
        assert!(matches!(geom_col.data_type(), NativeType::Polygon(_, _)));

        let (batches, schema) = table.into_inner();
        assert_eq!(batches[0].num_rows(), 10);
        assert!(matches!(
            schema.field_with_name("AREA").unwrap().data_type(),
            DataType::Float64
        ));
        assert!(matches!(
            schema.field_with_name("EAS_ID").unwrap().data_type(),
            DataType::Int64
        ));
        assert!(matches!(
            schema.field_with_name("PRFEDEA").unwrap().data_type(),
            DataType::Utf8
        ));
    }

    #[ignore = "fails on JSON columns"]
    #[test]
    fn test_all_datatypes() {
        let mut filein =
            BufReader::new(File::open("fixtures/flatgeobuf/alldatatypes.fgb").unwrap());
        let table = read_flatgeobuf(&mut filein, Default::default()).unwrap();

        let _geom_col = table.geometry_column(None).unwrap();
        // assert!(matches!(geom_col.data_type(), NativeType::Polygon(_, _)));

        // let (batches, schema) = table.into_inner();
        // assert_eq!(batches[0].num_rows(), 10);
        // assert!(matches!(
        //     schema.field_with_name("AREA").unwrap().data_type(),
        //     DataType::Float64
        // ));
        // assert!(matches!(
        //     schema.field_with_name("EAS_ID").unwrap().data_type(),
        //     DataType::Int64
        // ));
        // assert!(matches!(
        //     schema.field_with_name("PRFEDEA").unwrap().data_type(),
        //     DataType::Utf8
        // ));
    }
}
