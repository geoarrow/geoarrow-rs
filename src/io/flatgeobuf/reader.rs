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

use crate::array::linestring::LineStringCapacity;
use crate::array::multilinestring::MultiLineStringCapacity;
use crate::array::multipoint::MultiPointCapacity;
use crate::array::multipolygon::MultiPolygonCapacity;
use crate::array::polygon::PolygonCapacity;
use crate::array::PointBuilder;
use crate::array::*;
use crate::error::{GeoArrowError, Result};
use crate::io::flatgeobuf::anyvalue::AnyMutableArray;
use crate::table::GeoTable;
use crate::trait_::GeometryArrayBuilder;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, Int8Builder, StringBuilder, UInt16Builder, UInt32Builder, UInt64Builder,
    UInt8Builder,
};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use flatgeobuf::{ColumnType, GeometryType};
use flatgeobuf::{FgbReader, Header};
use geozero::{FeatureProcessor, GeomProcessor, PropertyProcessor};
use std::io::{Read, Seek};
use std::sync::Arc;

macro_rules! define_table_builder {
    ($name:ident, $geo_type:ty) => {
        struct $name {
            schema: Arc<Schema>,
            columns: Vec<AnyMutableArray>,
            geometry: $geo_type,
        }

        impl $name {
            pub fn finish(self) -> Result<GeoTable> {
                // Set geometry column after property columns
                let geometry_column_index = self.columns.len();

                let mut columns = Vec::with_capacity(self.columns.len() + 1);

                for mut_column in self.columns {
                    columns.push(mut_column.finish()?)
                }

                // Add geometry column and geometry field
                let geometry_column = self.geometry.into_array_ref();
                let geometry_field = Arc::new(Field::new(
                    "geometry",
                    geometry_column.data_type().clone(),
                    true,
                ));

                columns.push(geometry_column);

                // Add geometry field to schema
                let schema = self.schema;
                let mut fields: Vec<_> = schema.fields.into_iter().map(|f| f.to_owned()).collect();
                fields.push(geometry_field);
                let new_schema = Arc::new(Schema::new(fields));

                let batch = RecordBatch::try_new(new_schema.clone(), columns)?;
                GeoTable::try_new(new_schema, vec![batch], geometry_column_index)
            }
        }

        impl PropertyProcessor for $name {
            fn property(
                &mut self,
                idx: usize,
                _name: &str,
                value: &geozero::ColumnValue,
            ) -> geozero::error::Result<bool> {
                let column = &mut self.columns[idx];
                column.add_value(value);
                Ok(false)
            }
        }

        // delegate all methods to the geometry array
        impl GeomProcessor for $name {
            fn dimensions(&self) -> geozero::CoordDimensions {
                self.geometry.dimensions()
            }

            fn multi_dim(&self) -> bool {
                self.geometry.multi_dim()
            }

            fn srid(&mut self, srid: Option<i32>) -> geozero::error::Result<()> {
                self.geometry.srid(srid)
            }

            fn xy(&mut self, x: f64, y: f64, idx: usize) -> geozero::error::Result<()> {
                self.geometry.xy(x, y, idx)
            }

            fn coordinate(
                &mut self,
                x: f64,
                y: f64,
                z: Option<f64>,
                m: Option<f64>,
                t: Option<f64>,
                tm: Option<u64>,
                idx: usize,
            ) -> geozero::error::Result<()> {
                self.geometry.coordinate(x, y, z, m, t, tm, idx)
            }

            fn empty_point(&mut self, idx: usize) -> geozero::error::Result<()> {
                self.geometry.empty_point(idx)
            }

            fn point_begin(&mut self, idx: usize) -> geozero::error::Result<()> {
                self.geometry.point_begin(idx)
            }

            fn point_end(&mut self, idx: usize) -> geozero::error::Result<()> {
                self.geometry.point_end(idx)
            }

            fn multipoint_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
                self.geometry.multipoint_begin(size, idx)
            }

            fn multipoint_end(&mut self, idx: usize) -> geozero::error::Result<()> {
                self.geometry.multipoint_end(idx)
            }

            fn linestring_begin(
                &mut self,
                tagged: bool,
                size: usize,
                idx: usize,
            ) -> geozero::error::Result<()> {
                self.geometry.linestring_begin(tagged, size, idx)
            }

            fn linestring_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
                self.geometry.linestring_end(tagged, idx)
            }

            fn multilinestring_begin(
                &mut self,
                size: usize,
                idx: usize,
            ) -> geozero::error::Result<()> {
                self.geometry.multilinestring_begin(size, idx)
            }

            fn multilinestring_end(&mut self, idx: usize) -> geozero::error::Result<()> {
                self.geometry.multilinestring_end(idx)
            }

            fn polygon_begin(
                &mut self,
                tagged: bool,
                size: usize,
                idx: usize,
            ) -> geozero::error::Result<()> {
                self.geometry.polygon_begin(tagged, size, idx)
            }

            fn polygon_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
                self.geometry.polygon_end(tagged, idx)
            }

            fn multipolygon_begin(
                &mut self,
                size: usize,
                idx: usize,
            ) -> geozero::error::Result<()> {
                self.geometry.multipolygon_begin(size, idx)
            }

            fn multipolygon_end(&mut self, idx: usize) -> geozero::error::Result<()> {
                self.geometry.multipolygon_end(idx)
            }

            fn geometrycollection_begin(
                &mut self,
                size: usize,
                idx: usize,
            ) -> geozero::error::Result<()> {
                self.geometry.geometrycollection_begin(size, idx)
            }

            fn geometrycollection_end(&mut self, idx: usize) -> geozero::error::Result<()> {
                self.geometry.geometrycollection_end(idx)
            }

            fn circularstring_begin(
                &mut self,
                size: usize,
                idx: usize,
            ) -> geozero::error::Result<()> {
                self.geometry.circularstring_begin(size, idx)
            }

            fn circularstring_end(&mut self, idx: usize) -> geozero::error::Result<()> {
                self.geometry.circularstring_end(idx)
            }

            fn compoundcurve_begin(
                &mut self,
                size: usize,
                idx: usize,
            ) -> geozero::error::Result<()> {
                self.geometry.compoundcurve_begin(size, idx)
            }

            fn compoundcurve_end(&mut self, idx: usize) -> geozero::error::Result<()> {
                self.geometry.compoundcurve_end(idx)
            }

            fn curvepolygon_begin(
                &mut self,
                size: usize,
                idx: usize,
            ) -> geozero::error::Result<()> {
                self.geometry.curvepolygon_begin(size, idx)
            }

            fn curvepolygon_end(&mut self, idx: usize) -> geozero::error::Result<()> {
                self.geometry.curvepolygon_end(idx)
            }

            fn multicurve_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
                self.geometry.multicurve_begin(size, idx)
            }

            fn multicurve_end(&mut self, idx: usize) -> geozero::error::Result<()> {
                self.geometry.multicurve_end(idx)
            }

            fn multisurface_begin(
                &mut self,
                size: usize,
                idx: usize,
            ) -> geozero::error::Result<()> {
                self.geometry.multisurface_begin(size, idx)
            }

            fn multisurface_end(&mut self, idx: usize) -> geozero::error::Result<()> {
                self.geometry.multisurface_end(idx)
            }

            fn triangle_begin(
                &mut self,
                tagged: bool,
                size: usize,
                idx: usize,
            ) -> geozero::error::Result<()> {
                self.geometry.triangle_begin(tagged, size, idx)
            }

            fn triangle_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
                self.geometry.triangle_end(tagged, idx)
            }

            fn polyhedralsurface_begin(
                &mut self,
                size: usize,
                idx: usize,
            ) -> geozero::error::Result<()> {
                self.geometry.polyhedralsurface_begin(size, idx)
            }

            fn polyhedralsurface_end(&mut self, idx: usize) -> geozero::error::Result<()> {
                self.geometry.polyhedralsurface_end(idx)
            }

            fn tin_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
                self.geometry.tin_begin(size, idx)
            }

            fn tin_end(&mut self, idx: usize) -> geozero::error::Result<()> {
                self.geometry.tin_end(idx)
            }
        }

        impl FeatureProcessor for $name {}
    };
}

define_table_builder!(PointTableBuilder, PointBuilder);
define_table_builder!(LineStringTableBuilder, LineStringBuilder<i32>);
define_table_builder!(PolygonTableBuilder, PolygonBuilder<i32>);
define_table_builder!(MultiPointTableBuilder, MultiPointBuilder<i32>);
define_table_builder!(MultiLineStringTableBuilder, MultiLineStringBuilder<i32>);
define_table_builder!(MultiPolygonTableBuilder, MultiPolygonBuilder<i32>);

impl PointTableBuilder {
    pub fn new(
        schema: Arc<Schema>,
        columns: Vec<AnyMutableArray>,
        features_count: Option<usize>,
    ) -> Self {
        Self {
            schema,
            columns,
            geometry: PointBuilder::with_capacity(features_count.unwrap_or(0)),
        }
    }
}

impl LineStringTableBuilder {
    pub fn new(
        schema: Arc<Schema>,
        columns: Vec<AnyMutableArray>,
        features_count: Option<usize>,
    ) -> Self {
        let capacity = LineStringCapacity::new(0, features_count.unwrap_or(0));
        Self {
            schema,
            columns,
            geometry: LineStringBuilder::with_capacity(capacity),
        }
    }
}

impl PolygonTableBuilder {
    pub fn new(
        schema: Arc<Schema>,
        columns: Vec<AnyMutableArray>,
        features_count: Option<usize>,
    ) -> Self {
        let capacity = PolygonCapacity::new(0, 0, features_count.unwrap_or(0));
        Self {
            schema,
            columns,
            geometry: PolygonBuilder::with_capacity(capacity),
        }
    }
}

impl MultiPointTableBuilder {
    pub fn new(
        schema: Arc<Schema>,
        columns: Vec<AnyMutableArray>,
        features_count: Option<usize>,
    ) -> Self {
        let capacity = MultiPointCapacity::new(0, features_count.unwrap_or(0));
        Self {
            schema,
            columns,
            geometry: MultiPointBuilder::with_capacity(capacity),
        }
    }
}

impl MultiLineStringTableBuilder {
    pub fn new(
        schema: Arc<Schema>,
        columns: Vec<AnyMutableArray>,
        features_count: Option<usize>,
    ) -> Self {
        let capacity = MultiLineStringCapacity::new(0, 0, features_count.unwrap_or(0));
        Self {
            schema,
            columns,
            geometry: MultiLineStringBuilder::with_capacity(capacity),
        }
    }
}

impl MultiPolygonTableBuilder {
    pub fn new(
        schema: Arc<Schema>,
        columns: Vec<AnyMutableArray>,
        features_count: Option<usize>,
    ) -> Self {
        let capacity = MultiPolygonCapacity::new(0, 0, 0, features_count.unwrap_or(0));
        Self {
            schema,
            columns,
            geometry: MultiPolygonBuilder::with_capacity(capacity),
        }
    }
}

/// Read a FlatGeobuf file to a GeoTable
pub fn read_flatgeobuf<R: Read + Seek>(file: &mut R) -> Result<GeoTable> {
    let mut reader = FgbReader::open(file)?.select_all()?;

    let header = reader.header();
    let features_count = reader.features_count();

    let (schema, initialized_columns) = infer_schema_and_init_columns(header, features_count);

    match header.geometry_type() {
        GeometryType::Point => {
            let mut builder = PointTableBuilder::new(schema, initialized_columns, features_count);
            reader.process_features(&mut builder)?;
            builder.finish()
        }
        GeometryType::LineString => {
            let mut builder =
                LineStringTableBuilder::new(schema, initialized_columns, features_count);
            reader.process_features(&mut builder)?;
            builder.finish()
        }
        GeometryType::Polygon => {
            let mut builder = PolygonTableBuilder::new(schema, initialized_columns, features_count);
            reader.process_features(&mut builder)?;
            builder.finish()
        }
        GeometryType::MultiPoint => {
            let mut builder =
                MultiPointTableBuilder::new(schema, initialized_columns, features_count);
            reader.process_features(&mut builder)?;
            builder.finish()
        }
        GeometryType::MultiLineString => {
            let mut builder =
                MultiLineStringTableBuilder::new(schema, initialized_columns, features_count);
            reader.process_features(&mut builder)?;
            builder.finish()
        }
        GeometryType::MultiPolygon => {
            let mut builder =
                MultiPolygonTableBuilder::new(schema, initialized_columns, features_count);
            reader.process_features(&mut builder)?;
            builder.finish()
        }
        // TODO: Parse into a GeometryCollection array and then downcast to a single-typed array if possible.
        GeometryType::Unknown => Err(GeoArrowError::NotYetImplemented(
            "Parsing FlatGeobuf from unknown geometry type not yet supported.".to_string(),
        )),
        geom_type => Err(GeoArrowError::NotYetImplemented(format!(
            "Parsing FlatGeobuf from {:?} geometry type not yet supported",
            geom_type
        ))),
    }
}

fn infer_schema_and_init_columns(
    header: Header<'_>,
    features_count: Option<usize>,
) -> (Arc<Schema>, Vec<AnyMutableArray>) {
    let features_count = features_count.unwrap_or(0);

    let columns = header.columns().unwrap();
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays: Vec<AnyMutableArray> = Vec::with_capacity(columns.len());

    for col in columns.into_iter() {
        let (field, arr) = match col.type_() {
            ColumnType::Bool => (
                Field::new(col.name(), DataType::Boolean, col.nullable()),
                BooleanBuilder::with_capacity(features_count).into(),
            ),
            ColumnType::Byte => (
                Field::new(col.name(), DataType::Int8, col.nullable()),
                Int8Builder::with_capacity(features_count).into(),
            ),
            ColumnType::UByte => (
                Field::new(col.name(), DataType::UInt8, col.nullable()),
                UInt8Builder::with_capacity(features_count).into(),
            ),
            ColumnType::Short => (
                Field::new(col.name(), DataType::Int16, col.nullable()),
                Int16Builder::with_capacity(features_count).into(),
            ),
            ColumnType::UShort => (
                Field::new(col.name(), DataType::UInt16, col.nullable()),
                UInt16Builder::with_capacity(features_count).into(),
            ),
            ColumnType::Int => (
                Field::new(col.name(), DataType::Int32, col.nullable()),
                Int32Builder::with_capacity(features_count).into(),
            ),
            ColumnType::UInt => (
                Field::new(col.name(), DataType::UInt32, col.nullable()),
                UInt32Builder::with_capacity(features_count).into(),
            ),
            ColumnType::Long => (
                Field::new(col.name(), DataType::Int64, col.nullable()),
                Int64Builder::with_capacity(features_count).into(),
            ),
            ColumnType::ULong => (
                Field::new(col.name(), DataType::UInt64, col.nullable()),
                UInt64Builder::with_capacity(features_count).into(),
            ),
            ColumnType::Float => (
                Field::new(col.name(), DataType::Float32, col.nullable()),
                Float32Builder::with_capacity(features_count).into(),
            ),
            ColumnType::Double => (
                Field::new(col.name(), DataType::Float64, col.nullable()),
                Float64Builder::with_capacity(features_count).into(),
            ),
            ColumnType::String => (
                Field::new(col.name(), DataType::Utf8, col.nullable()),
                AnyMutableArray::String(StringBuilder::with_capacity(
                    features_count,
                    features_count,
                )),
            ),
            ColumnType::Json => (
                Field::new(col.name(), DataType::Utf8, col.nullable()),
                AnyMutableArray::Json(StringBuilder::with_capacity(features_count, features_count)),
            ),
            ColumnType::DateTime => (
                Field::new(
                    col.name(),
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    col.nullable(),
                ),
                AnyMutableArray::DateTime(StringBuilder::with_capacity(
                    features_count,
                    features_count,
                )),
            ),
            ColumnType::Binary => (
                Field::new(col.name(), DataType::Binary, col.nullable()),
                BinaryBuilder::with_capacity(features_count, features_count).into(),
            ),
            // ColumnType is actually a struct, not an enum, so the rust compiler doesn't know
            // we've matched all types
            _ => unreachable!(),
        };
        fields.push(field);
        arrays.push(arr);
    }

    let schema = Arc::new(Schema::new(fields));
    (schema, arrays)
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::BufReader;

    use super::*;

    #[test]
    fn test_countries() {
        let mut filein = BufReader::new(File::open("fixtures/flatgeobuf/countries.fgb").unwrap());
        let _table = read_flatgeobuf(&mut filein).unwrap();
    }

    #[test]
    fn test_nz_buildings() {
        let mut filein = BufReader::new(
            File::open("fixtures/flatgeobuf/nz-building-outlines-small.fgb").unwrap(),
        );
        let _table = read_flatgeobuf(&mut filein).unwrap();
    }
}
