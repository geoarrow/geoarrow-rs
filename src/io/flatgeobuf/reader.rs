use crate::array::MutablePointArray;
use crate::array::*;
use crate::io::flatgeobuf::anyvalue::AnyMutableArray;
use arrow2::datatypes::{DataType, Field, Schema};
use flatgeobuf::{ColumnType, GeometryType};
use flatgeobuf::{FgbReader, Header};
use geozero::{FeatureProcessor, GeomProcessor, PropertyProcessor};
use std::io::Cursor;

macro_rules! define_table {
    ($name:ident, $geo_type:ty) => {
        struct $name {
            schema: Schema,
            columns: Vec<AnyMutableArray>,
            geometry: $geo_type,
        }

        impl $name {
            pub fn new(schema: Schema, features_count: Option<usize>) -> Self {
                todo!()
            }
            /// Finish off this builder, creating immutable arrays
            pub fn finish(self) -> Table {
                // TODO: convert any datetime columns to timestamps
                // https://docs.rs/arrow2/latest/arrow2/temporal_conversions/fn.utf8_to_naive_timestamp_scalar.html
                todo!()
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

define_table!(PointTableBuilder, MutablePointArray);
define_table!(LineStringTableBuilder, MutableLineStringArray<i32>);
define_table!(PolygonTableBuilder, MutablePolygonArray<i32>);
define_table!(MultiPointTableBuilder, MutableMultiPointArray<i32>);
define_table!(
    MultiLineStringTableBuilder,
    MutableMultiLineStringArray<i32>
);
define_table!(MultiPolygonTableBuilder, MutableMultiPolygonArray<i32>);

pub struct Table;

pub fn read_flatgeobuf(buf: Vec<u8>) -> Table {
    let mut cursor = Cursor::new(buf);
    let mut reader = FgbReader::open(&mut cursor).unwrap().select_all().unwrap();

    let header = reader.header();
    let features_count = reader.features_count();

    let schema = infer_schema(header);

    match header.geometry_type() {
        GeometryType::Point => {
            let mut builder = PointTableBuilder::new(schema, features_count);
            reader.process_features(&mut builder).unwrap();
            builder.finish()
        }
        GeometryType::LineString => {
            let mut builder = LineStringTableBuilder::new(schema, features_count);
            reader.process_features(&mut builder).unwrap();
            builder.finish()
        }
        GeometryType::Polygon => {
            let mut builder = PolygonTableBuilder::new(schema, features_count);
            reader.process_features(&mut builder).unwrap();
            builder.finish()
        }
        GeometryType::MultiPoint => {
            let mut builder = MultiPointTableBuilder::new(schema, features_count);
            reader.process_features(&mut builder).unwrap();
            builder.finish()
        }
        GeometryType::MultiLineString => {
            let mut builder = MultiLineStringTableBuilder::new(schema, features_count);
            reader.process_features(&mut builder).unwrap();
            builder.finish()
        }
        GeometryType::MultiPolygon => {
            let mut builder = MultiPolygonTableBuilder::new(schema, features_count);
            reader.process_features(&mut builder).unwrap();
            builder.finish()
        }
        GeometryType::Unknown => todo!(),
        _ => todo!(),
    }
}

fn infer_schema(header: Header<'_>) -> Schema {
    let columns = header.columns().unwrap();
    let mut fields = Vec::with_capacity(columns.len());
    for col in columns.into_iter() {
        let field = match col.type_() {
            ColumnType::Bool => Field::new(col.name(), DataType::Boolean, col.nullable()),
            ColumnType::Byte => Field::new(col.name(), DataType::Int8, col.nullable()),
            ColumnType::UByte => Field::new(col.name(), DataType::UInt8, col.nullable()),
            ColumnType::Short => Field::new(col.name(), DataType::Int16, col.nullable()),
            ColumnType::UShort => Field::new(col.name(), DataType::UInt16, col.nullable()),
            ColumnType::Int => Field::new(col.name(), DataType::Int32, col.nullable()),
            ColumnType::UInt => Field::new(col.name(), DataType::UInt32, col.nullable()),
            ColumnType::Long => Field::new(col.name(), DataType::Int64, col.nullable()),
            ColumnType::ULong => Field::new(col.name(), DataType::UInt64, col.nullable()),
            ColumnType::Float => Field::new(col.name(), DataType::Float32, col.nullable()),
            ColumnType::Double => Field::new(col.name(), DataType::Float64, col.nullable()),
            ColumnType::String => Field::new(col.name(), DataType::Utf8, col.nullable()),
            ColumnType::Json => Field::new(col.name(), DataType::Utf8, col.nullable()),
            ColumnType::DateTime => Field::new(col.name(), DataType::Utf8, col.nullable()),
            ColumnType::Binary => Field::new(col.name(), DataType::Binary, col.nullable()),
            // ColumnType is actually a struct, not an enum, so the rust compiler doesn't know
            // we've matched all types
            _ => unreachable!(),
        };
        fields.push(field)
    }

    Schema {
        fields,
        metadata: Default::default(),
    }
}
