use arrow_schema::{DataType, Field, SchemaBuilder, TimeUnit};
use geozero_shp::reader::{FieldInfo, FieldType};
use geozero_shp::{Reader, ShapeType};
use std::io::{Read, Seek};
use std::sync::Arc;

use crate::array::{CoordType, MultiPointBuilder};
use crate::error::Result;
use crate::io::geozero::table::{GeoTableBuilder, GeoTableBuilderOptions};
use crate::table::GeoTable;

pub fn read_shapefile<R: Read + Seek>(
    shp_file: &mut R,
    dbf_file: &mut R,
    shx_file: Option<&mut R>,
    coord_type: CoordType,
    batch_size: Option<usize>,
) -> Result<GeoTable> {
    let mut reader = Reader::new(shp_file)?;
    let header = reader.header();
    let bbox = header.bbox;

    reader.add_dbf_source(dbf_file)?;
    if let Some(shx_file) = shx_file {
        reader.add_index_source(shx_file)?;
    };

    let properties_schema = infer_schema(reader.dbf_fields()?.as_slice());
    let options = GeoTableBuilderOptions::new(
        coord_type,
        true,
        batch_size,
        Some(Arc::new(properties_schema.finish())),
        None,
    );

    match header.shape_type {
        ShapeType::Point | ShapeType::Multipoint => {
            let mut builder = GeoTableBuilder::<MultiPointBuilder<i32>>::new_with_options(options);
            let iter = reader.iter_features(&mut builder)?;
            builder.finish()
        }
    }
}

fn infer_schema(dbf_fields: &[&FieldInfo]) -> SchemaBuilder {
    let mut schema = SchemaBuilder::with_capacity(dbf_fields.len());

    for dbf_field in dbf_fields {
        let name = dbf_field.name();
        let field = match dbf_field.field_type() {
            FieldType::Character | FieldType::Memo => Field::new(name, DataType::Utf8, true),
            FieldType::Double | FieldType::Numeric => Field::new(name, DataType::Float64, true),
            FieldType::Float => Field::new(name, DataType::Float32, true),
            FieldType::Logical => Field::new(name, DataType::Boolean, true),
            FieldType::Date => Field::new(name, DataType::Date32, true),
            FieldType::DateTime => {
                Field::new(name, DataType::Timestamp(TimeUnit::Microsecond, None), true)
            }
            FieldType::Integer => Field::new(name, DataType::Int64, true),
            FieldType::Currency => todo!(),
        };
    }

    schema
}
