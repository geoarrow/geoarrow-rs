use std::io::{Read, Seek};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dbase::{FieldInfo, FieldType, FieldValue, Record};
use geozero::FeatureProcessor;
use shapefile::{Reader, ShapeReader, ShapeType};

use crate::array::metadata::ArrayMetadata;
use crate::array::{
    CoordType, MultiLineStringBuilder, MultiPointBuilder, MultiPolygonBuilder, PointBuilder,
};
use geoarrow_schema::Dimension;
use crate::error::{GeoArrowError, Result};
use crate::io::geozero::table::builder::anyvalue::AnyBuilder;
use crate::io::geozero::table::builder::properties::PropertiesBatchBuilder;
use crate::io::geozero::table::{GeoTableBuilder, GeoTableBuilderOptions};
use crate::table::Table;

/// Options for the Shapefile reader
#[derive(Debug, Clone, Default)]
pub struct ShapefileReaderOptions {
    /// The GeoArrow coordinate type to use in the geometry arrays.
    pub coord_type: CoordType,

    /// The number of rows in each batch.
    pub batch_size: Option<usize>,

    /// The CRS to assign to the file. Read this from the `.prj` file in the same directory with
    /// the same name.
    pub crs: Option<String>,
}

// TODO:
// stretch goal: return a record batch reader.
/// Read a Shapefile into a [Table].
pub fn read_shapefile<T: Read + Seek>(
    shp_reader: T,
    dbf_reader: T,
    options: ShapefileReaderOptions,
) -> Result<Table> {
    let dbf_reader = dbase::Reader::new(dbf_reader).unwrap();
    let shp_reader = ShapeReader::new(shp_reader).unwrap();

    let header = shp_reader.header();

    let dbf_fields = dbf_reader.fields().to_vec();
    let schema = infer_schema(&dbf_fields);
    let geometry_type = header.shape_type;

    let features_count = dbf_reader.header().num_records as usize;
    let features_count = if features_count > 0 {
        Some(features_count)
    } else {
        None
    };

    let array_metadata = options
        .crs
        .map(ArrayMetadata::from_unknown_crs_type)
        .unwrap_or_default();

    let table_builder_options = GeoTableBuilderOptions::new(
        options.coord_type,
        true,
        options.batch_size,
        Some(schema),
        features_count,
        Arc::new(array_metadata),
    );

    let mut reader = Reader::new(shp_reader, dbf_reader);

    // TODO: these might work in a macro

    match geometry_type {
        ShapeType::Point => {
            let mut builder = GeoTableBuilder::<PointBuilder>::new_with_options(
                Dimension::XY,
                table_builder_options,
            );

            for geom_and_record in
                reader.iter_shapes_and_records_as::<shapefile::Point, dbase::Record>()
            {
                let (geom, record) = geom_and_record.unwrap();

                // Process properties
                let prop_builder = builder.properties_builder_mut();
                prop_builder.add_record(record, &dbf_fields)?;

                // Hack to advance internal row number
                builder.properties_end()?;

                let geom = super::scalar::Point::new(&geom);
                builder.geom_builder().push_point(Some(&geom));

                // Hack to advance internal row number
                builder.feature_end(0)?;
            }
            builder.finish()
        }
        ShapeType::PointZ => {
            let mut builder = GeoTableBuilder::<PointBuilder>::new_with_options(
                Dimension::XYZ,
                table_builder_options,
            );

            for geom_and_record in
                reader.iter_shapes_and_records_as::<shapefile::PointZ, dbase::Record>()
            {
                let (geom, record) = geom_and_record.unwrap();

                // Process properties
                let prop_builder = builder.properties_builder_mut();
                prop_builder.add_record(record, &dbf_fields)?;

                // Hack to advance internal row number
                builder.properties_end()?;

                let geom = super::scalar::PointZ::new(&geom);
                builder.geom_builder().push_point(Some(&geom));

                // Hack to advance internal row number
                builder.feature_end(0)?;
            }
            builder.finish()
        }
        ShapeType::Multipoint => {
            let mut builder = GeoTableBuilder::<MultiPointBuilder>::new_with_options(
                Dimension::XY,
                table_builder_options,
            );

            for geom_and_record in
                reader.iter_shapes_and_records_as::<shapefile::Multipoint, dbase::Record>()
            {
                let (geom, record) = geom_and_record.unwrap();

                // Process properties
                let prop_builder = builder.properties_builder_mut();
                prop_builder.add_record(record, &dbf_fields)?;

                // Hack to advance internal row number
                builder.properties_end()?;

                let geom = super::scalar::MultiPoint::new(&geom);
                builder.geom_builder().push_multi_point(Some(&geom))?;

                // Hack to advance internal row number
                builder.feature_end(0)?;
            }
            builder.finish()
        }
        ShapeType::MultipointZ => {
            let mut builder = GeoTableBuilder::<MultiPointBuilder>::new_with_options(
                Dimension::XYZ,
                table_builder_options,
            );

            for geom_and_record in
                reader.iter_shapes_and_records_as::<shapefile::MultipointZ, dbase::Record>()
            {
                let (geom, record) = geom_and_record.unwrap();

                // Process properties
                let prop_builder = builder.properties_builder_mut();
                prop_builder.add_record(record, &dbf_fields)?;

                // Hack to advance internal row number
                builder.properties_end()?;

                let geom = super::scalar::MultiPointZ::new(&geom);
                builder.geom_builder().push_multi_point(Some(&geom))?;

                // Hack to advance internal row number
                builder.feature_end(0)?;
            }
            builder.finish()
        }
        ShapeType::Polyline => {
            let mut builder = GeoTableBuilder::<MultiLineStringBuilder>::new_with_options(
                Dimension::XY,
                table_builder_options,
            );

            for geom_and_record in
                reader.iter_shapes_and_records_as::<shapefile::Polyline, dbase::Record>()
            {
                let (geom, record) = geom_and_record.unwrap();

                // Process properties
                let prop_builder = builder.properties_builder_mut();
                prop_builder.add_record(record, &dbf_fields)?;

                // Hack to advance internal row number
                builder.properties_end()?;

                let geom = super::scalar::Polyline::new(&geom);
                builder.geom_builder().push_multi_line_string(Some(&geom))?;

                // Hack to advance internal row number
                builder.feature_end(0)?;
            }
            builder.finish()
        }
        ShapeType::PolylineZ => {
            let mut builder = GeoTableBuilder::<MultiLineStringBuilder>::new_with_options(
                Dimension::XYZ,
                table_builder_options,
            );

            for geom_and_record in
                reader.iter_shapes_and_records_as::<shapefile::PolylineZ, dbase::Record>()
            {
                let (geom, record) = geom_and_record.unwrap();

                // Process properties
                let prop_builder = builder.properties_builder_mut();
                prop_builder.add_record(record, &dbf_fields)?;

                // Hack to advance internal row number
                builder.properties_end()?;

                let geom = super::scalar::PolylineZ::new(&geom);
                builder.geom_builder().push_multi_line_string(Some(&geom))?;

                // Hack to advance internal row number
                builder.feature_end(0)?;
            }
            builder.finish()
        }
        ShapeType::Polygon => {
            let mut builder = GeoTableBuilder::<MultiPolygonBuilder>::new_with_options(
                Dimension::XY,
                table_builder_options,
            );

            for geom_and_record in
                reader.iter_shapes_and_records_as::<shapefile::Polygon, dbase::Record>()
            {
                let (geom, record) = geom_and_record.unwrap();

                // Process properties
                let prop_builder = builder.properties_builder_mut();
                prop_builder.add_record(record, &dbf_fields)?;

                // Hack to advance internal row number
                builder.properties_end()?;

                let geom = super::scalar::MultiPolygon::new(geom);
                builder.geom_builder().push_multi_polygon(Some(&geom))?;

                // Hack to advance internal row number
                builder.feature_end(0)?;
            }
            builder.finish()
        }
        ShapeType::PolygonZ => {
            let mut builder = GeoTableBuilder::<MultiPolygonBuilder>::new_with_options(
                Dimension::XYZ,
                table_builder_options,
            );

            for geom_and_record in
                reader.iter_shapes_and_records_as::<shapefile::PolygonZ, dbase::Record>()
            {
                let (geom, record) = geom_and_record.unwrap();

                // Process properties
                let prop_builder = builder.properties_builder_mut();
                prop_builder.add_record(record, &dbf_fields)?;

                // Hack to advance internal row number
                builder.properties_end()?;

                let geom = super::scalar::MultiPolygonZ::new(geom);
                builder.geom_builder().push_multi_polygon(Some(&geom))?;

                // Hack to advance internal row number
                builder.feature_end(0)?;
            }
            builder.finish()
        }
        t => Err(GeoArrowError::General(format!(
            "Unsupported shapefile geometry type: {}",
            t
        ))),
    }
    // ?;

    // // Assign CRS onto the table
    // let geom_col_idx = table.default_geometry_column_idx()?;
    // let col = table.geometry_column(Some(geom_col_idx))?;
    // let field = col.data_type().to_field_with_metadata("geometry", true, &array_metadata);

    // table.remove_column(geom_col_idx);
    // table.append_column(field.into(), col.array_refs())?;
    // Ok(table)
}

impl PropertiesBatchBuilder {
    fn add_record(&mut self, record: Record, fields: &[FieldInfo]) -> Result<()> {
        for field_info in fields {
            let field_name = field_info.name();
            let builder = self
                .columns
                .get_mut(field_name)
                .ok_or(GeoArrowError::General(format!(
                    "Builder with name {} does not exist",
                    field_name
                )))?;
            let field_value = record
                .get(field_name)
                .ok_or(GeoArrowError::General(format!(
                    "Field value with name {} does not exist",
                    field_name
                )))?;
            builder.add_field_value(field_value)?;
        }

        Ok(())
    }
}

impl AnyBuilder {
    fn add_field_value(&mut self, value: &FieldValue) -> Result<()> {
        match value {
            FieldValue::Character(v) => {
                if let Some(v) = v {
                    self.as_string_mut().unwrap().append_value(v);
                } else {
                    self.append_null();
                }
            }
            FieldValue::Currency(v) | FieldValue::Double(v) => {
                self.as_float64_mut().unwrap().append_value(*v);
            }
            FieldValue::Date(v) => {
                if let Some(v) = v {
                    let unix_days = v.to_unix_days();
                    self.as_date32_mut().unwrap().append_value(unix_days);
                } else {
                    self.append_null();
                }
            }
            FieldValue::DateTime(v) => {
                let unix_timestamp_s = v.to_unix_timestamp();
                // seconds to microseconds
                let unix_timestamp_us = unix_timestamp_s * 1000 * 1000;
                self.as_date_time_mut()
                    .unwrap()
                    .0
                    .append_value(unix_timestamp_us);
            }
            FieldValue::Float(v) => {
                if let Some(v) = v {
                    self.as_float32_mut().unwrap().append_value(*v);
                } else {
                    self.append_null();
                }
            }
            FieldValue::Integer(v) => {
                self.as_int32_mut().unwrap().append_value(*v);
            }
            FieldValue::Logical(v) => {
                if let Some(v) = v {
                    self.as_bool_mut().unwrap().append_value(*v);
                } else {
                    self.append_null();
                }
            }
            FieldValue::Memo(v) => {
                self.as_string_mut().unwrap().append_value(v);
            }
            FieldValue::Numeric(v) => {
                if let Some(v) = v {
                    self.as_float64_mut().unwrap().append_value(*v);
                } else {
                    self.append_null();
                }
            }
        };
        Ok(())
    }
}

fn infer_schema(fields: &[FieldInfo]) -> SchemaRef {
    let mut out_fields = Vec::with_capacity(fields.len());

    for field in fields {
        let name = field.name().to_string();
        let field = match field.field_type() {
            FieldType::Numeric | FieldType::Double | FieldType::Currency => {
                Field::new(name, DataType::Float64, true)
            }
            FieldType::Character | FieldType::Memo => Field::new(name, DataType::Utf8, true),
            FieldType::Float => Field::new(name, DataType::Float32, true),
            FieldType::Integer => Field::new(name, DataType::Int32, true),
            FieldType::Logical => Field::new(name, DataType::Boolean, true),
            FieldType::Date => Field::new(name, DataType::Date32, true),
            FieldType::DateTime => Field::new(
                name,
                // The dbase DateTime only stores data at second precision, but we currently build
                // millisecond arrays, because that's our existing code path
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                true,
            ),
        };
        out_fields.push(Arc::new(field));
    }

    Arc::new(Schema::new(out_fields))
}
