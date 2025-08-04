use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::{
    ArrayBuilder, BinaryBuilder, BinaryViewBuilder, BooleanBuilder, Float32Builder, Float64Builder,
    Int8Builder, Int16Builder, Int32Builder, Int64Builder, LargeBinaryBuilder, LargeStringBuilder,
    StringBuilder, StringViewBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    TimestampNanosecondBuilder, TimestampSecondBuilder, UInt8Builder, UInt16Builder, UInt32Builder,
    UInt64Builder, make_builder,
};
use arrow_cast::parse::string_to_datetime;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::Utc;
use geo_traits::GeometryTrait;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::*;
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{GeoArrowType, GeometryType, PolygonType};
use geozero::PropertyProcessor;
use geozero::error::GeozeroError;

pub(crate) enum GeoArrowArrayBuilder {
    Point(PointBuilder),
    LineString(LineStringBuilder),
    Polygon(PolygonBuilder),
    MultiPoint(MultiPointBuilder),
    MultiLineString(MultiLineStringBuilder),
    MultiPolygon(MultiPolygonBuilder),
    GeometryCollection(Box<GeometryCollectionBuilder>),
    Geometry(Box<GeometryBuilder>),
    Wkb(WkbBuilder<i32>),
    LargeWkb(WkbBuilder<i64>),
}

impl GeoArrowArrayBuilder {
    pub fn new(geometry_type: GeoArrowType) -> Self {
        match geometry_type {
            GeoArrowType::Point(typ) => Self::Point(PointBuilder::new(typ)),
            GeoArrowType::LineString(typ) => Self::LineString(LineStringBuilder::new(typ)),
            GeoArrowType::Polygon(typ) => Self::Polygon(PolygonBuilder::new(typ)),
            GeoArrowType::MultiPoint(typ) => Self::MultiPoint(MultiPointBuilder::new(typ)),
            GeoArrowType::MultiLineString(typ) => {
                Self::MultiLineString(MultiLineStringBuilder::new(typ))
            }
            GeoArrowType::MultiPolygon(typ) => Self::MultiPolygon(MultiPolygonBuilder::new(typ)),
            GeoArrowType::GeometryCollection(typ) => {
                Self::GeometryCollection(Box::new(GeometryCollectionBuilder::new(typ)))
            }
            GeoArrowType::Rect(typ) => Self::Polygon(PolygonBuilder::new(PolygonType::new(
                typ.dimension(),
                typ.metadata().clone(),
            ))),
            GeoArrowType::Geometry(typ) => Self::Geometry(Box::new(GeometryBuilder::new(typ))),
            GeoArrowType::Wkb(typ) => Self::Wkb(WkbBuilder::new(typ)),
            GeoArrowType::LargeWkb(typ) => Self::LargeWkb(WkbBuilder::new(typ)),
            // For now, fall back to GeometryBuilder for WkbView and Wkt types
            // We don't have builders for these types yet
            GeoArrowType::WkbView(typ) => Self::Geometry(Box::new(GeometryBuilder::new(
                GeometryType::new(typ.metadata().clone()),
            ))),
            GeoArrowType::Wkt(typ) | GeoArrowType::LargeWkt(typ) | GeoArrowType::WktView(typ) => {
                Self::Geometry(Box::new(GeometryBuilder::new(GeometryType::new(
                    typ.metadata().clone(),
                ))))
            }
        }
    }

    /// Push a geometry to this builder.
    fn push_geometry(
        &mut self,
        geometry: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        match self {
            Self::Point(builder) => builder.push_geometry(geometry),
            Self::LineString(builder) => builder.push_geometry(geometry),
            Self::Polygon(builder) => builder.push_geometry(geometry),
            Self::MultiPoint(builder) => builder.push_geometry(geometry),
            Self::MultiLineString(builder) => builder.push_geometry(geometry),
            Self::MultiPolygon(builder) => builder.push_geometry(geometry),
            Self::GeometryCollection(builder) => builder.push_geometry(geometry),
            Self::Geometry(builder) => builder.push_geometry(geometry),
            Self::Wkb(builder) => builder.push_geometry(geometry),
            Self::LargeWkb(builder) => builder.push_geometry(geometry),
        }
    }

    fn finish(self) -> Arc<dyn GeoArrowArray> {
        match self {
            Self::Point(builder) => Arc::new(builder.finish()),
            Self::LineString(builder) => Arc::new(builder.finish()),
            Self::Polygon(builder) => Arc::new(builder.finish()),
            Self::MultiPoint(builder) => Arc::new(builder.finish()),
            Self::MultiLineString(builder) => Arc::new(builder.finish()),
            Self::MultiPolygon(builder) => Arc::new(builder.finish()),
            Self::GeometryCollection(builder) => Arc::new(builder.finish()),
            Self::Geometry(builder) => Arc::new(builder.finish()),
            Self::Wkb(builder) => Arc::new(builder.finish()),
            Self::LargeWkb(builder) => Arc::new(builder.finish()),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct GeoArrowRecordBatchBuilderOptions {
    pub(crate) batch_size: Option<usize>,
    /// If true, the builder will error if it encounters a column whose name does not match one in
    /// the properties schema.
    ///
    /// If false, the builder will ignore any extra columns in the input data.
    ///
    /// This can be used to provide a projection, by passing in a `properties_schema` that only
    /// contains the fields you want.
    pub(crate) error_on_extra_columns: bool,

    pub(crate) read_geometry: bool,
}

pub(crate) struct GeoArrowRecordBatchBuilder {
    properties_schema: SchemaRef,
    columns: Vec<Box<dyn ArrayBuilder>>,
    geometry_builder: GeoArrowArrayBuilder,
    /// If true, the builder will error if it encounters a column whose name does not match one in
    /// the properties schema.
    ///
    /// If false, the builder will ignore any extra columns in the input data.
    ///
    /// This can be used to provide a projection, by passing in a `properties_schema` that only
    /// contains the fields you want.
    error_on_extra_columns: bool,
    read_geometry: bool,
}

impl GeoArrowRecordBatchBuilder {
    pub fn new(
        properties_schema: SchemaRef,
        geometry_type: GeoArrowType,
        options: &GeoArrowRecordBatchBuilderOptions,
    ) -> Self {
        let mut columns = Vec::new();
        for field in properties_schema.fields() {
            let capacity = options.batch_size.unwrap_or(0);
            let builder = make_builder(field.data_type(), capacity);
            columns.push(builder);
        }

        let geometry_builder = GeoArrowArrayBuilder::new(geometry_type);

        Self {
            properties_schema,
            columns,
            geometry_builder,
            error_on_extra_columns: options.error_on_extra_columns,
            read_geometry: options.read_geometry,
        }
    }

    /// Add a geometry to the builder.
    ///
    /// If self.read_geometry is false, this will not add the geometry to the batch.
    pub(crate) fn push_geometry(
        &mut self,
        geometry: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if self.read_geometry {
            self.geometry_builder.push_geometry(geometry)?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn num_rows(&self) -> usize {
        self.columns.first().map_or(0, |col| col.len())
    }

    pub fn finish(self) -> GeoArrowResult<RecordBatch> {
        let geometry = self.geometry_builder.finish();

        let mut fields = self.properties_schema.fields.to_vec();
        let mut columns = self
            .columns
            .into_iter()
            .map(|mut col| col.finish())
            .collect::<Vec<_>>();

        if self.read_geometry {
            fields.push(geometry.data_type().to_field("geometry", true).into());
            columns.push(geometry.into_array_ref());
        }

        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            self.properties_schema.metadata().clone(),
        ));

        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

impl PropertyProcessor for GeoArrowRecordBatchBuilder {
    fn property(
        &mut self,
        _idx: usize,
        name: &str,
        value: &geozero::ColumnValue,
    ) -> geozero::error::Result<bool> {
        let column_index = match self
            .properties_schema
            .index_of(name)
            .map_err(|_| GeozeroError::Property(format!("{name} not in properties schema")))
        {
            Ok(index) => index,
            Err(e) if self.error_on_extra_columns => {
                return Err(e);
            }
            _ => return Ok(false),
        };
        let field = self.properties_schema.field(column_index);
        let column = self.columns.get_mut(column_index).unwrap();
        push_property(column, field, value)?;
        Ok(false)
    }
}

fn push_property(
    column: &mut Box<dyn ArrayBuilder>,
    field: &Field,
    value: &geozero::ColumnValue,
) -> geozero::error::Result<()> {
    use geozero::ColumnValue::*;

    macro_rules! impl_add_value {
        ($downcast_type:ident, $v:ident) => {{
            column
                .as_any_mut()
                .downcast_mut::<$downcast_type>()
                .unwrap()
                .append_value(*$v);
        }};
    }

    match value {
        Bool(v) => impl_add_value!(BooleanBuilder, v),
        Byte(v) => impl_add_value!(Int8Builder, v),
        Short(v) => impl_add_value!(Int16Builder, v),
        Int(v) => impl_add_value!(Int32Builder, v),
        Long(v) => impl_add_value!(Int64Builder, v),
        UByte(v) => impl_add_value!(UInt8Builder, v),
        UShort(v) => impl_add_value!(UInt16Builder, v),
        UInt(v) => impl_add_value!(UInt32Builder, v),
        ULong(v) => impl_add_value!(UInt64Builder, v),
        Float(v) => impl_add_value!(Float32Builder, v),
        Double(v) => impl_add_value!(Float64Builder, v),
        String(v) | Json(v) => match field.data_type() {
            DataType::Utf8 => impl_add_value!(StringBuilder, v),
            DataType::LargeUtf8 => impl_add_value!(LargeStringBuilder, v),
            DataType::Utf8View => impl_add_value!(StringViewBuilder, v),
            _ => unreachable!(),
        },
        Binary(v) => match field.data_type() {
            DataType::Binary => impl_add_value!(BinaryBuilder, v),
            DataType::LargeBinary => impl_add_value!(LargeBinaryBuilder, v),
            DataType::BinaryView => impl_add_value!(BinaryViewBuilder, v),
            _ => unreachable!(),
        },
        DateTime(v) => {
            let dt = string_to_datetime(&Utc, v).unwrap();
            match field.data_type() {
                DataType::Timestamp(time_unit, _tz) => match time_unit {
                    TimeUnit::Second => {
                        let builder = column
                            .as_any_mut()
                            .downcast_mut::<TimestampSecondBuilder>()
                            .unwrap();
                        builder.append_value(dt.timestamp());
                    }
                    TimeUnit::Millisecond => {
                        let builder = column
                            .as_any_mut()
                            .downcast_mut::<TimestampMillisecondBuilder>()
                            .unwrap();
                        builder.append_value(dt.timestamp_millis());
                    }
                    TimeUnit::Microsecond => {
                        let builder = column
                            .as_any_mut()
                            .downcast_mut::<TimestampMicrosecondBuilder>()
                            .unwrap();
                        builder.append_value(dt.timestamp_micros());
                    }
                    TimeUnit::Nanosecond => {
                        let builder = column
                            .as_any_mut()
                            .downcast_mut::<TimestampNanosecondBuilder>()
                            .unwrap();
                        builder.append_value(dt.timestamp_nanos_opt().unwrap());
                    }
                },
                _ => unreachable!(),
            }
        }
    }
    Ok(())
}
