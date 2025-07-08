use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::cast::{AsGeoArrowArray, to_wkb};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{CoordType, GeoArrowType};
use parquet::format::KeyValue;

use crate::metadata::{GeoParquetColumnEncoding, GeoParquetMetadata};
use crate::total_bounds::{BoundingRect, bounding_rect, total_bounds};
use crate::writer::GeoParquetWriterOptions;
use crate::writer::metadata::{ColumnInfo, GeoParquetMetadataBuilder};

/// An encoder for converting GeoArrow data (in an Arrow [`RecordBatch`]) into a format that can be
/// written into the upstream [`parquet`] writer APIs.
///
/// Each encoder should represent one output GeoParquet file. The encoder also keeps track of
/// unioning the bounding boxes for each encoded GeoArrow batch, so that the output GeoParquet
/// metadata can be accurate.
pub struct GeoParquetRecordBatchEncoder {
    metadata_builder: GeoParquetMetadataBuilder,
}

impl GeoParquetRecordBatchEncoder {
    /// Create a new encoder with the given schema and options.
    ///
    /// All record batches must have this same [`Schema`].
    pub fn try_new(schema: &Schema, options: &GeoParquetWriterOptions) -> GeoArrowResult<Self> {
        let metadata_builder = GeoParquetMetadataBuilder::try_new(schema, options)?;
        Ok(Self { metadata_builder })
    }

    /// Infer the output Arrow schema.
    ///
    /// This is the schema that must be used when constructing the upstream Parquet writer.
    ///
    /// This schema returned by this function matches the schema of the RecordBatches returned by
    /// [`Self::encode_record_batch`].
    pub fn target_schema(&self) -> SchemaRef {
        self.metadata_builder.output_schema.clone()
    }

    /// Encode a record batch into a GeoParquet-compatible format.
    ///
    /// This also updates the internal bounding box tracking
    ///
    /// This [`RecordBatch`] must have the same schema as the [`Schema`] passed into
    /// [`GeoParquetRecordBatchEncoder::try_new`].
    pub fn encode_record_batch(&mut self, batch: &RecordBatch) -> GeoArrowResult<RecordBatch> {
        let output_schema = self.target_schema();
        encode_record_batch(batch, &mut self.metadata_builder, output_schema)
    }

    /// Convert this encoder into a [`GeoParquetMetadata`] object.
    ///
    /// Call this before closing the Parquet file, so that you can write the GeoParquet metadata
    /// with
    /// [`ArrowWriter::append_key_value_metadata`][parquet::arrow::arrow_writer::ArrowWriter::append_key_value_metadata].
    ///
    /// Usually you'll want to use [`into_keyvalue`][Self::into_keyvalue] instead, unless you have
    /// a need to access information out of the finalized metadata. If you use this method, you'll
    /// need to manually encode the [`GeoParquetMetadata`] to JSON and then create a [`KeyValue`]
    /// with a key named `"geo"`.
    pub fn into_geoparquet_metadata(self) -> GeoParquetMetadata {
        self.metadata_builder.finish()
    }

    /// Convert this encoder in to a [`KeyValue`] that can be added to the Parquet file's metadata.
    ///
    /// Call this before closing the Parquet file, so that you can write the GeoParquet metadata
    /// with
    /// [`ArrowWriter::append_key_value_metadata`][parquet::arrow::arrow_writer::ArrowWriter::append_key_value_metadata].
    pub fn into_keyvalue(self) -> GeoArrowResult<KeyValue> {
        let geo_meta = self.into_geoparquet_metadata();
        Ok(KeyValue::new(
            "geo".to_string(),
            serde_json::to_string(&geo_meta)
                .map_err(|err| GeoArrowError::GeoParquet(err.to_string()))?,
        ))
    }
}

pub(super) fn encode_record_batch(
    batch: &RecordBatch,
    metadata_builder: &mut GeoParquetMetadataBuilder,
    output_schema: SchemaRef,
) -> GeoArrowResult<RecordBatch> {
    // This is a vec of Option<ArrayRef> so that we can insert the new covering columns at the
    // right indices, even if we iterate in a different order as before
    let mut output_columns: Vec<Option<ArrayRef>> =
        batch.columns().iter().map(|x| Some(x.clone())).collect();

    output_columns.resize(output_schema.fields().len(), None);
    for (column_idx, column_info) in metadata_builder.columns.iter_mut() {
        let array = batch.column(*column_idx);
        let field = batch.schema_ref().field(*column_idx);
        column_info.update_geometry_types(array, field)?;

        let (encoded_column, array_bounds) = encode_column(array, field, column_info)?;
        output_columns[*column_idx] = Some(encoded_column);

        if let Some(covering_field_idx) = column_info.covering_field_idx {
            let covering = bounding_rect(from_arrow_array(array, field)?.as_ref())?;
            output_columns[covering_field_idx] = Some(covering.into_array_ref());
        }

        column_info.update_bbox(&array_bounds);
    }

    Ok(RecordBatch::try_new(
        metadata_builder.output_schema.clone(),
        output_columns
            .into_iter()
            .map(|x| x.expect("Should have set all columns"))
            .collect(),
    )?)
}

fn encode_column(
    array: &dyn Array,
    field: &Field,
    column_info: &mut ColumnInfo,
) -> GeoArrowResult<(ArrayRef, BoundingRect)> {
    let geo_arr = from_arrow_array(array, field)?;
    let array_bounds = total_bounds(geo_arr.as_ref())?;
    let encoded_array = match column_info.encoding {
        GeoParquetColumnEncoding::WKB => encode_wkb_column(geo_arr.as_ref())?,
        _ => encode_native_column(geo_arr.as_ref()),
    };
    Ok((encoded_array, array_bounds))
}

/// Encode column as WKB
fn encode_wkb_column(geo_arr: &dyn GeoArrowArray) -> GeoArrowResult<ArrayRef> {
    Ok(to_wkb::<i32>(geo_arr)?.to_array_ref())
}

/// Encode column as GeoArrow.
///
/// Note that the GeoParquet specification requires separated coord type!
fn encode_native_column(geo_arr: &dyn GeoArrowArray) -> ArrayRef {
    macro_rules! impl_into_coord_type {
        ($cast_func:ident) => {
            geo_arr
                .$cast_func()
                .clone()
                .into_coord_type(CoordType::Separated)
                .to_array_ref()
        };
    }
    match geo_arr.data_type() {
        GeoArrowType::Point(_) => impl_into_coord_type!(as_point),
        GeoArrowType::LineString(_) => impl_into_coord_type!(as_line_string),
        GeoArrowType::Polygon(_) => impl_into_coord_type!(as_polygon),
        GeoArrowType::MultiPoint(_) => impl_into_coord_type!(as_multi_point),
        GeoArrowType::MultiLineString(_) => impl_into_coord_type!(as_multi_line_string),
        GeoArrowType::MultiPolygon(_) => impl_into_coord_type!(as_multi_polygon),
        GeoArrowType::Geometry(_) => impl_into_coord_type!(as_geometry),
        GeoArrowType::GeometryCollection(_) => impl_into_coord_type!(as_geometry_collection),
        _ => geo_arr.to_array_ref(),
    }
}
