//! Helpers for inferring a schema from FlatGeobuf files.

use std::io::{Read, Seek};
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use flatgeobuf::{
    FallibleStreamingIterator, FeatureIter, FeatureProperties, NotSeekable, Seekable,
};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geozero::PropertyProcessor;
use indexmap::IndexMap;

/// A scanner over FlatGeobuf files to infer a properties schema.
///
/// The FlatGeobuf specification allows for files that are [either homogeneous or
/// heterogenerous](https://worace.works/2022/03/12/flatgeobuf-implementers-guide/) in schema. For
/// homogeneous schemas, they usually have feature information in the file header, meaning that for
/// GeoArrow we can easily infer the desired schema upfront. For heterogenerous files, however,
/// there's no feature information in the header, so we must scan through actual data records in
/// the file to infer the schema.
///
/// This currently only infers the properties schema, not the geometry schema. For FlatGeobuf files
/// with unknown geometry, we currently always use the `Geometry` GeoArrow type, which allows for
/// all geometry types.
///
/// This can be used to infer a joint schema for multiple FlatGeobuf files by passing successive
/// files' data into the `process` method.
#[derive(Debug, Clone)]
pub struct FlatGeobufSchemaBuilder {
    fields: IndexMap<String, FieldRef>,
    prefer_view_types: bool,
}

impl FlatGeobufSchemaBuilder {
    /// Create a new FlatGeobuf schema builder.
    pub fn new(prefer_view_types: bool) -> Self {
        Self {
            fields: IndexMap::new(),
            prefer_view_types,
        }
    }
}

impl Default for FlatGeobufSchemaBuilder {
    fn default() -> Self {
        Self::new(true)
    }
}

impl FlatGeobufSchemaBuilder {
    /// Process the properties of a FlatGeobuf feature to build the schema.
    pub fn process<R: Read + Seek>(
        &mut self,
        selection: FeatureIter<R, Seekable>,
        max_read_records: Option<usize>,
    ) -> GeoArrowResult<()> {
        let mut selection = selection.take(max_read_records.unwrap_or(usize::MAX));

        loop {
            if let Some(feature) = selection
                .next()
                .map_err(|err| GeoArrowError::External(Box::new(err)))?
            {
                feature
                    .process_properties(self)
                    .map_err(|err| GeoArrowError::External(Box::new(err)))?;
            } else {
                return Ok(());
            }
        }
    }

    /// Process the properties of a FlatGeobuf feature to build the schema without using seek.
    pub fn process_seq<R: Read>(
        &mut self,
        selection: FeatureIter<R, NotSeekable>,
        max_read_records: Option<usize>,
    ) -> GeoArrowResult<()> {
        let mut selection = selection.take(max_read_records.unwrap_or(usize::MAX));

        loop {
            if let Some(feature) = selection
                .next()
                .map_err(|err| GeoArrowError::External(Box::new(err)))?
            {
                feature
                    .process_properties(self)
                    .map_err(|err| GeoArrowError::External(Box::new(err)))?;
            } else {
                return Ok(());
            }
        }
    }

    /// Process the properties of a FlatGeobuf feature to build the schema from an async source.
    #[cfg(feature = "async")]
    pub async fn process_async<
        T: http_range_client::AsyncHttpRangeClient + Unpin + Send + 'static,
    >(
        &mut self,
        mut selection: flatgeobuf::AsyncFeatureIter<T>,
        max_read_records: Option<usize>,
    ) -> GeoArrowResult<()> {
        let mut num_features_processed = 0;

        while let Some(feature) = selection
            .next()
            .await
            .map_err(|err| GeoArrowError::External(Box::new(err)))?
        {
            feature
                .process_properties(self)
                .map_err(|err| GeoArrowError::External(Box::new(err)))?;

            num_features_processed += 1;
            if let Some(max_read_records) = max_read_records {
                if num_features_processed >= max_read_records {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}

impl FlatGeobufSchemaBuilder {
    /// Finish the schema building process and return the resulting schema.
    pub fn finish(self) -> SchemaRef {
        Arc::new(Schema::new(self.fields.into_values().collect::<Vec<_>>()))
    }
}

impl PropertyProcessor for FlatGeobufSchemaBuilder {
    fn property(
        &mut self,
        _idx: usize,
        name: &str,
        value: &geozero::ColumnValue,
    ) -> geozero::error::Result<bool> {
        if let Some(field) = self.fields.get(name) {
            // We have already seen this field, so we skip it.
            if field != &column_value_to_field(name, value, self.prefer_view_types) {
                return Err(geozero::error::GeozeroError::Property(format!(
                    "Inconsistent field for property '{}': expected {:?}, found {:?}",
                    name,
                    field,
                    column_value_to_field(name, value, self.prefer_view_types),
                )));
            }
        } else {
            // We haven't seen this field yet, so we add it to the schema.
            let field = column_value_to_field(name, value, self.prefer_view_types);
            self.fields.insert(name.to_string(), field);
        }

        Ok(false)
    }
}

fn column_value_to_field(
    name: &str,
    value: &geozero::ColumnValue,
    prefer_view_types: bool,
) -> FieldRef {
    let data_type = match value {
        geozero::ColumnValue::Bool(_) => DataType::Boolean,
        geozero::ColumnValue::Byte(_) => DataType::Int8,
        geozero::ColumnValue::Short(_) => DataType::Int16,
        geozero::ColumnValue::Int(_) => DataType::Int32,
        geozero::ColumnValue::Long(_) => DataType::Int64,
        geozero::ColumnValue::UByte(_) => DataType::UInt8,
        geozero::ColumnValue::UShort(_) => DataType::UInt16,
        geozero::ColumnValue::UInt(_) => DataType::UInt32,
        geozero::ColumnValue::ULong(_) => DataType::UInt64,
        geozero::ColumnValue::Float(_) => DataType::Float32,
        geozero::ColumnValue::Double(_) => DataType::Float64,
        geozero::ColumnValue::String(_) => {
            if prefer_view_types {
                DataType::Utf8View
            } else {
                DataType::Utf8
            }
        }
        geozero::ColumnValue::Binary(_) => {
            if prefer_view_types {
                DataType::BinaryView
            } else {
                DataType::Binary
            }
        }
        geozero::ColumnValue::Json(_) => {
            let data_type = if prefer_view_types {
                DataType::Utf8View
            } else {
                DataType::Utf8
            };
            let field = Field::new(name, data_type, true)
                .with_extension_type(arrow_schema::extension::Json::default());
            return Arc::new(field);
        }
        geozero::ColumnValue::DateTime(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
    };

    Arc::new(Field::new(name, data_type, true))
}
