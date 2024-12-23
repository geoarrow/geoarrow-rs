use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use async_stream::stream;
use flatgeobuf::{AsyncFeatureIter, FgbFeature, GeometryType, HttpFgbReader};
use futures::future::BoxFuture;
use futures::Stream;
use geozero::{FeatureProcessor, FeatureProperties};
use http_range_client::{AsyncBufferedHttpRangeClient, AsyncHttpRangeClient};
use object_store::path::Path;
use object_store::ObjectStore;

use crate::algorithm::native::DowncastTable;
use crate::array::metadata::ArrayMetadata;
use crate::array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::io::flatgeobuf::reader::common::{infer_schema, parse_crs, FlatGeobufReaderOptions};
use crate::io::flatgeobuf::reader::object_store_reader::ObjectStoreWrapper;
use crate::io::geozero::array::GeometryStreamBuilder;
use crate::io::geozero::table::{GeoTableBuilder, GeoTableBuilderOptions};
use crate::table::Table;

/// A builder for [FlatGeobufReader]
pub struct FlatGeobufStreamBuilder<T: AsyncHttpRangeClient> {
    reader: HttpFgbReader<T>,
}

impl<T: AsyncHttpRangeClient> FlatGeobufStreamBuilder<T> {
    // pub async fn open(reader: T) -> Result<Self> {
    //     HttpFgbReader::new(client)
    //     todo!()
    // }

    pub async fn open_existing(reader: AsyncBufferedHttpRangeClient<T>) -> Result<Self> {
        let reader = HttpFgbReader::new(reader).await.unwrap();
        Ok(Self { reader })
    }

    pub async fn read(self) {
        let features = self.reader.select_all().await.unwrap();
    }
}

impl FlatGeobufStreamBuilder<ObjectStoreWrapper> {
    pub async fn open_store(
        reader: Arc<dyn ObjectStore>,
        location: Path,
        options: FlatGeobufReaderOptions,
    ) -> Result<Self> {
        let head = reader.head(&location).await?;

        let object_store_wrapper = ObjectStoreWrapper {
            reader,
            location,
            size: head.size,
        };

        let async_client = AsyncBufferedHttpRangeClient::with(object_store_wrapper, "");

        Self::open_existing(async_client).await
    }
}

enum StreamState {
    /// At the start of a new row group, or the end of the FlatGeobuf stream
    Init,
    /// Decoding a batch
    Decoding,
    /// Reading data from input
    Reading(BoxFuture<'static, ()>),
    /// Error
    Error,
}

/// An iterator over record batches from a FlatGeobuf file.
///
/// This implements [arrow_array::RecordBatchReader], which you can use to access data.
pub struct FlatGeobufStreamReader<T: AsyncHttpRangeClient> {
    selection: AsyncFeatureIter<T>,
    data_type: NativeType,
    batch_size: usize,
    properties_schema: SchemaRef,
    num_rows_remaining: Option<usize>,
    array_metadata: Arc<ArrayMetadata>,
    state: StreamState,
}

impl<T: AsyncHttpRangeClient> FlatGeobufStreamReader<T> {
    fn schema(&self) -> SchemaRef {
        let geom_field =
            self.data_type
                .to_field_with_metadata("geometry", true, &self.array_metadata);
        let mut fields = self.properties_schema.fields().to_vec();
        fields.push(Arc::new(geom_field));
        Arc::new(Schema::new_with_metadata(
            fields,
            self.properties_schema.metadata().clone(),
        ))
    }

    fn into_stream(self) -> impl Stream<Item = Result<Option<FgbFeature>>> {
        stream! {
            for await value in self.selection {
                yield value
            }
        }
    }
}

// impl<T: AsyncHttpRangeClient + Unpin + Send + 'static> Stream for FlatGeobufStreamReader<T> {
//     type Item = Result<RecordBatch>;
// }

// fn fgb_to_stream<T: AsyncHttpRangeClient>(selection: AsyncFeatureIter<T>) ->

/// Read a FlatGeobuf file to a Table asynchronously from object storage.
pub async fn read_flatgeobuf_async(
    reader: Arc<dyn ObjectStore>,
    location: Path,
    options: FlatGeobufReaderOptions,
) -> Result<Table> {
    let head = reader.head(&location).await?;

    let object_store_wrapper = ObjectStoreWrapper {
        reader,
        location,
        size: head.size,
    };
    let async_client: AsyncBufferedHttpRangeClient<ObjectStoreWrapper> =
        AsyncBufferedHttpRangeClient::with(object_store_wrapper, "");

    let reader = HttpFgbReader::new(async_client).await.unwrap();

    let header = reader.header();
    if header.has_m() | header.has_t() | header.has_tm() {
        return Err(GeoArrowError::General(
            "Only XY and XYZ dimensions are supported".to_string(),
        ));
    }
    let has_z = header.has_z();

    let schema = infer_schema(header);
    let geometry_type = header.geometry_type();
    let array_metadata = parse_crs(header.crs());

    let mut selection = if let Some((min_x, min_y, max_x, max_y)) = options.bbox {
        reader.select_bbox(min_x, min_y, max_x, max_y).await?
    } else {
        reader.select_all().await?
    };

    let features_count = selection.features_count();

    let options = GeoTableBuilderOptions::new(
        options.coord_type,
        true,
        options.batch_size,
        Some(schema),
        features_count,
        array_metadata,
    );

    macro_rules! impl_read {
        ($builder:ty, $dim:expr) => {{
            let mut builder = GeoTableBuilder::<$builder>::new_with_options($dim, options);
            while let Some(feature) = selection.next().await? {
                feature.process_properties(&mut builder)?;
                builder.properties_end()?;

                builder.push_geometry(feature.geometry_trait()?.as_ref())?;

                builder.feature_end(0)?;
            }
            selection.process_features(&mut builder).await?;
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
            let mut builder =
                GeoTableBuilder::<GeometryStreamBuilder>::new_with_options(Dimension::XY, options);
            selection.process_features(&mut builder).await?;
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
            let mut builder =
                GeoTableBuilder::<GeometryStreamBuilder>::new_with_options(Dimension::XYZ, options);
            selection.process_features(&mut builder).await?;
            let table = builder.finish()?;
            table.downcast()
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
    use std::env::current_dir;

    use super::*;
    use object_store::local::LocalFileSystem;

    #[tokio::test]
    async fn test_countries() {
        let fs = Arc::new(LocalFileSystem::new_with_prefix(current_dir().unwrap()).unwrap());
        let options = FlatGeobufReaderOptions::default();
        let table =
            read_flatgeobuf_async(fs, Path::from("fixtures/flatgeobuf/countries.fgb"), options)
                .await
                .unwrap();
        assert_eq!(table.len(), 179);
    }

    #[tokio::test]
    async fn test_countries_bbox() {
        let fs = Arc::new(LocalFileSystem::new_with_prefix(current_dir().unwrap()).unwrap());
        let options = FlatGeobufReaderOptions {
            bbox: Some((0., -90., 180., 90.)),
            ..Default::default()
        };
        let table =
            read_flatgeobuf_async(fs, Path::from("fixtures/flatgeobuf/countries.fgb"), options)
                .await
                .unwrap();
        assert_eq!(table.len(), 133);
    }

    #[tokio::test]
    async fn test_nz_buildings() {
        let fs = Arc::new(LocalFileSystem::new_with_prefix(current_dir().unwrap()).unwrap());
        let options = FlatGeobufReaderOptions::default();
        let _table = read_flatgeobuf_async(
            fs,
            Path::from("fixtures/flatgeobuf/nz-building-outlines-small.fgb"),
            options,
        )
        .await
        .unwrap();
    }
}
