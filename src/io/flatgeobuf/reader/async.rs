use std::sync::Arc;

use flatgeobuf::{GeometryType, HttpFgbReader};
use http_range_client::AsyncBufferedHttpRangeClient;
use object_store::path::Path;
use object_store::ObjectStore;

use crate::algorithm::native::DowncastTable;
use crate::array::*;
use crate::error::{GeoArrowError, Result};
use crate::io::flatgeobuf::reader::common::{infer_schema, FlatGeobufReaderOptions};
use crate::io::flatgeobuf::reader::object_store_reader::ObjectStoreWrapper;
use crate::io::geozero::array::MixedGeometryStreamBuilder;
use crate::io::geozero::table::{GeoTableBuilder, GeoTableBuilderOptions};
use crate::table::Table;

pub async fn read_flatgeobuf_async<T: ObjectStore>(
    reader: T,
    location: Path,
    options: FlatGeobufReaderOptions,
) -> Result<Table> {
    let head = reader.head(&location).await?;

    let object_store_wrapper = ObjectStoreWrapper {
        reader,
        location,
        size: head.size,
    };
    let async_client = AsyncBufferedHttpRangeClient::with(object_store_wrapper, "");

    let reader = HttpFgbReader::new(async_client).await.unwrap();

    let header = reader.header();
    if header.has_m() | header.has_t() | header.has_tm() | header.has_z() {
        return Err(GeoArrowError::General(
            "Only XY dimensions are supported".to_string(),
        ));
    }

    let schema = infer_schema(header);
    let geometry_type = header.geometry_type();

    let mut selection = if let Some((min_x, min_y, max_x, max_y)) = options.bbox {
        reader.select_bbox(min_x, min_y, max_x, max_y).await?
    } else {
        reader.select_all().await?
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
            let mut builder = GeoTableBuilder::<PointBuilder<2>>::new_with_options(options);
            selection.process_features(&mut builder).await?;
            builder.finish()
        }
        GeometryType::LineString => {
            let mut builder =
                GeoTableBuilder::<LineStringBuilder<i32, 2>>::new_with_options(options);
            selection.process_features(&mut builder).await?;
            builder.finish()
        }
        GeometryType::Polygon => {
            let mut builder = GeoTableBuilder::<PolygonBuilder<i32, 2>>::new_with_options(options);
            selection.process_features(&mut builder).await?;
            builder.finish()
        }
        GeometryType::MultiPoint => {
            let mut builder =
                GeoTableBuilder::<MultiPointBuilder<i32, 2>>::new_with_options(options);
            selection.process_features(&mut builder).await?;
            builder.finish()
        }
        GeometryType::MultiLineString => {
            let mut builder =
                GeoTableBuilder::<MultiLineStringBuilder<i32, 2>>::new_with_options(options);
            selection.process_features(&mut builder).await?;
            builder.finish()
        }
        GeometryType::MultiPolygon => {
            let mut builder =
                GeoTableBuilder::<MultiPolygonBuilder<i32, 2>>::new_with_options(options);
            selection.process_features(&mut builder).await?;
            builder.finish()
        }
        GeometryType::Unknown => {
            let mut builder =
                GeoTableBuilder::<MixedGeometryStreamBuilder<i32>>::new_with_options(options);
            selection.process_features(&mut builder).await?;
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
    use std::env::current_dir;

    use super::*;
    use object_store::local::LocalFileSystem;

    #[tokio::test]
    async fn test_countries() {
        let fs = LocalFileSystem::new_with_prefix(current_dir().unwrap()).unwrap();
        let options = FlatGeobufReaderOptions::default();
        let table =
            read_flatgeobuf_async(fs, Path::from("fixtures/flatgeobuf/countries.fgb"), options)
                .await
                .unwrap();
        assert_eq!(table.len(), 179);
    }

    #[tokio::test]
    async fn test_countries_bbox() {
        let fs = LocalFileSystem::new_with_prefix(current_dir().unwrap()).unwrap();
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
        let fs = LocalFileSystem::new_with_prefix(current_dir().unwrap()).unwrap();
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
