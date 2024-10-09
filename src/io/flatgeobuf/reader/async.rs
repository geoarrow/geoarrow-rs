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
    let async_client = AsyncBufferedHttpRangeClient::with(object_store_wrapper, "");

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
        Some(schema),
        features_count,
        Default::default(),
    );

    macro_rules! impl_read {
        ($builder:ty) => {{
            let mut builder = GeoTableBuilder::<$builder>::new_with_options(options);
            selection.process_features(&mut builder).await?;
            builder.finish()
        }};
    }

    match (geometry_type, has_z) {
        (GeometryType::Point, false) => impl_read!(PointBuilder<2>),
        (GeometryType::LineString, false) => impl_read!(LineStringBuilder<2>),
        (GeometryType::Polygon, false) => impl_read!(PolygonBuilder<2>),
        (GeometryType::MultiPoint, false) => impl_read!(MultiPointBuilder<2>),
        (GeometryType::MultiLineString, false) => impl_read!(MultiLineStringBuilder<2>),
        (GeometryType::MultiPolygon, false) => impl_read!(MultiPolygonBuilder<2>),
        (GeometryType::Unknown, false) => {
            let mut builder =
                GeoTableBuilder::<MixedGeometryStreamBuilder<2>>::new_with_options(options);
            selection.process_features(&mut builder).await?;
            let table = builder.finish()?;
            table.downcast(true)
        }
        (GeometryType::Point, true) => impl_read!(PointBuilder<3>),
        (GeometryType::LineString, true) => impl_read!(LineStringBuilder<3>),
        (GeometryType::Polygon, true) => impl_read!(PolygonBuilder<3>),
        (GeometryType::MultiPoint, true) => impl_read!(MultiPointBuilder<3>),
        (GeometryType::MultiLineString, true) => impl_read!(MultiLineStringBuilder<3>),
        (GeometryType::MultiPolygon, true) => impl_read!(MultiPolygonBuilder<3>),
        (GeometryType::Unknown, true) => {
            let mut builder =
                GeoTableBuilder::<MixedGeometryStreamBuilder<3>>::new_with_options(options);
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
