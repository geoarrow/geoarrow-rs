use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use geoarrow_array::array::{GenericWktArray, WktArray, from_arrow_array};
use geoarrow_array::builder::{
    GeometryCollectionBuilder, LineStringBuilder, MultiLineStringBuilder, MultiPointBuilder,
    MultiPolygonBuilder, PointBuilder, PolygonBuilder,
};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{
    CoordType, Dimension, GeoArrowType, GeometryCollectionType, LineStringType,
    MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::reader::{GeoParquetReaderBuilder, parse_record_batch};
use crate::test::geoarrow_data_example_files;

fn dimension_path_part(dim: Dimension) -> &'static str {
    match dim {
        Dimension::XY => "",
        Dimension::XYZ => "-z",
        Dimension::XYM => "-m",
        Dimension::XYZM => "-zm",
    }
}

/// Construct the filepath to files in geoarrow-data
///
/// This suffix should either be "geo" or "native"
fn geoparquet_filepath(data_type: GeoArrowType, suffix: &str) -> PathBuf {
    let path = geoarrow_data_example_files();
    let mut fname = "example_".to_string();

    use GeoArrowType::*;
    match data_type {
        Point(typ) => {
            fname.push_str("point");
            fname.push_str(dimension_path_part(typ.dimension()));
        }
        LineString(typ) => {
            fname.push_str("linestring");
            fname.push_str(dimension_path_part(typ.dimension()));
        }
        Polygon(typ) => {
            fname.push_str("polygon");
            fname.push_str(dimension_path_part(typ.dimension()));
        }
        MultiPoint(typ) => {
            fname.push_str("multipoint");
            fname.push_str(dimension_path_part(typ.dimension()));
        }
        MultiLineString(typ) => {
            fname.push_str("multilinestring");
            fname.push_str(dimension_path_part(typ.dimension()));
        }
        MultiPolygon(typ) => {
            fname.push_str("multipolygon");
            fname.push_str(dimension_path_part(typ.dimension()));
        }
        GeometryCollection(typ) => {
            fname.push_str("geometrycollection");
            fname.push_str(dimension_path_part(typ.dimension()));
        }
        _ => todo!(),
    }
    fname.push('_');
    fname.push_str(suffix);
    fname.push_str(".parquet");
    path.join(fname)
}

/// Read a GeoParquet file and return the WKT and geometry arrays; columns 0 and 1.
fn read_gpq_file(path: impl AsRef<Path>) -> (GenericWktArray<i32>, Arc<dyn GeoArrowArray>) {
    println!("reading path: {:?}", path.as_ref());
    let file = File::open(path).unwrap();
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let native_geoarrow_schema = reader_builder
        .native_geoarrow_schema(CoordType::Separated)
        .unwrap();
    let reader = reader_builder.build().unwrap();

    let batches = reader
        .map(|batch| parse_record_batch(batch?, native_geoarrow_schema.clone()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches.len(), 1);

    let batch = batches[0].clone();

    let wkt_arr = WktArray::new(
        batch.column(0).as_string::<i32>().clone(),
        Default::default(),
    );
    let geo_arr = from_arrow_array(batch.column(1), native_geoarrow_schema.field(1)).unwrap();

    (wkt_arr, geo_arr)
}

#[test]
fn point() {
    for dim in [
        Dimension::XY,
        Dimension::XYZ,
        Dimension::XYM,
        Dimension::XYZM,
    ] {
        for file_type in ["geo", "native"] {
            let expected_typ =
                PointType::new(dim, Default::default()).with_coord_type(CoordType::Separated);
            let path = geoparquet_filepath(expected_typ.clone().into(), file_type);
            let (wkt_arr, geo_arr) = read_gpq_file(path);

            assert_eq!(geo_arr.data_type(), expected_typ.clone().into());

            let wkt_geoms = wkt_arr
                .iter()
                .map(|x| x.transpose().unwrap())
                .collect::<Vec<_>>();
            let from_wkt = PointBuilder::from_nullable_geometries(&wkt_geoms, expected_typ)
                .unwrap()
                .finish();

            assert_eq!(geo_arr.as_point(), &from_wkt);
        }
    }
}

#[test]
fn linestring() {
    for dim in [
        Dimension::XY,
        Dimension::XYZ,
        Dimension::XYM,
        Dimension::XYZM,
    ] {
        for file_type in ["geo", "native"] {
            let expected_typ =
                LineStringType::new(dim, Default::default()).with_coord_type(CoordType::Separated);
            let path = geoparquet_filepath(expected_typ.clone().into(), file_type);
            let (wkt_arr, geo_arr) = read_gpq_file(path);

            assert_eq!(geo_arr.data_type(), expected_typ.clone().into());

            let wkt_geoms = wkt_arr
                .iter()
                .map(|x| x.transpose().unwrap())
                .collect::<Vec<_>>();
            let from_wkt = LineStringBuilder::from_nullable_geometries(&wkt_geoms, expected_typ)
                .unwrap()
                .finish();

            assert_eq!(geo_arr.as_line_string(), &from_wkt);
        }
    }
}

#[test]
fn polygon() {
    for dim in [
        Dimension::XY,
        Dimension::XYZ,
        Dimension::XYM,
        Dimension::XYZM,
    ] {
        for file_type in ["geo", "native"] {
            let expected_typ =
                PolygonType::new(dim, Default::default()).with_coord_type(CoordType::Separated);
            let path = geoparquet_filepath(expected_typ.clone().into(), file_type);
            let (wkt_arr, geo_arr) = read_gpq_file(path);

            assert_eq!(geo_arr.data_type(), expected_typ.clone().into());

            let wkt_geoms = wkt_arr
                .iter()
                .map(|x| x.transpose().unwrap())
                .collect::<Vec<_>>();
            let from_wkt = PolygonBuilder::from_nullable_geometries(&wkt_geoms, expected_typ)
                .unwrap()
                .finish();

            assert_eq!(geo_arr.as_polygon(), &from_wkt);
        }
    }
}

#[test]
fn multipoint() {
    for dim in [
        Dimension::XY,
        Dimension::XYZ,
        Dimension::XYM,
        Dimension::XYZM,
    ] {
        for file_type in ["geo", "native"] {
            let expected_typ =
                MultiPointType::new(dim, Default::default()).with_coord_type(CoordType::Separated);
            let path = geoparquet_filepath(expected_typ.clone().into(), file_type);
            let (wkt_arr, geo_arr) = read_gpq_file(path);

            assert_eq!(geo_arr.data_type(), expected_typ.clone().into());

            let wkt_geoms = wkt_arr
                .iter()
                .map(|x| x.transpose().unwrap())
                .collect::<Vec<_>>();
            let from_wkt = MultiPointBuilder::from_nullable_geometries(&wkt_geoms, expected_typ)
                .unwrap()
                .finish();

            assert_eq!(geo_arr.as_multi_point(), &from_wkt);
        }
    }
}

#[test]
fn multilinestring() {
    for dim in [
        Dimension::XY,
        Dimension::XYZ,
        Dimension::XYM,
        Dimension::XYZM,
    ] {
        for file_type in ["geo", "native"] {
            let expected_typ = MultiLineStringType::new(dim, Default::default())
                .with_coord_type(CoordType::Separated);
            let path = geoparquet_filepath(expected_typ.clone().into(), file_type);
            let (wkt_arr, geo_arr) = read_gpq_file(path);

            assert_eq!(geo_arr.data_type(), expected_typ.clone().into());

            let wkt_geoms = wkt_arr
                .iter()
                .map(|x| x.transpose().unwrap())
                .collect::<Vec<_>>();
            let from_wkt =
                MultiLineStringBuilder::from_nullable_geometries(&wkt_geoms, expected_typ)
                    .unwrap()
                    .finish();

            assert_eq!(geo_arr.as_multi_line_string(), &from_wkt);
        }
    }
}

#[test]
fn multipolygon() {
    for dim in [
        Dimension::XY,
        Dimension::XYZ,
        Dimension::XYM,
        Dimension::XYZM,
    ] {
        for file_type in ["geo", "native"] {
            let expected_typ = MultiPolygonType::new(dim, Default::default())
                .with_coord_type(CoordType::Separated);
            let path = geoparquet_filepath(expected_typ.clone().into(), file_type);
            let (wkt_arr, geo_arr) = read_gpq_file(path);

            assert_eq!(geo_arr.data_type(), expected_typ.clone().into());

            let wkt_geoms = wkt_arr
                .iter()
                .map(|x| x.transpose().unwrap())
                .collect::<Vec<_>>();
            let from_wkt = MultiPolygonBuilder::from_nullable_geometries(&wkt_geoms, expected_typ)
                .unwrap()
                .finish();

            assert_eq!(geo_arr.as_multi_polygon(), &from_wkt);
        }
    }
}

#[test]
fn geometrycollection() {
    // Note: there is no native encoding for geometry collection; just WKB
    for dim in [
        Dimension::XY,
        Dimension::XYZ,
        Dimension::XYM,
        Dimension::XYZM,
    ] {
        let expected_typ = GeometryCollectionType::new(dim, Default::default())
            .with_coord_type(CoordType::Separated);
        let path = geoparquet_filepath(expected_typ.clone().into(), "geo");
        let (wkt_arr, geo_arr) = read_gpq_file(path);

        assert_eq!(geo_arr.data_type(), expected_typ.clone().into());

        let wkt_geoms = wkt_arr
            .iter()
            .map(|x| x.transpose().unwrap())
            .collect::<Vec<_>>();
        // NOTE: this hard-coding of `prefer_multi` to `true` matches some hard-coding somewhere in
        // the Parquet reader. Ideally we'd find a way to expose this.
        let from_wkt =
            GeometryCollectionBuilder::from_nullable_geometries(&wkt_geoms, expected_typ)
                .unwrap()
                .finish();

        assert_eq!(geo_arr.as_geometry_collection(), &from_wkt);
    }
}
