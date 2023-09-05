use std::fs::File;

use arrow2::error::Result;
use arrow2::io::parquet::read;
use criterion::{criterion_group, criterion_main, Criterion};
use geoarrow2::array::{MultiPolygonArray, WKBArray};
use geoarrow2::geo_traits::{CoordTrait, LineStringTrait, MultiPolygonTrait, PolygonTrait};
use geoarrow2::scalar::WKB;
use geoarrow2::trait_::GeometryScalarTrait;

fn load_parquet() -> Result<WKBArray<i32>> {
    let mut file = File::open("fixtures/geoparquet/nz-building-outlines-geometry.parquet")?;

    let metadata = read::read_metadata(&mut file)?;

    let schema = read::infer_schema(&metadata)?;

    let schema = schema.filter(|_index, field| field.name == "geometry");

    let chunks = read::FileReader::new(file, metadata.row_groups, schema, None, None, None);

    let mut wkb_arrays = vec![];
    for maybe_chunk in chunks {
        let chunk = maybe_chunk?;
        assert_eq!(chunk.columns().len(), 1);
        let column = &chunk.columns()[0];
        let wkb_arr: WKBArray<i32> = column.as_ref().try_into().unwrap();
        wkb_arrays.push(wkb_arr);
    }

    assert_eq!(wkb_arrays.len(), 1);

    Ok(wkb_arrays.get(0).unwrap().clone())
}

fn wkb_linear_ring_to_geo<'a>(
    wkb_linear_ring: impl LineStringTrait<'a, T = f64>,
) -> geo::LineString {
    let mut coords = Vec::with_capacity(wkb_linear_ring.num_coords());
    for coord_idx in 0..wkb_linear_ring.num_coords() {
        let wkb_coord = wkb_linear_ring.coord(coord_idx).unwrap();
        let coord = geo::Coord {
            x: wkb_coord.x(),
            y: wkb_coord.y(),
        };
        coords.push(coord)
    }
    geo::LineString::new(coords)
}

fn wkb_polygon_to_geo<'a>(wkb_polygon: impl PolygonTrait<'a, T = f64>) -> geo::Polygon {
    let exterior = wkb_linear_ring_to_geo(wkb_polygon.exterior().unwrap());

    let mut interiors = Vec::with_capacity(wkb_polygon.num_interiors());
    for interior_idx in 0..wkb_polygon.num_interiors() {
        let interior = wkb_linear_ring_to_geo(wkb_polygon.interior(interior_idx).unwrap());
        interiors.push(interior);
    }

    geo::Polygon::new(exterior, interiors)
}

fn wkb_multi_polygon_to_geo<'a>(
    wkb_multi_polygon: impl MultiPolygonTrait<'a, T = f64>,
) -> geo::Geometry {
    let mut polygons = Vec::with_capacity(wkb_multi_polygon.num_polygons());
    for polygon_idx in 0..wkb_multi_polygon.num_polygons() {
        let polygon = wkb_polygon_to_geo(wkb_multi_polygon.polygon(polygon_idx).unwrap());
        polygons.push(polygon);
    }
    let multi_polygon = geo::MultiPolygon::new(polygons);
    geo::Geometry::MultiPolygon(multi_polygon)
}

fn parse_directly_to_geo(value: WKBArray<i32>) -> Vec<geo::Geometry> {
    let wkb_objects: Vec<WKB<'_, i32>> = value.values_iter().collect();
    wkb_objects
        .iter()
        .map(|wkb| wkb_multi_polygon_to_geo(wkb.to_wkb_object().into_multi_polygon()))
        .collect()
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let array = load_parquet().unwrap();

    c.bench_function("parse WKBArray to geoarrow MultiPolygonArray", |b| {
        b.iter(|| {
            let _values: MultiPolygonArray<i32> = array.clone().try_into().unwrap();
        })
    });
    c.bench_function(
        "parse WKBArray to geoarrow MultiPolygonArray then to Vec<geo::Geometry>",
        |b| {
            b.iter(|| {
                let array: MultiPolygonArray<i32> = array.clone().try_into().unwrap();
                let _out: Vec<geo::Geometry> = array
                    .iter_geo_values()
                    .map(geo::Geometry::MultiPolygon)
                    .collect();
            })
        },
    );
    c.bench_function("parse WKBArray directly to Vec<geo::Geometry>", |b| {
        b.iter(|| {
            let _out = parse_directly_to_geo(array.clone());
        })
    });
    c.bench_function("parse WKBArray to Vec<geo::Geometry>", |b| {
        b.iter(|| {
            // Note: As of Sept 2023, `to_geo` uses geozero. This could change in the future, in
            // which case, this bench would no longer be benching geozero.
            let _values: Vec<geo::Geometry> = array
                .clone()
                .values_iter()
                .map(|wkb| wkb.to_geo())
                .collect();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
