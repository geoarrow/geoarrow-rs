use std::fs::File;

use arrow2::error::Result;
use arrow2::io::ipc::read;
use criterion::{criterion_group, criterion_main, Criterion};
use geoarrow2::algorithm::geo::EuclideanDistance;
use geoarrow2::array::{MultiPolygonArray, PointArray};
use geoarrow2::{self, GeometryArrayTrait};

fn load_nybb() -> MultiPolygonArray<i32> {
    let mut file = File::open("fixtures/nybb.arrow").unwrap();

    let metadata = read::read_file_metadata(&mut file).unwrap();

    let schema = metadata.schema.clone();
    let geom_idx = schema
        .fields
        .iter()
        .position(|field| field.name == "geometry")
        .unwrap();

    let reader = read::FileReader::new(file, metadata, Some(vec![geom_idx]), None);

    let chunks = reader.collect::<Result<Vec<_>>>().unwrap();

    assert_eq!(chunks.len(), 1);

    let chunk = &chunks[0];

    let arrays = chunk.arrays();
    assert_eq!(arrays.len(), 1);

    let array = &arrays[0];

    array.as_ref().try_into().unwrap()
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let array = load_nybb();

    c.bench_function("to geo", |b| {
        b.iter(|| {
            let _values: Vec<_> = array.iter_geo_values().collect();
        })
    });
    c.bench_function("euclidean distance to scalar point", |b| {
        b.iter(|| {
            let point = geo::Point::new(0.0f64, 0.0f64);
            let point_array = PointArray::from(vec![point]);

            let _distances = array.euclidean_distance(&point_array.value(0));
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
