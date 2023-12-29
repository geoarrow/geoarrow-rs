use std::fs::File;

use arrow_ipc::reader::FileReader;
use criterion::{criterion_group, criterion_main, Criterion};
use geoarrow::algorithm::geo::EuclideanDistance;
use geoarrow::array::{MultiPolygonArray, PointArray};
use geoarrow::trait_::GeometryArrayAccessor;

fn load_nybb() -> MultiPolygonArray<i32> {
    let file = File::open("fixtures/nybb.arrow").unwrap();
    let reader = FileReader::try_new(file, None).unwrap();

    let mut arrays = vec![];
    for maybe_record_batch in reader {
        let record_batch = maybe_record_batch.unwrap();
        let geom_idx = record_batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == "geometry")
            .unwrap();
        let arr = record_batch.column(geom_idx);
        let multi_poly_arr: MultiPolygonArray<i32> = arr.as_ref().try_into().unwrap();
        arrays.push(multi_poly_arr);
    }

    assert_eq!(arrays.len(), 1);
    arrays.first().unwrap().clone()
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
            let point_array = PointArray::from(vec![point].as_slice());

            let _distances = array.euclidean_distance(&point_array.value(0));
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
