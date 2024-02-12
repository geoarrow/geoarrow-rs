use criterion::{criterion_group, criterion_main, Criterion};
use geoarrow::algorithm::geo::Area;
use geoarrow::array::{AsChunkedGeometryArray, MultiPolygonArray};
use geoarrow::io::flatgeobuf::read_flatgeobuf;
use std::fs::File;

fn load_file() -> MultiPolygonArray<i32> {
    let mut file = File::open("fixtures/flatgeobuf/countries.fgb").unwrap();
    let table = read_flatgeobuf(&mut file, Default::default(), Some(9999999)).unwrap();
    table
        .geometry()
        .unwrap()
        .as_ref()
        .as_multi_polygon()
        .chunks()
        .first()
        .unwrap()
        .clone()
}

fn criterion_benchmark(c: &mut Criterion) {
    let data = load_file();

    c.bench_function("area", |bencher| {
        bencher.iter(|| {
            criterion::black_box(criterion::black_box(&data).signed_area());
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
