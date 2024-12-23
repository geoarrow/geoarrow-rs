use criterion::{criterion_group, criterion_main, Criterion};
use geoarrow::algorithm::geo::Area;
use geoarrow::array::{AsChunkedNativeArray, MultiPolygonArray};
use geoarrow::io::flatgeobuf::FlatGeobufReaderBuilder;
use geoarrow::table::Table;
use std::fs::File;

fn load_file() -> MultiPolygonArray {
    let file = File::open("fixtures/flatgeobuf/countries.fgb").unwrap();
    let reader_builder = FlatGeobufReaderBuilder::open(file).unwrap();
    let record_batch_reader = reader_builder.read(Default::default()).unwrap();
    let table =
        Table::try_from(Box::new(record_batch_reader) as Box<dyn arrow_array::RecordBatchReader>)
            .unwrap();

    table
        .geometry_column(None)
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
