use std::fs::File;

use arrow::compute::concat;
use criterion::{criterion_group, criterion_main, Criterion};
use geoarrow::array::{MultiPolygonArray, WKBArray};
use geoarrow::trait_::ArrayAccessor;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

fn load_parquet() -> WKBArray<i32> {
    let file = File::open("fixtures/geoparquet/nz-building-outlines.parquet").expect("You need to download nz-building-outlines.parquet before running this benchmark, see fixtures/README.md for more info");

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let parquet_schema = builder.parquet_schema();
    let geometry_column_index = parquet_schema
        .columns()
        .iter()
        .position(|column| column.name() == "geometry")
        .unwrap();
    let projection = ProjectionMask::roots(parquet_schema, vec![geometry_column_index]);
    let reader = builder.with_projection(projection).build().unwrap();

    let mut arrays = vec![];
    for maybe_record_batch in reader {
        let record_batch = maybe_record_batch.unwrap();
        assert_eq!(record_batch.num_columns(), 1);
        let column = record_batch.column(0);
        arrays.push(column.clone());
    }
    let array_refs = arrays.iter().map(|arr| arr.as_ref()).collect::<Vec<_>>();
    let single_array = concat(array_refs.as_slice()).unwrap();
    single_array.as_ref().try_into().unwrap()
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let array = load_parquet();

    c.bench_function("parse WKBArray to geoarrow MultiPolygonArray", |b| {
        b.iter(|| {
            let _values: MultiPolygonArray<i32, 2> = array.clone().try_into().unwrap();
        })
    });
    c.bench_function(
        "parse WKBArray to geoarrow MultiPolygonArray then to Vec<geo::Geometry>",
        |b| {
            b.iter(|| {
                let array: MultiPolygonArray<i32, 2> = array.clone().try_into().unwrap();
                let _out: Vec<geo::Geometry> = array
                    .iter_geo_values()
                    .map(geo::Geometry::MultiPolygon)
                    .collect();
            })
        },
    );
    c.bench_function("parse WKBArray to Vec<geo::Geometry>", |b| {
        b.iter(|| {
            // Note: As of Sept 2023, `to_geo` uses geozero. This could change in the future, in
            // which case, this bench would no longer be benching geozero.
            let _values: Vec<geo::Geometry> = array.clone().iter_geo_values().collect();
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);
