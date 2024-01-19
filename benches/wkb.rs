use std::fs::File;

use criterion::{criterion_group, criterion_main, Criterion};
use geoarrow::array::{MultiPolygonArray, WKBArray};
use geoarrow::trait_::GeometryArrayAccessor;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn load_parquet() -> WKBArray<i32> {
    let file = File::open("fixtures/geoparquet/nz-building-outlines-geometry.parquet").unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let reader = builder.build().unwrap();

    let mut wkb_arrays = vec![];
    for maybe_record_batch in reader {
        let record_batch = maybe_record_batch.unwrap();
        assert_eq!(record_batch.num_columns(), 1);
        let column = record_batch.column(0);
        let wkb_arr: WKBArray<i32> = column.as_ref().try_into().unwrap();
        wkb_arrays.push(wkb_arr);
    }

    assert_eq!(wkb_arrays.len(), 1);

    wkb_arrays.first().unwrap().clone()
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let array = load_parquet();

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
    c.bench_function("parse WKBArray to Vec<geo::Geometry>", |b| {
        b.iter(|| {
            // Note: As of Sept 2023, `to_geo` uses geozero. This could change in the future, in
            // which case, this bench would no longer be benching geozero.
            let _values: Vec<geo::Geometry> = array.clone().iter_geo_values().collect();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
