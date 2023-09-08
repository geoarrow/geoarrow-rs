use std::fs::File;

use arrow2::error::Result;
use arrow2::io::parquet::read;
use criterion::{criterion_group, criterion_main, Criterion};
use geoarrow2::array::{MultiPolygonArray, WKBArray};
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
