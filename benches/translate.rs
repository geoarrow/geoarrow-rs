use geo::polygon;

use criterion::{criterion_group, criterion_main, Criterion};
use geoarrow::algorithm::geo::Translate;
use geoarrow::array::PolygonArray;

fn create_data() -> PolygonArray<i32> {
    // An L shape
    // https://github.com/georust/geo/blob/7cb7d0ffa6bf1544c5ca9922bd06100c36f815d7/README.md?plain=1#L40
    let poly = polygon![
        (x: 0.0, y: 0.0),
        (x: 4.0, y: 0.0),
        (x: 4.0, y: 1.0),
        (x: 1.0, y: 1.0),
        (x: 1.0, y: 4.0),
        (x: 0.0, y: 4.0),
        (x: 0.0, y: 0.0),
    ];
    let v = vec![poly; 1000];
    v.as_slice().into()
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let data = create_data();

    c.bench_function("translate PolygonArray", |b| {
        b.iter(|| {
            let _ = data.translate(10.0.into(), 20.0.into());
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
