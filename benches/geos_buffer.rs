use criterion::{criterion_group, criterion_main, Criterion};
use geoarrow::algorithm::geos::Buffer;
use geoarrow::array::{CoordBuffer, InterleavedCoordBuffer, PointArray, PolygonArray};

fn generate_data() -> PointArray {
    let coords = vec![0.0; 100_000];
    let coord_buffer = CoordBuffer::Interleaved(InterleavedCoordBuffer::new(coords.into()));
    PointArray::new(coord_buffer, None, Default::default())
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let point_array = generate_data();

    c.bench_function("buffer", |b| {
        b.iter(|| {
            let _buffered: PolygonArray<i32> = point_array.buffer(1.0, 8).unwrap();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
