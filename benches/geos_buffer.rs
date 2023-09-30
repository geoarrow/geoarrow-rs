use criterion::{criterion_group, criterion_main, Criterion};
use geoarrow2::algorithm::geos::buffer::Buffer;
use geoarrow2::array::{CoordBuffer, InterleavedCoordBuffer, PointArray};

fn generate_data() -> PointArray {
    let coords = vec![0.0; 100_000];
    let coord_buffer = CoordBuffer::Interleaved(InterleavedCoordBuffer::new(coords.into()));
    PointArray::new(coord_buffer, None)
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let point_array = generate_data();

    c.bench_function("buffer", |b| {
        b.iter(|| {
            let _buffered = point_array.buffer(1.0, 8).unwrap();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
