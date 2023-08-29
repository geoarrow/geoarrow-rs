# Create @geoarrow/geoparquet-wasm
ENV=$ENV FEATURES="--no-default-features --features debug --features parquet" NAME="@geoarrow/geoparquet-wasm" bash ./scripts/build.sh
# Create @geoarrow/flatgeobuf-wasm
ENV=$ENV FEATURES="--no-default-features --features debug --features flatgeobuf" NAME="@geoarrow/flatgeobuf-wasm" bash ./scripts/build.sh
