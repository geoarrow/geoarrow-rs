# Development


To install versions of the package under active development, you need to have Rust installed, e.g. with rustup:

```
rustup update stable
```

clone the repo and navigate into it:

```
git clone https://github.com/geoarrow/geoarrow-rs
cd geoarrow-rs
```

Then enter into the `python` directory:

```
cd python
poetry install
poetry run maturin develop -m geoarrow-core/Cargo.toml
poetry run maturin develop -m geoarrow-io/Cargo.toml
```

## Documentation

Start docs locally:

```
poetry run mkdocs serve
```

Deploy docs (automatically):

Push a new tag with the format `py-v*`, such as `py-v0.1.0`.

Deploy docs (manually):

```
poetry run mike deploy VERSION_TAG --update-aliases --push --deploy-prefix python/
```

This only needs to be run **once ever**, to set the redirect from `https://geoarrow.github.io/geoarrow-rs/python/` to `https://geoarrow.github.io/geoarrow-rs/python/latest/`.

```
poetry run mike set-default latest --deploy-prefix python/ --push
```

## Emscripten Python wheels

Install rust nightly and add wasm toolchain

```bash
rustup toolchain install nightly
rustup target add --toolchain nightly wasm32-unknown-emscripten
```

Install maturin and pyodide-build

```bash
pip install -U maturin
pip install pyodide-build
```

Clone emsdk. I clone this into a specific path at `~/github/emscripten-core/emsdk` so that it can be shared across projects.

```bash
mkdir -p ~/github/emscripten-core/
git clone https://github.com/emscripten-core/emsdk.git ~/github/emscripten-core/emsdk
# Or, set this manually
PYODIDE_EMSCRIPTEN_VERSION=$(pyodide config get emscripten_version)
~/github/emscripten-core/emsdk/emsdk install ${PYODIDE_EMSCRIPTEN_VERSION}
~/github/emscripten-core/emsdk/emsdk activate ${PYODIDE_EMSCRIPTEN_VERSION}
source ~/github/emscripten-core/emsdk/emsdk_env.sh
```

Note that the addition of `RUSTFLAGS="-Zinline-mir=no"` is temporary due to https://github.com/rust-lang/rust/issues/128887.

Build `geoarrow-rust-core`:

```bash
RUSTFLAGS="-Zinline-mir=no" RUSTUP_TOOLCHAIN=nightly \
    maturin build \
    --release \
    --no-default-features \
    -o dist \
    -m geoarrow-core/Cargo.toml \
    --target wasm32-unknown-emscripten \
    -i python3.11
```

Build `geoarrow-rust-io`:

```bash
RUSTFLAGS="-Zinline-mir=no" RUSTUP_TOOLCHAIN=nightly \
    maturin build \
    --release \
    --no-default-features \
    -o dist \
    -m geoarrow-io/Cargo.toml \
    --target wasm32-unknown-emscripten \
    -i python3.11
```
