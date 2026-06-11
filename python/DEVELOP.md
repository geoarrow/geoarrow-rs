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
uv install
uv run maturin develop -m geoarrow-core/Cargo.toml
uv run maturin develop -m geoarrow-compute/Cargo.toml
uv run maturin develop -m geoarrow-io/Cargo.toml
```

## Documentation

Start docs locally:

```
uv run mkdocs serve
```

Deploy docs (automatically):

Push a new tag with the format `py-v*`, such as `py-v0.1.0`.

Deploy docs (manually):

```
uv run mike deploy VERSION_TAG --update-aliases --push --deploy-prefix python/
```

This only needs to be run **once ever**, to set the redirect from `https://geoarrow.org/geoarrow-rs/python/` to `https://geoarrow.org/geoarrow-rs/python/latest/`.

```
uv run mike set-default latest --deploy-prefix python/ --push
```

## Emscripten Python wheels

Emscripten wheels (PEP 783, for Pyodide) are currently built only for
`geoarrow-rust-core`, once per Python version. The entire toolchain config
(Rust toolchain, Emscripten version, ABI tag, rustflags) is defined by
`pyodide-build` *running under that same Python version* — e.g. Python 3.13
maps to ABI `2025_0`/Emscripten 4.0.9 while Python 3.14 maps to ABI
`2026_0`/Emscripten 5.0.3. Use `uvx -p` to query the config for a given
Python version without touching the project venv:

```bash
PYTHON_VERSION=3.14  # or 3.13
# The `pyodide` executable lives in pyodide-cli; most subcommands (config,
# xbuildenv) are plugins provided by pyodide-build, so both packages are
# needed.
pyodide_cmd() {
    uvx -p "$PYTHON_VERSION" --from pyodide-cli --with pyodide-build pyodide "$@"
}
RUST_TOOLCHAIN=$(pyodide_cmd config get rust_toolchain)
PYODIDE_ABI_VERSION=$(pyodide_cmd config get pyodide_abi_version)
PYODIDE_RUSTFLAGS=$(pyodide_cmd config get rustflags)
PYODIDE_CFLAGS=$(pyodide_cmd config get cflags)

echo "RUST_TOOLCHAIN:     $RUST_TOOLCHAIN"
echo "PYODIDE_ABI_VERSION: $PYODIDE_ABI_VERSION"
echo "PYODIDE_RUSTFLAGS:  $PYODIDE_RUSTFLAGS"
echo "PYODIDE_CFLAGS:     $PYODIDE_CFLAGS"
```

Install the matching Rust toolchain and wasm target:

```bash
rustup toolchain install $RUST_TOOLCHAIN
rustup target add --toolchain $RUST_TOOLCHAIN wasm32-unknown-emscripten
```

Install Emscripten via the Pyodide cross-build environment rather than a
stock emsdk. This pins the Emscripten version matching the target Pyodide ABI
automatically, and applies [Pyodide's patches to
Emscripten](https://github.com/pyodide/pyodide/tree/main/emsdk/patches) —
several of which affect dynamic linking of Rust side modules:

```bash
export PYODIDE_XBUILDENV_PATH="$HOME/.cache/pyodide-xbuildenv"
pyodide_cmd xbuildenv install
pyodide_cmd xbuildenv install-emscripten
source "$PYODIDE_XBUILDENV_PATH/$(pyodide_cmd xbuildenv version)/emsdk/emsdk_env.sh"
```

Build the wheel. Notes on the environment variables:

- `MATURIN_PYEMSCRIPTEN_PLATFORM_VERSION` is required for the wheel to get the
  PyPI-accepted `pyemscripten_*` platform tag instead of the legacy
  `emscripten_x_y_z` tag PyPI rejects (this also needs a recent maturin, hence
  `uvx maturin` rather than the project venv's maturin).
- `CFLAGS_wasm32_unknown_emscripten` is needed for crates that compile C code:
  Pyodide's cflags include `-fPIC`, without which the C objects can't be
  linked into a `SIDE_MODULE`.
- Always build with `--release`: debug builds are ~10x larger (full DWARF) and
  slow.

```bash
RUSTUP_TOOLCHAIN=$RUST_TOOLCHAIN \
CARGO_TARGET_WASM32_UNKNOWN_EMSCRIPTEN_RUSTFLAGS="$PYODIDE_RUSTFLAGS" \
CFLAGS_wasm32_unknown_emscripten="$PYODIDE_CFLAGS" \
MATURIN_PYEMSCRIPTEN_PLATFORM_VERSION=$PYODIDE_ABI_VERSION \
    uvx maturin build \
    --release \
    -o dist \
    -m geoarrow-core/Cargo.toml \
    --target wasm32-unknown-emscripten \
    -i python$PYTHON_VERSION
```
