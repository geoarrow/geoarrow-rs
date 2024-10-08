## Pyodide


Install rust nightly and add wasm toolchain

```
rustup toolchain install nightly
rustup target add --toolchain nightly wasm32-unknown-emscripten
```

Install dependencies. You need to set the `pyodide-build` version to the same version as the `pyodide` release you distribute for.

```
pip install -U maturin
pip install pyodide-build
```

Install emsdk.

```
git clone https://github.com/emscripten-core/emsdk.git
cd emsdk
PYODIDE_EMSCRIPTEN_VERSION=$(pyodide config get emscripten_version)
./emsdk install ${PYODIDE_EMSCRIPTEN_VERSION}
./emsdk activate ${PYODIDE_EMSCRIPTEN_VERSION}
source emsdk_env.sh
cd ..
```

- The `RUSTFLAGS` is temporary to get around this compiler bug.
- You must use rust nightly
- You must use `--no-default-features` to remove any async support. `tokio` does not compile for emscripten.

```bash
RUSTFLAGS='-Zinline-mir=no' /
    RUSTUP_TOOLCHAIN=nightly /
    maturin build /
    --no-default-features /
    --release /
    -o dist /
    --target wasm32-unknown-emscripten
```
