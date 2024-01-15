# Developer documentation

## Compile to Pyodide

Add wasm emscripten target.

```
rustup update
rustup toolchain install nightly
rustup target add --toolchain nightly wasm32-unknown-emscripten
```

Find emscripten version that the latest pyodide is built against.

```
virtualenv emsdk_env
source ./emsdk_env/bin/activate
pip install -U pyodide-build
PYODIDE_EMSCRIPTEN_VERSION=$(./emsdk_env/bin/pyodide config get emscripten_version)
echo $PYODIDE_EMSCRIPTEN_VERSION
deactivate
```

```
pip install -U pyodide-build
```

```
git clone https://github.com/emscripten-core/emsdk # tested and working at commit hash 961e66c
cd emsdk
./emsdk install $PYODIDE_EMSCRIPTEN_VERSION
./emsdk activate $PYODIDE_EMSCRIPTEN_VERSION
source ./emsdk_env.sh
cd ../
```

```
RUSTUP_TOOLCHAIN=nightly maturin build --release -o dist --target wasm32-unknown-emscripten -i python3.11
```
