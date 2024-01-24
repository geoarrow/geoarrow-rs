# `geoarrow.rust.proj`

Python bindings to [PROJ](https://proj.org/en/9.3/) for coordinate reprojection on GeoArrow arrays.


Wheel notes

```
docker run --rm -it --platform linux/amd64 --entrypoint bash quay.io/pypa/manylinux2014_x86_64
```

```bash
cd home

git clone https://github.com/Microsoft/vcpkg.git
yum install perl-IPC-Cmd curl zip unzip tar llvm-toolset-7 -y
scl enable llvm-toolset-7 bash

./vcpkg/bootstrap-vcpkg.sh
./vcpkg/vcpkg install proj

# Install rust
curl --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --profile minimal --default-toolchain stable -y
source "$HOME/.cargo/env"
git clone https://github.com/geoarrow/geoarrow-rs
cd geoarrow-rs
git checkout kyle/python-proj-wheels2

export PATH="/opt/python/cp38-cp38/bin/:$PATH"
pip install -U pip
pip install maturin
cd python/proj

PKG_CONFIG_PATH="/home/vcpkg/installed/x64-linux/lib/pkgconfig/:$PKG_CONFIG_PATH" maturin build --interpreter /opt/python/cp38-cp38/bin/python --manylinux 2014 -o .
```

It looks like this might also be a static wheel by default.

Mac notes:

```
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/vcpkg install proj
> ls vcpkg/installed/arm64-osx/lib/
libcurl.a      liblzma.a      libsqlite3.a   libturbojpeg.a pkgconfig
libjpeg.a      libproj.a      libtiff.a      libz.a
```

```
PKG_CONFIG_PATH="$(pwd)/vcpkg/installed/arm64-osx/lib/pkgconfig/:$PKG_CONFIG_PATH" maturin build -o .
```

that _automatically_ builds a static wheel!

```
> delocate-listdeps ./geoarrow_rust_proj-0.1.0-cp38-abi3-macosx_11_0_arm64.whl
> unzip -l geoarrow_rust_proj-0.1.0-cp38-abi3-macosx_11_0_arm64.whl
Archive:  geoarrow_rust_proj-0.1.0-cp38-abi3-macosx_11_0_arm64.whl
  Length      Date    Time    Name
---------  ---------- -----   ----
     1371  01-23-2024 05:29   geoarrow_rust_proj-0.1.0.dist-info/METADATA
      102  01-23-2024 05:29   geoarrow_rust_proj-0.1.0.dist-info/WHEEL
       26  01-23-2024 05:29   geoarrow/rust/proj/__init__.py
        0  01-23-2024 05:29   geoarrow/rust/proj/py.typed
  6904480  01-23-2024 05:29   geoarrow/rust/proj/_rust_proj.abi3.so
      503  01-23-2024 05:29   geoarrow_rust_proj-0.1.0.dist-info/RECORD
---------                     -------
  6906482                     6 files
```

(in contrast, when not setting `PKG_CONFIG_PATH`, the rust proj size is under 1MB and links into homebrew.)

**Windows:**

Previously an issue was in just installing pkg-config. Can we now use pixi to manage pkg-config on windows?
