# `geoarrow.rust.proj`

Python bindings to [PROJ](https://proj.org/en/9.3/) for coordinate reprojection on GeoArrow arrays.


Wheel notes

```
docker run --rm -it --platform linux/amd64 --entrypoint bash quay.io/pypa/manylinux2014_x86_64
```

```bash
git clone https://github.com/Microsoft/vcpkg.git
yum install perl-IPC-Cmd curl zip unzip tar -y
./vcpkg/bootstrap-vcpkg.sh
./vcpkg/vcpkg install proj

# Install rust
curl --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --profile minimal --default-toolchain stable -y

```

Mac notes:

```
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/vcpkg install proj
> ls vcpkg/installed/arm64-osx/lib/
libcurl.a      liblzma.a      libsqlite3.a   libturbojpeg.a pkgconfig
libjpeg.a      libproj.a      libtiff.a      libz.a
```
