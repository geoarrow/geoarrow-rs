# Build support

geoarrow-rs depends on a number of optional C dependencies, such as GEOS, PROJ, and GDAL. To simplify these dependencies, we use [Pixi](https://github.com/prefix-dev/pixi), a package management system for native dependencies with an easy-to-use lockfile.

```
cargo install pixi
pixi install
pixi shell
```

You can also update your local environment variables by running this from the repo root.

```bash
export GDAL_HOME="$(pwd)/build/.pixi/envs/default"
export LD_LIBRARY_PATH="$(pwd)/build/.pixi/envs/default/lib:$LD_LIBRARY_PATH"
export GEOS_LIB_DIR="$(pwd)/build/.pixi/envs/default/lib:$GEOS_LIB_DIR"
export GEOS_VERSION=3.12.1
export PKG_CONFIG_PATH="$(pwd)/build/.pixi/envs/default/lib/pkgconfig:$PKG_CONFIG_PATH"
```
