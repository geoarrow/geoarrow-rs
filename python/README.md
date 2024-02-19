# `geoarrow.rust`: Python bindings to `geoarrow-rs`

This folder contains Python bindings to the [GeoArrow Rust implementation](https://github.com/geoarrow/geoarrow-rs).

`geoarrow.rust` is distributed with [namespace packaging](https://packaging.python.org/en/latest/guides/packaging-namespace-packages/), meaning that each python package `geoarrow-rust-[submodule-name]` (imported as `geoarrow.rust.[submodule-name]`) can be published to PyPI independently. The benefit of this approach is that complex C dependencies can be built and packaged independently.

Modules so far:

- [`core`](./core/README.md): All algorithms and data structures implemented in pure Rust without any C dependencies. Having a pure Rust dependency tree means it's trivial to build binary wheels for many operating system architectures that might not be possible with C dependencies.

Future modules:

- `geos`: [GEOS](https://libgeos.org/)-based algorithms on GeoArrow memory.
- `proj`: [PROJ](https://proj.org/en/9.3/)-based coordinate reprojection on GeoArrow memory.

In order to obtain relevant modules, you should install them from PyPI directly, e.g.:

```
pip install geoarrow-rust-core
```

See [DEVELOP.md](docs/DEVELOP.md) in the `docs` folder for more information on developing and building the Python bindings.
