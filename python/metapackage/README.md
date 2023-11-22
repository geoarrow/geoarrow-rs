# `geoarrow-rust`

This is a metapackage for the `geoarrow.rust` namespace.

Both the `geoarrow` and `geoarrow-rust` Python libraries are distributed with [namespace packaging](https://packaging.python.org/en/latest/guides/packaging-namespace-packages/), meaning that each python package `geoarrow-[submodule-name]` and `geoarrow-rust-[submodule-name]` (imported as `geoarrow.[submodule-name]` or `geoarrow.rust.[submodule-name]`) can be published to PyPI independently.

In order to obtain relevant modules, you should install them from PyPI directly, e.g.:

```
pip install geoarrow-rust-core
```
