# Develop Docs


## Development

To install versions of the package under active development, you need to have Rust and `maturin` installed, e.g. with:

```
rustup update stable
pip install maturin
```

clone the repo and navigate into it:

```
git clone https://github.com/geoarrow/geoarrow-rs
cd geoarrow-rs
```

From there navigate to the `python/core` (or other package) directory and develop with `maturin` (add the `--release` flag to the final command to build in release mode if you're benchmarking):

```
cd python/core  
virtualenv env  
source ./env/bin/activate  
pip install -U maturin  
maturin develop
```

You can also install packages locally with `pip`:

```
pip install .
```


## Environment installation for docs

Install Python dependencies, e.g. mkdocs et al.

```
poetry install
```

Install current

```
poetry run maturin develop -m ../core/Cargo.toml
```

## Docs

View docs:

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
