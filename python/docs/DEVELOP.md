# Develop Docs

## Environment installation

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
