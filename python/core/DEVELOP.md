# Develop Docs

## Docs

View docs:

```
poetry run mkdocs serve
```

Deploy docs (automatically):

Push a new tag with the format `py-core-v*`, such as `py-core-v0.1.0`.

Deploy docs (manually):

```
poetry run mike deploy VERSION_TAG --update-aliases --push --deploy-prefix python/core/
```

This only needs to be run once ever, to set the redirect from `https://geoarrow.github.io/geoarrow-rs/python/core/` to `https://geoarrow.github.io/geoarrow-rs/python/core/latest/`.

```
poetry run mike set-default latest --deploy-prefix python/core/ --push
```
