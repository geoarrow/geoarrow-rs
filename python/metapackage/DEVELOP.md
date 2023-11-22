## Publishing

Install dependencies:

```
pip install -U build twine
```

then build the package:

```
python -m build
```

then upload to pypi:

```
python -m twine upload dist/*
```
