# `geodatafusion`

Python bindings for `geodatafusion`, providing geospatial UDFs for the
`datafusion` Python package.

### Usage

To use, register the User-Defined Functions (UDFs) provided by `geodatafusion` on your `SessionContext`. The easiest way to do this is via `geodatafusion.register_all`.

```py
from datafusion import SessionContext
from geodatafusion import register_all

ctx = SessionContext()
register_all(ctx)
```

Then you can use the UDFs in SQL queries:

```py
sql = "SELECT ST_X(ST_GeomFromText('POINT(1 2)'));"
df = ctx.sql(sql)
df.show()
```

prints:
```
+-------------------------------------------+
| st_x(st_geomfromtext(Utf8("POINT(1 2)"))) |
+-------------------------------------------+
| 1.0                                       |
+-------------------------------------------+
```
