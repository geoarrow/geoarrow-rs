from geodatafusion import register_all

from datafusion import SessionContext


def test_simple():
    ctx = SessionContext()
    register_all(ctx)

    sql = "SELECT ST_AsText(ST_GeomFromText('POINT(1 2)'));"
    df = ctx.sql(sql)
    assert df.to_arrow_table().columns[0][0].as_py() == "POINT(1 2)"
