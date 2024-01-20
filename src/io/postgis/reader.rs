//! This is partially derived from <https://github.com/alttch/myval> under the Apache 2 license

use arrow_schema::{DataType, Field, SchemaBuilder};
use geozero::wkb::process_ewkb_geom;
use geozero::{ColumnValue, FeatureProcessor, GeomProcessor, GeozeroGeometry, PropertyProcessor};
// use chrono::{DateTime, NaiveDateTime, Utc};
use futures::stream::TryStreamExt;
use sqlx::postgres::{PgRow, PgTypeInfo};
use sqlx::{Column, Decode, Executor, Postgres, Row, Type, TypeInfo};
use std::io::Cursor;
use std::sync::Arc;

use crate::error::{GeoArrowError, Result};
use crate::io::geozero::array::MixedGeometryStreamBuilder;
use crate::io::geozero::table::{GeoTableBuilder, GeoTableBuilderOptions};
use crate::table::GeoTable;
use crate::trait_::GeometryArrayBuilder;

/// A wrapper for an EWKB-encoded postgis geometry
pub struct PostgisEWKBGeometry<'a>(&'a [u8]);

impl<'a, 'r: 'a> Decode<'r, Postgres> for PostgisEWKBGeometry<'a> {
    fn decode(
        value: <Postgres as sqlx::database::HasValueRef<'r>>::ValueRef,
    ) -> std::prelude::v1::Result<Self, sqlx::error::BoxDynError> {
        Ok(Self(value.as_bytes()?))
    }
}

impl<'a> Type<Postgres> for PostgisEWKBGeometry<'a> {
    fn type_info() -> <Postgres as sqlx::Database>::TypeInfo {
        PgTypeInfo::with_name("geometry")
    }
}

impl<'a> GeozeroGeometry for PostgisEWKBGeometry<'a> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_ewkb_geom(&mut Cursor::new(self.0), processor)
    }
}

// TODO: right now this uses a hashmap with names. In the future, it should switch to using a
// positional schema.
// TODO: manage buffering
impl<G: GeometryArrayBuilder + GeomProcessor> GeoTableBuilder<G> {
    fn add_postgres_geometry(&mut self, value: PostgisEWKBGeometry) -> Result<()> {
        self.geometry_begin()?;
        value.process_geom(self)?;
        self.geometry_end()?;
        Ok(())
    }

    fn add_postgres_row(&mut self, row_idx: u64, row: &PgRow) -> Result<()> {
        self.feature_begin(row_idx)?;
        self.properties_begin()?;
        let mut geometry: Option<PostgisEWKBGeometry> = None;
        for (i, column) in row.columns().iter().enumerate() {
            let column_name = column.name();
            let upstream_type_info = column.type_info();
            if let Some(our_type_info) =
                super::type_info::PgTypeInfo::from_upstream(upstream_type_info)
            {
                use super::type_info::PgType::*;
                let column_value = match our_type_info.0 {
                    Bool => ColumnValue::Bool(row.try_get(i)?),
                    Bytea | Bit => ColumnValue::Binary(row.try_get(i)?),
                    Int2 => ColumnValue::Short(row.try_get(i)?),
                    Int4 => ColumnValue::Int(row.try_get(i)?),
                    Int8 => ColumnValue::Long(row.try_get(i)?),
                    Float4 => ColumnValue::Float(row.try_get(i)?),
                    Float8 => ColumnValue::Double(row.try_get(i)?),
                    Text | Varchar | Char | Json | Jsonb => ColumnValue::String(row.try_get(i)?),
                    v => todo!("unimplemented type in column value: {}", v.display_name()),
                };
                self.property(i, column_name, &column_value)?;
            } else {
                match upstream_type_info.name() {
                    "geometry" | "geography" => {
                        geometry = Some(row.try_get(i)?);
                    }
                    other => {
                        return Err(GeoArrowError::General(format!(
                            "unknown non-standard type: {}",
                            other
                        )))
                    }
                }
            };
        }
        self.properties_end()?;
        // Add geometry after we've finished writing properties
        self.add_postgres_geometry(geometry.expect("missing geometry for row {}"))?;
        self.feature_end(row_idx)?;
        Ok(())
    }

    fn initialize_from_row(row: &PgRow, mut options: GeoTableBuilderOptions) -> Result<Self> {
        let mut schema = SchemaBuilder::new();
        for column in row.columns() {
            let column_name = column.name();
            let upstream_type_info = column.type_info();
            if let Some(our_type_info) =
                super::type_info::PgTypeInfo::from_upstream(upstream_type_info)
            {
                use super::type_info::PgType::*;
                let data_type = match our_type_info.0 {
                    Bool => DataType::Boolean,
                    Bytea | Bit => DataType::Binary,
                    Int2 => DataType::Int16,
                    Int4 => DataType::Int32,
                    Int8 => DataType::Int64,
                    Float4 => DataType::Float32,
                    Float8 => DataType::Float64,
                    Text | Varchar | Char | Json | Jsonb => DataType::Utf8,
                    v => todo!("unimplemented type in initialization: {}", v.display_name()),
                };
                schema.push(Field::new(column_name, data_type, true))
            } else {
                match upstream_type_info.name() {
                    // We only want to initialize the schema fields for attributes
                    "geometry" | "geography" => {
                        continue;
                    }
                    other => {
                        return Err(GeoArrowError::General(format!(
                            "unknown non-standard type in initialization: {}",
                            other
                        )))
                    }
                }
            };
        }
        options.properties_schema = Some(Arc::new(schema.finish()));

        // Create builder and add this row
        let mut builder = Self::new_with_options(options);
        builder.add_postgres_row(0, row)?;
        Ok(builder)
    }
}

pub async fn read_postgis<'c, E: Executor<'c, Database = Postgres>>(
    executor: E,
    sql: &str,
) -> Result<Option<GeoTable>> {
    let query = sqlx::query::<Postgres>(sql);
    let mut result_stream = query.fetch(executor);
    let mut table_builder: Option<GeoTableBuilder<MixedGeometryStreamBuilder<i32>>> = None;

    // TODO: try out chunking with `result_stream.try_chunks`
    let mut row_idx = 0;
    while let Some(row) = result_stream.try_next().await? {
        if let Some(ref mut table_builder) = table_builder {
            // Add this row
            table_builder.add_postgres_row(row_idx, &row)?;
        } else {
            // Initialize table builder
            let table_builder_options = GeoTableBuilderOptions::default();
            table_builder = Some(GeoTableBuilder::initialize_from_row(
                &row,
                table_builder_options,
            )?)
        };
        row_idx += 1;
    }

    if let Some(table_builder) = table_builder {
        Ok(Some(table_builder.finish()?))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    #[ignore = "don't test postgres on ci"]
    #[tokio::test]
    async fn test() {
        dbg!("testing!");
        let connection_url = "postgresql://username:password@localhost:54321/postgis";
        let pool = PgPoolOptions::new().connect(connection_url).await.unwrap();
        let sql = "SELECT * FROM sample1;";
        let _table = read_postgis(&pool, sql).await.unwrap();
    }
}
