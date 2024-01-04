//! Abstractions for Arrow tables. Useful for dataset IO where data will have geometries and
//! attributes.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::array::*;
use crate::chunked_array::chunked_array::ChunkedGeometryArrayTrait;
use crate::chunked_array::ChunkedGeometryArray;
use crate::datatypes::GeoDataType;
use crate::error::Result;

#[derive(Debug, PartialEq, Clone)]
pub struct GeoTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    geometry_column_index: usize,
}

impl GeoTable {
    pub fn try_new(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        geometry_column_index: usize,
    ) -> Result<Self> {
        // TODO: validate
        Ok(Self {
            schema,
            batches,
            geometry_column_index,
        })
    }

    pub fn len(&self) -> usize {
        self.batches.iter().fold(0, |sum, val| sum + val.num_rows())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_inner(self) -> (SchemaRef, Vec<RecordBatch>, usize) {
        (self.schema, self.batches, self.geometry_column_index)
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn batches(&self) -> &Vec<RecordBatch> {
        &self.batches
    }

    pub fn geometry_column_index(&self) -> usize {
        self.geometry_column_index
    }

    pub fn geometry_data_type(&self) -> Result<GeoDataType> {
        Ok(*self.geometry()?.data_type())
    }

    /// Access the geometry column of the table
    pub fn geometry(&self) -> Result<Arc<dyn ChunkedGeometryArrayTrait>> {
        let field = self.schema.field(self.geometry_column_index);
        let array_refs = self
            .batches
            .iter()
            .map(|batch| batch.column(self.geometry_column_index))
            .collect::<Vec<_>>();
        let geo_data_type = GeoDataType::try_from(field)?;
        match geo_data_type {
            GeoDataType::Point(_) => {
                let chunks: Result<Vec<PointArray>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LineString(_) => {
                let chunks: Result<Vec<LineStringArray<i32>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LargeLineString(_) => {
                let chunks: Result<Vec<LineStringArray<i64>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::Polygon(_) => {
                let chunks: Result<Vec<PolygonArray<i32>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LargePolygon(_) => {
                let chunks: Result<Vec<PolygonArray<i64>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::MultiPoint(_) => {
                let chunks: Result<Vec<MultiPointArray<i32>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LargeMultiPoint(_) => {
                let chunks: Result<Vec<MultiPointArray<i64>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::MultiLineString(_) => {
                let chunks: Result<Vec<MultiLineStringArray<i32>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LargeMultiLineString(_) => {
                let chunks: Result<Vec<MultiLineStringArray<i64>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::MultiPolygon(_) => {
                let chunks: Result<Vec<MultiPolygonArray<i32>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LargeMultiPolygon(_) => {
                let chunks: Result<Vec<MultiPolygonArray<i64>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::Mixed(_) => {
                let chunks: Result<Vec<MixedGeometryArray<i32>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LargeMixed(_) => {
                let chunks: Result<Vec<MixedGeometryArray<i64>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::GeometryCollection(_) => {
                let chunks: Result<Vec<GeometryCollectionArray<i32>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LargeGeometryCollection(_) => {
                let chunks: Result<Vec<GeometryCollectionArray<i64>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::WKB => {
                let chunks: Result<Vec<WKBArray<i32>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LargeWKB => {
                let chunks: Result<Vec<WKBArray<i64>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::Rect => {
                // tryfrom not implemented for RectArray
                todo!()
                // let chunks: Result<Vec<RectArray>> = array_refs
                //     .into_iter()
                //     .map(|arr| arr.as_ref().try_into())
                //     .collect();
                // Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
        }
    }
}
