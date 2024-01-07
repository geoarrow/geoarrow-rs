use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::OffsetSizeTrait;
use arrow_array::Array;
use arrow_schema::{DataType, Field};

#[cfg(feature = "rayon")]
use rayon::prelude::*;

use crate::array::*;
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::GeometryArrayTrait;

#[derive(Debug, Clone, PartialEq)]
pub struct ChunkedArray<A: Array> {
    pub(crate) chunks: Vec<A>,
    length: usize,
}

impl<A: Array> ChunkedArray<A> {
    pub fn new(chunks: Vec<A>) -> Self {
        let mut length = 0;
        chunks.iter().for_each(|x| length += x.len());
        // TODO: assert all equal data types
        // chunks.iter().map(|x| x.data_type())
        Self { chunks, length }
    }

    pub fn into_inner(self) -> Vec<A> {
        self.chunks
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn data_type(&self) -> &DataType {
        self.chunks.first().unwrap().data_type()
    }

    pub fn chunks(&self) -> &[A] {
        self.chunks.as_slice()
    }

    #[allow(dead_code)]
    pub(crate) fn map<F: Fn(&A) -> R + Sync + Send, R: Send>(&self, map_op: F) -> Vec<R> {
        #[cfg(feature = "rayon")]
        {
            let mut output_vec = Vec::with_capacity(self.chunks.len());
            self.chunks
                .par_iter()
                .map(map_op)
                .collect_into_vec(&mut output_vec);
            output_vec
        }

        #[cfg(not(feature = "rayon"))]
        {
            self.chunks.iter().map(map_op).collect()
        }
    }

    pub(crate) fn try_map<F: Fn(&A) -> Result<R> + Sync + Send, R: Send>(
        &self,
        map_op: F,
    ) -> Result<Vec<R>> {
        #[cfg(feature = "rayon")]
        {
            self.chunks.par_iter().map(map_op).collect()
        }

        #[cfg(not(feature = "rayon"))]
        {
            self.chunks.iter().map(map_op).collect()
        }
    }
}

impl<A: Array> TryFrom<Vec<A>> for ChunkedArray<A> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<A>) -> Result<Self> {
        Ok(Self::new(value))
    }
}

/// ## Invariants:
///
/// - Must have at least one chunk
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkedGeometryArray<G: GeometryArrayTrait> {
    pub(crate) chunks: Vec<G>,
    length: usize,
}

impl<G: GeometryArrayTrait> ChunkedGeometryArray<G> {
    pub fn new(chunks: Vec<G>) -> Self {
        // TODO: assert all equal extension fields
        let mut length = 0;
        chunks.iter().for_each(|x| length += x.len());
        Self { chunks, length }
    }

    // TODO: check/assert on creation that all are the same so we can be comfortable here only
    // taking the first.
    pub fn extension_field(&self) -> Arc<Field> {
        self.chunks.first().unwrap().extension_field()
    }

    pub fn into_inner(self) -> Vec<G> {
        self.chunks
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn chunks(&self) -> &[G] {
        self.chunks.as_slice()
    }

    pub fn data_type(&self) -> &GeoDataType {
        self.chunks.first().unwrap().data_type()
    }

    pub(crate) fn map<F: Fn(&G) -> R + Sync + Send, R: Send>(&self, map_op: F) -> Vec<R> {
        #[cfg(feature = "rayon")]
        {
            let mut output_vec = Vec::with_capacity(self.chunks.len());
            self.chunks
                .par_iter()
                .map(map_op)
                .collect_into_vec(&mut output_vec);
            output_vec
        }

        #[cfg(not(feature = "rayon"))]
        {
            self.chunks.iter().map(map_op).collect()
        }
    }

    pub(crate) fn try_map<F: Fn(&G) -> Result<R> + Sync + Send, R: Send>(
        &self,
        map_op: F,
    ) -> Result<Vec<R>> {
        #[cfg(feature = "rayon")]
        {
            self.chunks.par_iter().map(map_op).collect()
        }

        #[cfg(not(feature = "rayon"))]
        {
            self.chunks.iter().map(map_op).collect()
        }
    }
}

impl<G: GeometryArrayTrait> TryFrom<Vec<G>> for ChunkedGeometryArray<G> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<G>) -> Result<Self> {
        Ok(Self::new(value))
    }
}

pub type ChunkedPointArray = ChunkedGeometryArray<PointArray>;
pub type ChunkedLineStringArray<O> = ChunkedGeometryArray<LineStringArray<O>>;
pub type ChunkedPolygonArray<O> = ChunkedGeometryArray<PolygonArray<O>>;
pub type ChunkedMultiPointArray<O> = ChunkedGeometryArray<MultiPointArray<O>>;
pub type ChunkedMultiLineStringArray<O> = ChunkedGeometryArray<MultiLineStringArray<O>>;
pub type ChunkedMultiPolygonArray<O> = ChunkedGeometryArray<MultiPolygonArray<O>>;
pub type ChunkedMixedGeometryArray<O> = ChunkedGeometryArray<MixedGeometryArray<O>>;
pub type ChunkedGeometryCollectionArray<O> = ChunkedGeometryArray<GeometryCollectionArray<O>>;
pub type ChunkedWKBArray<O> = ChunkedGeometryArray<WKBArray<O>>;
pub type ChunkedRectArray = ChunkedGeometryArray<RectArray>;
pub type ChunkedUnknownGeometryArray = ChunkedGeometryArray<Arc<dyn GeometryArrayTrait>>;

pub trait ChunkedGeometryArrayTrait: std::fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn data_type(&self) -> &GeoDataType;

    fn extension_field(&self) -> Arc<Field>;

    fn geometry_chunks(&self) -> Vec<Arc<dyn GeometryArrayTrait>>;

    fn num_chunks(&self) -> usize;
}

impl ChunkedGeometryArrayTrait for ChunkedPointArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> &GeoDataType {
        self.chunks.first().unwrap().data_type()
    }

    // TODO: check/assert on creation that all are the same so we can be comfortable here only
    // taking the first.
    fn extension_field(&self) -> Arc<Field> {
        self.chunks.first().unwrap().extension_field()
    }

    fn geometry_chunks(&self) -> Vec<Arc<dyn GeometryArrayTrait>> {
        self.chunks
            .iter()
            .map(|chunk| Arc::new(chunk.clone()) as Arc<dyn GeometryArrayTrait>)
            .collect()
    }

    fn num_chunks(&self) -> usize {
        self.chunks.len()
    }
}

macro_rules! impl_trait {
    ($chunked_array:ty) => {
        impl<O: OffsetSizeTrait> ChunkedGeometryArrayTrait for $chunked_array {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn data_type(&self) -> &GeoDataType {
                self.chunks.first().unwrap().data_type()
            }

            // TODO: check/assert on creation that all are the same so we can be comfortable here only
            // taking the first.
            fn extension_field(&self) -> Arc<Field> {
                self.chunks.first().unwrap().extension_field()
            }

            fn geometry_chunks(&self) -> Vec<Arc<dyn GeometryArrayTrait>> {
                self.chunks
                    .iter()
                    .map(|chunk| Arc::new(chunk.clone()) as Arc<dyn GeometryArrayTrait>)
                    .collect()
            }

            fn num_chunks(&self) -> usize {
                self.chunks.len()
            }
        }
    };
}

impl_trait!(ChunkedLineStringArray<O>);
impl_trait!(ChunkedPolygonArray<O>);
impl_trait!(ChunkedMultiPointArray<O>);
impl_trait!(ChunkedMultiLineStringArray<O>);
impl_trait!(ChunkedMultiPolygonArray<O>);
impl_trait!(ChunkedMixedGeometryArray<O>);
impl_trait!(ChunkedGeometryCollectionArray<O>);
impl_trait!(ChunkedWKBArray<O>);

impl ChunkedGeometryArrayTrait for ChunkedRectArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> &GeoDataType {
        self.chunks.first().unwrap().data_type()
    }

    // TODO: check/assert on creation that all are the same so we can be comfortable here only
    // taking the first.
    fn extension_field(&self) -> Arc<Field> {
        self.chunks.first().unwrap().extension_field()
    }

    fn geometry_chunks(&self) -> Vec<Arc<dyn GeometryArrayTrait>> {
        self.chunks
            .iter()
            .map(|chunk| Arc::new(chunk.clone()) as Arc<dyn GeometryArrayTrait>)
            .collect()
    }

    fn num_chunks(&self) -> usize {
        self.chunks.len()
    }
}

/// Does **not** parse WKB. Will return a ChunkedWKBArray for WKB input.
pub fn from_arrow_chunks(
    chunks: &[&dyn Array],
    field: &Field,
) -> Result<Arc<dyn ChunkedGeometryArrayTrait>> {
    if let Some(extension_name) = field.metadata().get("ARROW:extension:name") {
        let geom_arr: Arc<dyn ChunkedGeometryArrayTrait> = match extension_name.as_str() {
            "geoarrow.point" => Arc::new(ChunkedGeometryArray::new(
                chunks
                    .iter()
                    .map(|array| PointArray::try_from(*array))
                    .collect::<Result<Vec<_>>>()?,
            )),
            "geoarrow.linestring" => match field.data_type() {
                DataType::List(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|array| LineStringArray::<i32>::try_from(*array))
                        .collect::<Result<Vec<_>>>()?,
                )),
                DataType::LargeList(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|array| LineStringArray::<i64>::try_from(*array))
                        .collect::<Result<Vec<_>>>()?,
                )),
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.polygon" => match field.data_type() {
                DataType::List(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|array| PolygonArray::<i32>::try_from(*array))
                        .collect::<Result<Vec<_>>>()?,
                )),
                DataType::LargeList(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|array| PolygonArray::<i64>::try_from(*array))
                        .collect::<Result<Vec<_>>>()?,
                )),
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.multipoint" => match field.data_type() {
                DataType::List(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|array| MultiPointArray::<i32>::try_from(*array))
                        .collect::<Result<Vec<_>>>()?,
                )),
                DataType::LargeList(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|array| MultiPointArray::<i64>::try_from(*array))
                        .collect::<Result<Vec<_>>>()?,
                )),
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.multilinestring" => match field.data_type() {
                DataType::List(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|array| MultiLineStringArray::<i32>::try_from(*array))
                        .collect::<Result<Vec<_>>>()?,
                )),
                DataType::LargeList(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|array| MultiLineStringArray::<i64>::try_from(*array))
                        .collect::<Result<Vec<_>>>()?,
                )),
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.multipolygon" => match field.data_type() {
                DataType::List(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|array| MultiPolygonArray::<i32>::try_from(*array))
                        .collect::<Result<Vec<_>>>()?,
                )),
                DataType::LargeList(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|array| MultiPolygonArray::<i64>::try_from(*array))
                        .collect::<Result<Vec<_>>>()?,
                )),
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.geometry" => match field.data_type() {
                DataType::Union(fields, _) => {
                    let mut large_offsets: Vec<bool> = vec![];

                    fields.iter().for_each(|(_type_ids, field)| {
                        match field.data_type() {
                            DataType::List(_) => large_offsets.push(false),
                            DataType::LargeList(_) => large_offsets.push(true),
                            _ => (),
                        };
                    });

                    if large_offsets.is_empty() {
                        // Only contains a point array, we can cast to i32
                        Arc::new(ChunkedGeometryArray::new(
                            chunks
                                .iter()
                                .map(|array| MixedGeometryArray::<i32>::try_from(*array))
                                .collect::<Result<Vec<_>>>()?,
                        ))
                    } else if large_offsets.iter().all(|x| *x) {
                        // All large offsets, cast to i64
                        Arc::new(ChunkedGeometryArray::new(
                            chunks
                                .iter()
                                .map(|array| MixedGeometryArray::<i64>::try_from(*array))
                                .collect::<Result<Vec<_>>>()?,
                        ))
                    } else if large_offsets.iter().all(|x| !x) {
                        // All small offsets, cast to i32
                        Arc::new(ChunkedGeometryArray::new(
                            chunks
                                .iter()
                                .map(|array| MixedGeometryArray::<i32>::try_from(*array))
                                .collect::<Result<Vec<_>>>()?,
                        ))
                    } else {
                        panic!("Mix of offset types");
                    }
                }
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.geometrycollection" => match field.data_type() {
                DataType::List(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|array| GeometryCollectionArray::<i32>::try_from(*array))
                        .collect::<Result<Vec<_>>>()?,
                )),
                DataType::LargeList(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|array| GeometryCollectionArray::<i64>::try_from(*array))
                        .collect::<Result<Vec<_>>>()?,
                )),
                _ => panic!("Unexpected data type"),
            },
            "geoarrow.wkb" | "ogc.wkb" => match field.data_type() {
                DataType::Binary => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|array| WKBArray::<i32>::try_from(*array))
                        .collect::<Result<Vec<_>>>()?,
                )),
                DataType::LargeBinary => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|array| WKBArray::<i64>::try_from(*array))
                        .collect::<Result<Vec<_>>>()?,
                )),
                _ => panic!("Unexpected data type"),
            },
            _ => {
                return Err(GeoArrowError::General(format!(
                    "Unknown geoarrow type {}",
                    extension_name
                )))
            }
        };
        Ok(geom_arr)
    } else {
        // TODO: better error here, and document that arrays without geoarrow extension
        // metadata should use TryFrom for a specific geometry type directly, instead of using
        // GeometryArray
        let geom_arr: Arc<dyn ChunkedGeometryArrayTrait> = match field.data_type() {
            DataType::Binary => Arc::new(ChunkedGeometryArray::new(
                chunks
                    .iter()
                    .map(|array| WKBArray::<i32>::try_from(*array))
                    .collect::<Result<Vec<_>>>()?,
            )),
            DataType::LargeBinary => Arc::new(ChunkedGeometryArray::new(
                chunks
                    .iter()
                    .map(|array| WKBArray::<i64>::try_from(*array))
                    .collect::<Result<Vec<_>>>()?,
            )),
            DataType::Struct(_) => Arc::new(ChunkedGeometryArray::new(
                chunks
                    .iter()
                    .map(|array| PointArray::try_from(*array))
                    .collect::<Result<Vec<_>>>()?,
            )),
            DataType::FixedSizeList(_, _) => Arc::new(ChunkedGeometryArray::new(
                chunks
                    .iter()
                    .map(|array| PointArray::try_from(*array))
                    .collect::<Result<Vec<_>>>()?,
            )),
            _ => {
                return Err(GeoArrowError::General("Only Binary, LargeBinary, FixedSizeList, and Struct arrays are unambigously typed and can be used without extension metadata.".to_string()));
            }
        };
        Ok(geom_arr)
    }
}

pub fn from_geoarrow_chunks(
    chunks: &[&dyn GeometryArrayTrait],
) -> Result<Arc<dyn ChunkedGeometryArrayTrait>> {
    let mut data_types = HashSet::new();
    chunks.iter().for_each(|chunk| {
        data_types.insert(chunk.data_type());
    });
    if data_types.len() == 1 {
        use GeoDataType::*;
        let chunked_arr: Arc<dyn ChunkedGeometryArrayTrait> =
            match *data_types.drain().next().unwrap() {
                Point(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_point().clone())
                        .collect(),
                )),
                LineString(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_line_string().clone())
                        .collect(),
                )),
                LargeLineString(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_large_line_string().clone())
                        .collect(),
                )),
                Polygon(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_polygon().clone())
                        .collect(),
                )),
                LargePolygon(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_large_polygon().clone())
                        .collect(),
                )),
                MultiPoint(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_multi_point().clone())
                        .collect(),
                )),
                LargeMultiPoint(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_large_multi_point().clone())
                        .collect(),
                )),
                MultiLineString(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_multi_line_string().clone())
                        .collect(),
                )),
                LargeMultiLineString(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_large_multi_line_string().clone())
                        .collect(),
                )),
                MultiPolygon(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_multi_polygon().clone())
                        .collect(),
                )),
                LargeMultiPolygon(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_large_multi_polygon().clone())
                        .collect(),
                )),
                Mixed(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_mixed().clone())
                        .collect(),
                )),
                LargeMixed(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_large_mixed().clone())
                        .collect(),
                )),
                GeometryCollection(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_geometry_collection().clone())
                        .collect(),
                )),
                LargeGeometryCollection(_) => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_large_geometry_collection().clone())
                        .collect(),
                )),
                WKB => Arc::new(ChunkedGeometryArray::new(
                    chunks.iter().map(|chunk| chunk.as_wkb().clone()).collect(),
                )),
                LargeWKB => Arc::new(ChunkedGeometryArray::new(
                    chunks
                        .iter()
                        .map(|chunk| chunk.as_large_wkb().clone())
                        .collect(),
                )),
                Rect => Arc::new(ChunkedGeometryArray::new(
                    chunks.iter().map(|chunk| chunk.as_rect().clone()).collect(),
                )),
            };
        Ok(chunked_arr)
    } else {
        todo!()
    }
}
