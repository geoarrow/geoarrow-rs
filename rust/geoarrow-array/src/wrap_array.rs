use std::sync::Arc;

use crate::GeoArrowArray;
use crate::array::*;
use arrow_array::{
    Array, BinaryArray, BinaryViewArray, FixedSizeListArray, LargeBinaryArray, LargeStringArray,
    ListArray, StringArray, StringViewArray, StructArray, UnionArray,
};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::*;

pub trait WrapArray<Input> {
    type Output: GeoArrowArray;

    fn wrap_array(&self, input: Input) -> GeoArrowResult<Self::Output>;
}

impl WrapArray<&StructArray> for PointType {
    type Output = PointArray;

    fn wrap_array(&self, input: &StructArray) -> GeoArrowResult<Self::Output> {
        PointArray::try_from((input, self))
    }
}

impl WrapArray<&FixedSizeListArray> for PointType {
    type Output = PointArray;

    fn wrap_array(&self, input: &FixedSizeListArray) -> GeoArrowResult<Self::Output> {
        PointArray::try_from((input, self))
    }
}

impl WrapArray<&dyn Array> for PointType {
    type Output = PointArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        PointArray::try_from((input, self))
    }
}

impl WrapArray<&ListArray> for LineStringType {
    type Output = LineStringArray;

    fn wrap_array(&self, input: &ListArray) -> GeoArrowResult<Self::Output> {
        LineStringArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&dyn Array> for LineStringType {
    type Output = LineStringArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        LineStringArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&ListArray> for PolygonType {
    type Output = PolygonArray;

    fn wrap_array(&self, input: &ListArray) -> GeoArrowResult<Self::Output> {
        PolygonArray::try_from((input, self))
    }
}

impl WrapArray<&dyn Array> for PolygonType {
    type Output = PolygonArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        PolygonArray::try_from((input, self))
    }
}

impl WrapArray<&ListArray> for MultiPointType {
    type Output = MultiPointArray;

    fn wrap_array(&self, input: &ListArray) -> GeoArrowResult<Self::Output> {
        MultiPointArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&dyn Array> for MultiPointType {
    type Output = MultiPointArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        MultiPointArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&ListArray> for MultiLineStringType {
    type Output = MultiLineStringArray;

    fn wrap_array(&self, input: &ListArray) -> GeoArrowResult<Self::Output> {
        MultiLineStringArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&dyn Array> for MultiLineStringType {
    type Output = MultiLineStringArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        MultiLineStringArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&ListArray> for MultiPolygonType {
    type Output = MultiPolygonArray;

    fn wrap_array(&self, input: &ListArray) -> GeoArrowResult<Self::Output> {
        MultiPolygonArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&dyn Array> for MultiPolygonType {
    type Output = MultiPolygonArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        MultiPolygonArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&StructArray> for BoxType {
    type Output = RectArray;

    fn wrap_array(&self, input: &StructArray) -> GeoArrowResult<Self::Output> {
        RectArray::try_from((input, self))
    }
}

impl WrapArray<&dyn Array> for BoxType {
    type Output = RectArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        RectArray::try_from((input, self))
    }
}

impl WrapArray<&ListArray> for GeometryCollectionType {
    type Output = GeometryCollectionArray;

    fn wrap_array(&self, input: &ListArray) -> GeoArrowResult<Self::Output> {
        GeometryCollectionArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&dyn Array> for GeometryCollectionType {
    type Output = GeometryCollectionArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        GeometryCollectionArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&UnionArray> for GeometryType {
    type Output = GeometryArray;

    fn wrap_array(&self, input: &UnionArray) -> GeoArrowResult<Self::Output> {
        GeometryArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&dyn Array> for GeometryType {
    type Output = GeometryArray;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        GeometryArray::try_from((input, self.clone()))
    }
}

impl WrapArray<&BinaryViewArray> for WkbType {
    type Output = WkbViewArray;

    fn wrap_array(&self, input: &BinaryViewArray) -> GeoArrowResult<Self::Output> {
        Ok(WkbViewArray::from((input.clone(), self)))
    }
}

impl WrapArray<&BinaryArray> for WkbType {
    type Output = WkbArray;

    fn wrap_array(&self, input: &BinaryArray) -> GeoArrowResult<Self::Output> {
        Ok(WkbArray::from((input.clone(), self.clone())))
    }
}

impl WrapArray<&LargeBinaryArray> for WkbType {
    type Output = LargeWkbArray;

    fn wrap_array(&self, input: &LargeBinaryArray) -> GeoArrowResult<Self::Output> {
        Ok(LargeWkbArray::from((input.clone(), self.clone())))
    }
}

impl WrapArray<&StringViewArray> for WktType {
    type Output = WktViewArray;

    fn wrap_array(&self, input: &StringViewArray) -> GeoArrowResult<Self::Output> {
        Ok(WktViewArray::from((input.clone(), self.clone())))
    }
}

impl WrapArray<&StringArray> for WktType {
    type Output = WktArray;

    fn wrap_array(&self, input: &StringArray) -> GeoArrowResult<Self::Output> {
        Ok(WktArray::from((input.clone(), self.clone())))
    }
}

impl WrapArray<&LargeStringArray> for WktType {
    type Output = LargeWktArray;

    fn wrap_array(&self, input: &LargeStringArray) -> GeoArrowResult<Self::Output> {
        Ok(LargeWktArray::from((input.clone(), self.clone())))
    }
}

impl WrapArray<&dyn Array> for GeoArrowType {
    type Output = Arc<dyn GeoArrowArray>;

    fn wrap_array(&self, input: &dyn Array) -> GeoArrowResult<Self::Output> {
        use GeoArrowType::*;

        let result: Arc<dyn GeoArrowArray> = match self {
            Point(t) => Arc::new(t.wrap_array(input)?),
            LineString(t) => Arc::new(t.wrap_array(input)?),
            Polygon(t) => Arc::new(t.wrap_array(input)?),
            MultiPoint(t) => Arc::new(t.wrap_array(input)?),
            MultiLineString(t) => Arc::new(t.wrap_array(input)?),
            MultiPolygon(t) => Arc::new(t.wrap_array(input)?),
            GeometryCollection(t) => Arc::new(t.wrap_array(input)?),
            Rect(t) => Arc::new(t.wrap_array(input)?),
            Geometry(t) => Arc::new(t.wrap_array(input)?),
            Wkb(t) => Arc::new(WkbArray::try_from((input, t.clone()))?),
            LargeWkb(t) => Arc::new(LargeWkbArray::try_from((input, t.clone()))?),
            WkbView(t) => Arc::new(WkbViewArray::try_from((input, t.clone()))?),
            Wkt(t) => Arc::new(WktArray::try_from((input, t.clone()))?),
            LargeWkt(t) => Arc::new(LargeWktArray::try_from((input, t.clone()))?),
            WktView(t) => Arc::new(WktViewArray::try_from((input, t.clone()))?),
        };
        Ok(result)

        // from_arrow_array(array, field)
        // Arc<dyn GeoArrowArray>::try_from((input, self.clone()))
    }
}
