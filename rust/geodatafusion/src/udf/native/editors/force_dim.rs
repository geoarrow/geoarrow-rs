//! Editors to force dimensions of geometries.

use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::ArrayRef;
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geoarrow_array::array::{
    CoordBuffer, GeometryArray, GeometryCollectionArray, InterleavedCoordBuffer, LineStringArray,
    MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray, PolygonArray, RectArray,
    SeparatedCoordBuffer, from_arrow_array,
};
use geoarrow_array::builder::{GeometryBuilder, GeometryCollectionBuilder};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, Dimension, GeoArrowType, GeometryType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug)]
pub struct Force2D {
    signature: Signature,
    coord_type: CoordType,
}

impl Force2D {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: any_single_geometry_type_input(),
            coord_type,
        }
    }
}

impl Default for Force2D {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static FORCE2D_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Force2D {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_force2d"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        Ok(impl_return_field(args, Dimension::XY, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(impl_invoke_with_args(args, Dimension::XY, self.coord_type)?.into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FORCE2D_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Forces the geometries into a \"2-dimensional mode\" so that all output representations will only have the X and Y coordinates.",
                "ST_Force2D(geometry)",
            )
            .with_argument("geomA", "geometry")
            .build()
        }))
    }
}

#[derive(Debug)]
pub struct Force3DZ {
    signature: Signature,
    coord_type: CoordType,
    aliases: Vec<String>,
}

impl Force3DZ {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: any_single_geometry_type_input(),
            coord_type,
            aliases: vec!["st_force3d".to_string()],
        }
    }
}

impl Default for Force3DZ {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static FORCE3DZ_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Force3DZ {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_force3dz"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        Ok(impl_return_field(args, Dimension::XYZ, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(impl_invoke_with_args(args, Dimension::XYZ, self.coord_type)?.into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FORCE3DZ_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Forces the geometries into XYZ mode. If a geometry has no Z component, then a Zvalue Z coordinate is tacked on.",
                "ST_Force3DZ(geometry)",
            )
            .with_argument("geomA", "geometry")
            .build()
        }))
    }
}

#[derive(Debug)]
pub struct Force3DM {
    signature: Signature,
    coord_type: CoordType,
}

impl Force3DM {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: any_single_geometry_type_input(),
            coord_type,
        }
    }
}

impl Default for Force3DM {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static FORCE3DM_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Force3DM {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_force3dm"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        Ok(impl_return_field(args, Dimension::XYM, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(impl_invoke_with_args(args, Dimension::XYM, self.coord_type)?.into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FORCE3DM_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Forces the geometries into XYM mode. If a geometry has no M component, then a Mvalue M coordinate is tacked on. If it has a Z component, then Z is removed.",
                "ST_Force3DM(geometry)",
            )
            .with_argument("geomA", "geometry")
            .build()
        }))
    }
}

#[derive(Debug)]
pub struct Force4D {
    signature: Signature,
    coord_type: CoordType,
}

impl Force4D {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: any_single_geometry_type_input(),
            coord_type,
        }
    }
}

impl Default for Force4D {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static FORCE4D_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Force4D {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_force4d"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        Ok(impl_return_field(args, Dimension::XYZM, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(impl_invoke_with_args(args, Dimension::XYZM, self.coord_type)?.into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FORCE4D_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Forces the geometries into XYZM mode. Zvalue and Mvalue is tacked on for missing Z and M dimensions, respectively.",
                "ST_Force4D(geometry)",
            )
            .with_argument("geomA", "geometry")
            .build()
        }))
    }
}

fn impl_return_field(
    args: ReturnFieldArgs,
    target_dim: Dimension,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let field = &args.arg_fields[0];
    let geom_type = GeoArrowType::from_extension_field(field)?;
    Ok(geom_type
        .with_dimension(target_dim)
        .with_coord_type(coord_type)
        .to_field("", true)
        .into())
}

fn impl_invoke_with_args(
    args: ScalarFunctionArgs,
    target_dim: Dimension,
    coord_type: CoordType,
) -> GeoDataFusionResult<ArrayRef> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let field = &args.arg_fields[0];
    let geom_array = from_arrow_array(&arrays[0], field)?;
    Ok(force_dim(geom_array, target_dim, coord_type)?.into_array_ref())
}

fn force_dim(
    array: Arc<dyn GeoArrowArray>,
    target_dim: Dimension,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    if let Some(dim) = array.data_type().dimension() {
        if dim == target_dim {
            return Ok(array);
        }
    }

    use GeoArrowType::*;
    let out = match array.data_type() {
        Point(typ) => match typ.dimension() {
            Dimension::XY => array,
            _ => Arc::new(force_point_dim(array.as_point(), target_dim)),
        },
        LineString(typ) => match typ.dimension() {
            Dimension::XY => array,
            _ => Arc::new(force_line_string_dim(array.as_line_string(), target_dim)),
        },
        Polygon(typ) => match typ.dimension() {
            Dimension::XY => array,
            _ => Arc::new(force_polygon_dim(array.as_polygon(), target_dim)),
        },
        MultiPoint(typ) => match typ.dimension() {
            Dimension::XY => array,
            _ => Arc::new(force_multi_point_dim(array.as_multi_point(), target_dim)),
        },
        MultiLineString(typ) => match typ.dimension() {
            Dimension::XY => array,
            _ => Arc::new(force_multi_line_string_dim(
                array.as_multi_line_string(),
                target_dim,
            )),
        },
        MultiPolygon(typ) => match typ.dimension() {
            Dimension::XY => array,
            _ => Arc::new(force_multi_polygon_dim(
                array.as_multi_polygon(),
                target_dim,
            )),
        },
        Rect(typ) => match typ.dimension() {
            Dimension::XY => array,
            _ => Arc::new(force_rect_dim(array.as_rect(), target_dim)),
        },
        GeometryCollection(_) => Arc::new(force_geometry_collection_dim(
            array.as_geometry_collection(),
            target_dim,
        )?),
        _ => {
            let array_ref = array.as_ref();
            Arc::new(downcast_geoarrow_array!(
                array_ref,
                force_dim_geometry_impl,
                target_dim,
                coord_type
            )?)
        }
    };
    Ok(out)
}

fn force_coords_dim(coords: &CoordBuffer, target_dim: Dimension) -> CoordBuffer {
    match coords {
        CoordBuffer::Interleaved(cb) => force_interleaved_coords_dim(cb, target_dim).into(),
        CoordBuffer::Separated(cb) => force_separated_coords_dim(cb, target_dim).into(),
    }
}

fn force_interleaved_coords_dim(
    coords: &InterleavedCoordBuffer,
    target_dim: Dimension,
) -> InterleavedCoordBuffer {
    todo!()
    // let mut new_coords = Vec::with_capacity(cb.len() * 2);
    // let existing_coords = cb.coords();
    // match cb.dim() {
    //     Dimension::XY => unreachable!(),
    //     Dimension::XYZ | Dimension::XYM => {
    //         for coord_idx in 0..cb.len() {
    //             let x = existing_coords[coord_idx * 3];
    //             let y = existing_coords[coord_idx * 3 + 1];
    //             new_coords.push(x);
    //             new_coords.push(y);
    //         }
    //     }
    //     Dimension::XYZM => {
    //         for coord_idx in 0..cb.len() {
    //             let x = existing_coords[coord_idx * 4];
    //             let y = existing_coords[coord_idx * 4 + 1];
    //             new_coords.push(x);
    //             new_coords.push(y);
    //         }
    //     }
    // }
    // InterleavedCoordBuffer::new(new_coords.into(), Dimension::XY).into()
}

fn force_separated_coords_dim(
    coords: &SeparatedCoordBuffer,
    target_dim: Dimension,
) -> SeparatedCoordBuffer {
    use Dimension::*;

    let current_dim = coords.dim();
    let mut buffers = coords.raw_buffers().clone();
    match (current_dim, target_dim) {
        // Same dimensions: no change
        (XY, XY) | (XYZ, XYZ) | (XYM, XYM) | (XYZM, XYZM) => return coords.clone(),
        // Down to 2D: remove dimensions
        (_, Dimension::XY) => {
            buffers[2] = vec![].into();
            buffers[3] = vec![].into();
            SeparatedCoordBuffer::from_array(buffers, target_dim).unwrap()
        }
        // 2d to 3d: add buffer
        (XY, XYZ) | (XY, XYM) => {
            buffers[2] = vec![0.0; coords.len()].into();
            SeparatedCoordBuffer::from_array(buffers, target_dim).unwrap()
        }
        // 3d to 3d: keep buffers same, change dimension semantics
        (XYZ, XYM) | (XYM, XYZ) => SeparatedCoordBuffer::from_array(buffers, target_dim).unwrap(),
        // 4d to 3d: remove m buffer
        (XYZM, XYZ) | (XYZM, XYM) => {
            buffers[3] = vec![].into();
            SeparatedCoordBuffer::from_array(buffers, target_dim).unwrap()
        }
        // 2d to 4d: add z and m buffers
        (XY, XYZM) => {
            buffers[2] = vec![0.0; coords.len()].into();
            buffers[3] = vec![0.0; coords.len()].into();
            SeparatedCoordBuffer::from_array(buffers, target_dim).unwrap()
        }
        // 3d to 4d: add m buffer
        (XYZ, XYZM) | (XYM, XYZM) => {
            buffers[3] = vec![0.0; coords.len()].into();
            SeparatedCoordBuffer::from_array(buffers, target_dim).unwrap()
        }
    }
}

fn force_point_dim(array: &PointArray, target_dim: Dimension) -> PointArray {
    PointArray::new(
        force_coords_dim(array.coords(), target_dim),
        array.logical_nulls(),
        array.data_type().metadata().clone(),
    )
}

fn force_line_string_dim(array: &LineStringArray, target_dim: Dimension) -> LineStringArray {
    LineStringArray::new(
        force_coords_dim(array.coords(), target_dim),
        array.geom_offsets().clone(),
        array.logical_nulls(),
        array.data_type().metadata().clone(),
    )
}

fn force_polygon_dim(array: &PolygonArray, target_dim: Dimension) -> PolygonArray {
    PolygonArray::new(
        force_coords_dim(array.coords(), target_dim),
        array.geom_offsets().clone(),
        array.ring_offsets().clone(),
        array.logical_nulls(),
        array.data_type().metadata().clone(),
    )
}

fn force_multi_point_dim(array: &MultiPointArray, target_dim: Dimension) -> MultiPointArray {
    MultiPointArray::new(
        force_coords_dim(array.coords(), target_dim),
        array.geom_offsets().clone(),
        array.logical_nulls(),
        array.data_type().metadata().clone(),
    )
}

fn force_multi_line_string_dim(
    array: &MultiLineStringArray,
    target_dim: Dimension,
) -> MultiLineStringArray {
    MultiLineStringArray::new(
        force_coords_dim(array.coords(), target_dim),
        array.geom_offsets().clone(),
        array.ring_offsets().clone(),
        array.logical_nulls(),
        array.data_type().metadata().clone(),
    )
}

fn force_multi_polygon_dim(array: &MultiPolygonArray, target_dim: Dimension) -> MultiPolygonArray {
    MultiPolygonArray::new(
        force_coords_dim(array.coords(), target_dim),
        array.geom_offsets().clone(),
        array.polygon_offsets().clone(),
        array.ring_offsets().clone(),
        array.logical_nulls(),
        array.data_type().metadata().clone(),
    )
}

fn force_rect_dim(array: &RectArray, target_dim: Dimension) -> RectArray {
    RectArray::new(
        force_separated_coords_dim(array.lower(), target_dim),
        force_separated_coords_dim(array.upper(), target_dim),
        array.logical_nulls(),
        array.data_type().metadata().clone(),
    )
}

fn force_dim_geometry_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    target_dim: Dimension,
    coord_type: CoordType,
) -> GeoArrowResult<GeometryArray> {
    // TODO: use with_capacity
    let mut builder = GeometryBuilder::new(GeometryType::new(array.data_type().metadata().clone()));
    for geom in array.iter() {
        if let Some(geom) = geom {
            todo!();
            // builder.push_geometry(Some(&geom_2d))?;
        } else {
            builder.push_null();
        }
    }
    Ok(builder.finish())
}

fn force_geometry_collection_dim(
    array: &GeometryCollectionArray,
    target_dim: Dimension,
) -> GeoArrowResult<GeometryCollectionArray> {
    // TODO: use with_capacity
    let mut builder =
        GeometryCollectionBuilder::new(array.extension_type().clone().with_dimension(target_dim));
    for geom in array.iter() {
        if let Some(geom) = geom {
            todo!();
            // builder.push_geometry_collection(Some(&geom_2d))?;
        } else {
            builder.push_null();
        }
    }
    Ok(builder.finish())
}

#[cfg(test)]
mod test {
    use super::*;
    use geo_traits::to_geo::{ToGeoPoint, ToGeoPolygon};
    use geoarrow_array::cast::to_wkb;
    use geoarrow_array::test::{point, polygon};
    use geoarrow_geo::util::to_geo::geometry_to_geo;
    use geoarrow_schema::CoordType;

    #[test]
    fn test_force_2d_point() {
        let array = point::array(CoordType::Separated, Dimension::XYZ);
        let array_2d =
            force_dim(Arc::new(array.clone()), Dimension::XY, CoordType::default()).unwrap();
        let point_array_2d = array_2d.as_point();
        let pt0 = array.value(0).unwrap().to_point();
        let pt1 = point_array_2d.value(0).unwrap().to_point();
        assert_eq!(pt0, pt1);
    }

    #[test]
    fn test_force_2d_polygon() {
        let array = polygon::array(CoordType::Separated, Dimension::XYZ);
        let array_2d =
            force_dim(Arc::new(array.clone()), Dimension::XY, CoordType::default()).unwrap();
        let polygon_array_2d = array_2d.as_polygon();
        let pt0 = array.value(0).unwrap().to_polygon();
        let pt1 = polygon_array_2d.value(0).unwrap().to_polygon();
        assert_eq!(pt0, pt1);
    }

    #[test]
    fn test_force_2d_wkb() {
        let array = polygon::array(CoordType::Separated, Dimension::XYZ);
        let wkb_array = to_wkb::<i32>(&array).unwrap();
        let array_2d = force_dim(
            Arc::new(wkb_array.clone()),
            Dimension::XY,
            CoordType::default(),
        )
        .unwrap();
        let geometry_array_2d = array_2d.as_geometry();
        let pt0 = geometry_to_geo(&array.value(0).unwrap()).unwrap();
        let pt1 = geometry_to_geo(&geometry_array_2d.value(0).unwrap()).unwrap();
        assert_eq!(pt0, pt1);
    }
}
