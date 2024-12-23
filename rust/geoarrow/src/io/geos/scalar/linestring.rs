use crate::array::util::OffsetBufferUtils;
use crate::error::{GeoArrowError, Result};
use crate::io::geos::scalar::coord::{coords_to_geos, dims_to_geos, GEOSConstCoord};
use crate::scalar::LineString;
use geo_traits::LineStringTrait;
use geos::{Geom, GeometryTypes};

impl<'a> TryFrom<&'a LineString<'_>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(value: &'a LineString<'_>) -> std::result::Result<geos::Geometry, geos::Error> {
        let (start, end) = value.geom_offsets.start_end(value.geom_index);

        let sliced_coords = value.coords.clone().slice(start, end - start);

        geos::Geometry::create_line_string(sliced_coords.try_into()?)
    }
}

impl LineString<'_> {
    /// Convert to a GEOS LinearRing
    #[allow(dead_code)]
    pub(crate) fn to_geos_linear_ring(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);

        let sliced_coords = self.coords.clone().slice(start, end - start);

        geos::Geometry::create_linear_ring(sliced_coords.try_into()?)
    }
}

pub(crate) fn to_geos_line_string(
    line_string: &impl LineStringTrait<T = f64>,
) -> std::result::Result<geos::Geometry, geos::Error> {
    let dims = dims_to_geos(line_string.dim());
    let coord_seq = coords_to_geos(line_string.coords(), dims)?;
    geos::Geometry::create_line_string(coord_seq)
}

pub(crate) fn to_geos_linear_ring(
    line_string: &impl LineStringTrait<T = f64>,
) -> std::result::Result<geos::Geometry, geos::Error> {
    let dims = dims_to_geos(line_string.dim());
    let coord_seq = coords_to_geos(line_string.coords(), dims)?;
    geos::Geometry::create_linear_ring(coord_seq)
}

#[derive(Clone)]
pub struct GEOSLineString(geos::Geometry);

impl GEOSLineString {
    pub fn new_unchecked(geom: geos::Geometry) -> Self {
        Self(geom)
    }
    pub fn try_new(geom: geos::Geometry) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::LineString) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be line string".to_string(),
            ))
        }
    }
}

impl LineStringTrait for GEOSLineString {
    type T = f64;
    type CoordType<'b>
        = GEOSConstCoord
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        let point = self.0.get_point_n(i).unwrap();
        GEOSConstCoord {
            coords: point.get_coord_seq().unwrap(),
            geom_index: 0,
            dim: self.dim(),
        }
    }
}

impl LineStringTrait for &GEOSLineString {
    type T = f64;
    type CoordType<'b>
        = GEOSConstCoord
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        let point = self.0.get_point_n(i).unwrap();
        GEOSConstCoord {
            coords: point.get_coord_seq().unwrap(),
            geom_index: 0,
            dim: self.dim(),
        }
    }
}

pub struct GEOSConstLineString<'a>(geos::ConstGeometry<'a>);

impl<'a> GEOSConstLineString<'a> {
    pub fn new_unchecked(geom: geos::ConstGeometry<'a>) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::ConstGeometry<'a>) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::LineString) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be line string".to_string(),
            ))
        }
    }
}

impl LineStringTrait for GEOSConstLineString<'_> {
    type T = f64;
    type CoordType<'b>
        = GEOSConstCoord
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        let point = self.0.get_point_n(i).unwrap();
        GEOSConstCoord {
            coords: point.get_coord_seq().unwrap(),
            geom_index: 0,
            dim: self.dim(),
        }
    }
}

impl<'a> LineStringTrait for &'a GEOSConstLineString<'a> {
    type T = f64;
    type CoordType<'b>
        = GEOSConstCoord
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_coords(&self) -> usize {
        self.0.get_num_points().unwrap()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        let point = self.0.get_point_n(i).unwrap();
        GEOSConstCoord {
            coords: point.get_coord_seq().unwrap(),
            geom_index: 0,
            dim: self.dim(),
        }
    }
}
