use std::ops::Add;

use crate::array::mixed::MixedCapacity;
use crate::error::Result;
use crate::geo_traits::{
    GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait, MultiLineStringTrait,
    MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
};

#[derive(Debug, Clone, Copy)]
pub struct GeometryCollectionCapacity {
    pub(crate) mixed_capacity: MixedCapacity,
    pub(crate) geom_capacity: usize,
}

impl GeometryCollectionCapacity {
    pub fn new(mixed_capacity: MixedCapacity, geom_capacity: usize) -> Self {
        Self {
            mixed_capacity,
            geom_capacity,
        }
    }

    pub fn new_empty() -> Self {
        Self::new(MixedCapacity::new_empty(), 0)
    }

    pub fn is_empty(&self) -> bool {
        self.mixed_capacity.is_empty() && self.geom_capacity == 0
    }

    #[inline]
    fn add_valid_point(&mut self, _geom: &impl PointTrait) {
        self.mixed_capacity.add_point();
    }

    #[inline]
    fn add_valid_line_string(&mut self, geom: &impl LineStringTrait) {
        self.mixed_capacity.add_line_string(Some(geom));
    }

    #[inline]
    fn add_valid_polygon(&mut self, geom: &impl PolygonTrait) {
        self.mixed_capacity.add_polygon(Some(geom));
    }

    #[inline]
    fn add_valid_multi_point(&mut self, geom: &impl MultiPointTrait) {
        self.mixed_capacity.add_multi_point(Some(geom));
    }

    #[inline]
    fn add_valid_multi_line_string(&mut self, geom: &impl MultiLineStringTrait) {
        self.mixed_capacity.add_multi_line_string(Some(geom));
    }

    #[inline]
    fn add_valid_multi_polygon(&mut self, geom: &impl MultiPolygonTrait) {
        self.mixed_capacity.add_multi_polygon(Some(geom));
    }

    #[inline]
    fn add_valid_geometry_collection(&mut self, geom: &impl GeometryCollectionTrait) -> Result<()> {
        for i in 0..geom.num_geometries() {
            let g = geom.geometry(i).unwrap();
            self.mixed_capacity.add_geometry(Some(&g))?
        }
        Ok(())
    }

    #[inline]
    pub fn add_geometry(&mut self, geom: Option<&impl GeometryTrait>) -> Result<()> {
        if let Some(geom) = geom {
            match geom.as_type() {
                GeometryType::Point(p) => self.add_valid_point(p),
                GeometryType::LineString(p) => self.add_valid_line_string(p),
                GeometryType::Polygon(p) => self.add_valid_polygon(p),
                GeometryType::MultiPoint(p) => self.add_valid_multi_point(p),
                GeometryType::MultiLineString(p) => self.add_valid_multi_line_string(p),
                GeometryType::MultiPolygon(p) => self.add_valid_multi_polygon(p),
                GeometryType::GeometryCollection(p) => self.add_valid_geometry_collection(p)?,
                GeometryType::Rect(_) => todo!(),
            }
        };
        Ok(())
    }

    #[inline]
    pub fn add_geometry_collection<'a>(
        &mut self,
        geom: Option<&'a (impl GeometryCollectionTrait + 'a)>,
    ) -> Result<()> {
        if let Some(geom) = geom {
            self.add_valid_geometry_collection(geom)?;
        }
        self.geom_capacity += 1;
        Ok(())
    }

    pub fn from_geometry_collections<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait + 'a)>>,
    ) -> Result<Self> {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_geometry_collection(maybe_geom)?;
        }
        Ok(counter)
    }

    pub fn from_owned_geometries<'a>(
        geoms: impl Iterator<Item = Option<(impl GeometryCollectionTrait + 'a)>>,
    ) -> Result<Self> {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_geometry_collection(maybe_geom.as_ref())?;
        }
        Ok(counter)
    }

    pub fn from_geometries<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
    ) -> Result<Self> {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_geometry(maybe_geom)?;
        }
        Ok(counter)
    }
}

impl Default for GeometryCollectionCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl Add for GeometryCollectionCapacity {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let mixed_capacity = self.mixed_capacity + rhs.mixed_capacity;
        let geom_capacity = self.geom_capacity + rhs.geom_capacity;

        Self::new(mixed_capacity, geom_capacity)
    }
}
