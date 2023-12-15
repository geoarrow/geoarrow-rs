use crate::array::mixed::MixedCapacity;
use crate::geo_traits::GeometryCollectionTrait;

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

    pub fn add_geometry_collection<'a>(
        &mut self,
        geom: Option<&'a (impl GeometryCollectionTrait + 'a)>,
    ) {
        if let Some(geom) = geom {
            for i in 0..geom.num_geometries() {
                let g = geom.geometry(i).unwrap();
                self.mixed_capacity.add_geometry(Some(&g))
            }
        }
        self.geom_capacity += 1;
    }

    pub fn from_geometry_collections<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_geometry_collection(maybe_geom);
        }
        counter
    }

    pub fn from_owned_geometries<'a>(
        geoms: impl Iterator<Item = Option<(impl GeometryCollectionTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_geometry_collection(maybe_geom.as_ref());
        }
        counter
    }
}

impl Default for GeometryCollectionCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}
