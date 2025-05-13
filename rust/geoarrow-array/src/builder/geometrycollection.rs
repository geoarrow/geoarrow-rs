use arrow_array::OffsetSizeTrait;
use arrow_buffer::NullBufferBuilder;
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait,
};
use geoarrow_schema::GeometryCollectionType;
use wkt::WktNum;

use crate::array::{GeometryCollectionArray, WkbArray};
use crate::builder::geo_trait_wrappers::{LineWrapper, RectWrapper, TriangleWrapper};
use crate::builder::{MixedGeometryBuilder, OffsetsBuilder};
use crate::capacity::GeometryCollectionCapacity;
use crate::error::{GeoArrowError, Result};
use crate::trait_::{GeoArrowArrayAccessor, GeometryArrayBuilder};

/// The GeoArrow equivalent to `Vec<Option<GeometryCollection>>`: a mutable collection of
/// GeometryCollections.
///
/// Converting an [`GeometryCollectionBuilder`] into a [`GeometryCollectionArray`] is `O(1)`.
#[derive(Debug)]
pub struct GeometryCollectionBuilder {
    data_type: GeometryCollectionType,

    pub(crate) geoms: MixedGeometryBuilder,

    pub(crate) geom_offsets: OffsetsBuilder<i32>,

    pub(crate) validity: NullBufferBuilder,
}

impl<'a> GeometryCollectionBuilder {
    /// Creates a new empty [`GeometryCollectionBuilder`].
    pub fn new(typ: GeometryCollectionType) -> Self {
        Self::with_capacity(typ, Default::default())
    }

    /// Creates a new empty [`GeometryCollectionBuilder`] with the provided capacity.
    pub fn with_capacity(
        typ: GeometryCollectionType,
        capacity: GeometryCollectionCapacity,
    ) -> Self {
        Self {
            geoms: MixedGeometryBuilder::with_capacity_and_options(
                typ.dimension(),
                capacity.mixed_capacity,
                typ.coord_type(),
            ),
            geom_offsets: OffsetsBuilder::with_capacity(capacity.geom_capacity),
            validity: NullBufferBuilder::new(capacity.geom_capacity),
            data_type: typ,
        }
    }

    /// Change whether to prefer multi or single arrays for new single-part geometries.
    ///
    /// If `true`, a new `Point` will be added to the `MultiPointBuilder` child array, a new
    /// `LineString` will be added to the `MultiLineStringBuilder` child array, and a new `Polygon`
    /// will be added to the `MultiPolygonBuilder` child array.
    ///
    /// This can be desired when the user wants to downcast the array to a single geometry array
    /// later, as casting to a, say, `MultiPointArray` from a `GeometryCollectionArray` could be
    /// done zero-copy.
    ///
    /// Note that only geometries added _after_ this method is called will be affected.
    pub fn with_prefer_multi(self, prefer_multi: bool) -> Self {
        Self {
            geoms: self.geoms.with_prefer_multi(prefer_multi),
            ..self
        }
    }

    /// Reserves capacity for at least `additional` more GeometryCollections.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: GeometryCollectionCapacity) {
        self.geoms.reserve(additional.mixed_capacity);
        self.geom_offsets.reserve(additional.geom_capacity);
    }

    /// Reserves the minimum capacity for at least `additional` more GeometryCollections.
    ///
    /// Unlike [`reserve`], this will not deliberately over-allocate to speculatively avoid
    /// frequent allocations. After calling `reserve_exact`, capacity will be greater than or equal
    /// to `self.len() + additional`. Does nothing if the capacity is already sufficient.
    ///
    /// Note that the allocator may give the collection more space than it
    /// requests. Therefore, capacity can not be relied upon to be precisely
    /// minimal. Prefer [`reserve`] if future insertions are expected.
    ///
    /// [`reserve`]: Self::reserve
    pub fn reserve_exact(&mut self, additional: GeometryCollectionCapacity) {
        self.geoms.reserve_exact(additional.mixed_capacity);
        self.geom_offsets.reserve_exact(additional.geom_capacity);
    }

    /// Consume the builder and convert to an immutable [`GeometryCollectionArray`]
    pub fn finish(mut self) -> GeometryCollectionArray {
        let validity = self.validity.finish();
        GeometryCollectionArray::new(
            self.geoms.finish(),
            self.geom_offsets.finish(),
            validity,
            self.data_type.metadata().clone(),
        )
    }

    /// Creates a new [`GeometryCollectionBuilder`] with a capacity inferred by the provided
    /// iterator.
    pub fn with_capacity_from_iter<T: WktNum>(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait<T = T> + 'a)>>,
        typ: GeometryCollectionType,
    ) -> Result<Self> {
        let counter = GeometryCollectionCapacity::from_geometry_collections(geoms)?;
        Ok(Self::with_capacity(typ, counter))
    }

    /// Reserve more space in the underlying buffers with the capacity inferred from the provided
    /// geometries.
    pub fn reserve_from_iter<T: WktNum>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait<T = T> + 'a)>>,
    ) -> Result<()> {
        let counter = GeometryCollectionCapacity::from_geometry_collections(geoms)?;
        self.reserve(counter);
        Ok(())
    }

    /// Reserve more space in the underlying buffers with the capacity inferred from the provided
    /// geometries.
    pub fn reserve_exact_from_iter<T: WktNum>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait<T = T> + 'a)>>,
    ) -> Result<()> {
        let counter = GeometryCollectionCapacity::from_geometry_collections(geoms)?;
        self.reserve_exact(counter);
        Ok(())
    }

    /// Push a Point onto the end of this builder
    #[inline]
    pub fn push_point(&mut self, value: Option<&impl PointTrait<T = f64>>) -> Result<()> {
        if let Some(geom) = value {
            self.geoms.push_point(geom)?;
            self.geom_offsets.try_push_usize(1)?;
            self.validity.append(value.is_some());
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Push a LineString onto the end of this builder
    #[inline]
    pub fn push_line_string(
        &mut self,
        value: Option<&impl LineStringTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(geom) = value {
            self.geoms.push_line_string(geom)?;
            self.geom_offsets.try_push_usize(1)?;
            self.validity.append(value.is_some());
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Push a Polygon onto the end of this builder
    #[inline]
    pub fn push_polygon(&mut self, value: Option<&impl PolygonTrait<T = f64>>) -> Result<()> {
        if let Some(geom) = value {
            self.geoms.push_polygon(geom)?;
            self.geom_offsets.try_push_usize(1)?;
            self.validity.append(value.is_some());
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Push a MultiPoint onto the end of this builder
    #[inline]
    pub fn push_multi_point(
        &mut self,
        value: Option<&impl MultiPointTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(geom) = value {
            self.geoms.push_multi_point(geom)?;
            self.geom_offsets.try_push_usize(1)?;
            self.validity.append(value.is_some());
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Push a MultiLineString onto the end of this builder
    #[inline]
    pub fn push_multi_line_string(
        &mut self,
        value: Option<&impl MultiLineStringTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(geom) = value {
            self.geoms.push_multi_line_string(geom)?;
            self.geom_offsets.try_push_usize(1)?;
            self.validity.append(value.is_some());
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Push a MultiPolygon onto the end of this builder
    #[inline]
    pub fn push_multi_polygon(
        &mut self,
        value: Option<&impl MultiPolygonTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(geom) = value {
            self.geoms.push_multi_polygon(geom)?;
            self.geom_offsets.try_push_usize(1)?;
            self.validity.append(value.is_some());
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Push a Geometry onto the end of this builder
    #[inline]
    pub fn push_geometry(&mut self, value: Option<&impl GeometryTrait<T = f64>>) -> Result<()> {
        use geo_traits::GeometryType::*;

        if let Some(g) = value {
            match g.as_type() {
                Point(p) => self.push_point(Some(p))?,
                LineString(p) => {
                    self.push_line_string(Some(p))?;
                }
                Polygon(p) => self.push_polygon(Some(p))?,
                MultiPoint(p) => self.push_multi_point(Some(p))?,
                MultiLineString(p) => self.push_multi_line_string(Some(p))?,
                MultiPolygon(p) => self.push_multi_polygon(Some(p))?,
                GeometryCollection(p) => self.push_geometry_collection(Some(p))?,
                Rect(r) => self.push_polygon(Some(&RectWrapper::try_new(r)?))?,
                Triangle(tri) => self.push_polygon(Some(&TriangleWrapper(tri)))?,
                Line(l) => self.push_line_string(Some(&LineWrapper(l)))?,
            }
        } else {
            self.push_null();
        };
        Ok(())
    }

    /// Push a GeometryCollection onto the end of this builder
    #[inline]
    pub fn push_geometry_collection(
        &mut self,
        value: Option<&impl GeometryCollectionTrait<T = f64>>,
    ) -> Result<()> {
        if let Some(gc) = value {
            let num_geoms = gc.num_geometries();
            for g in gc.geometries() {
                self.geoms.push_geometry(&g)?;
            }
            self.try_push_length(num_geoms)?;
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Extend this builder with the given geometries
    pub fn extend_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryCollectionTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_gc| self.push_geometry_collection(maybe_gc))
            .unwrap();
    }

    #[inline]
    pub(crate) fn try_push_length(&mut self, geom_offsets_length: usize) -> Result<()> {
        self.geom_offsets.try_push_usize(geom_offsets_length)?;
        self.validity.append(true);
        Ok(())
    }

    #[inline]
    pub(crate) fn push_null(&mut self) {
        self.geom_offsets.extend_constant(1);
        self.validity.append(false);
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_geometry_collections(
        geoms: &[impl GeometryCollectionTrait<T = f64>],
        typ: GeometryCollectionType,
    ) -> Result<Self> {
        let mut array = Self::with_capacity_from_iter(geoms.iter().map(Some), typ)?;
        array.extend_from_iter(geoms.iter().map(Some));
        Ok(array)
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_geometry_collections(
        geoms: &[Option<impl GeometryCollectionTrait<T = f64>>],
        typ: GeometryCollectionType,
    ) -> Result<Self> {
        let mut array = Self::with_capacity_from_iter(geoms.iter().map(|x| x.as_ref()), typ)?;
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        Ok(array)
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_geometries(
        geoms: &[impl GeometryTrait<T = f64>],
        typ: GeometryCollectionType,
    ) -> Result<Self> {
        let capacity = GeometryCollectionCapacity::from_geometries(geoms.iter().map(Some))?;
        let mut array = Self::with_capacity(typ, capacity);
        for geom in geoms {
            array.push_geometry(Some(geom))?;
        }
        Ok(array)
    }

    /// Construct a new builder, pre-filling it with the provided geometries
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        typ: GeometryCollectionType,
    ) -> Result<Self> {
        let capacity =
            GeometryCollectionCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()))?;
        let mut array = Self::with_capacity(typ, capacity);
        for geom in geoms {
            array.push_geometry(geom.as_ref())?;
        }
        Ok(array)
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WkbArray<O>, GeometryCollectionType)>
    for GeometryCollectionBuilder
{
    type Error = GeoArrowError;

    fn try_from((value, typ): (WkbArray<O>, GeometryCollectionType)) -> Result<Self> {
        let wkb_objects = value
            .iter()
            .map(|x| x.transpose())
            .collect::<Result<Vec<_>>>()?;
        Self::from_nullable_geometries(&wkb_objects, typ)
    }
}

impl GeometryArrayBuilder for GeometryCollectionBuilder {
    fn len(&self) -> usize {
        self.geom_offsets.len_proxy()
    }

    fn push_null(&mut self) {
        self.push_null();
    }
}
