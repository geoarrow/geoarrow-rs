//! Read and write geometries encoded as [Well-Known Text](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry).
//!
//! ## Example
//!
//! ```
//! use std::sync::Arc;
//!
//! use arrow_array::StringArray;
//!
//! use geoarrow::array::metadata::ArrayMetadata;
//! use geoarrow::array::{AsNativeArray, CoordType, GeometryArray, WKTArray};
//! use geoarrow::datatypes::NativeType;
//! use geoarrow::io::wkt::{read_wkt, ToWKT};
//! use geoarrow::trait_::ArrayAccessor;
//! use geoarrow::NativeArray;
//!
//! // Start with some WKT data
//! let wkt_strings = vec![
//!     "POINT(30 10)",
//!     "LINESTRING(30 10, 10 30, 40 40)",
//!     "POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))",
//! ];
//!
//! // Construct an Arrow StringArray from this data
//! let arrow_arr = StringArray::from_iter_values(wkt_strings);
//!
//! // GeoArrow has a `WKTArray` concept in order to associate geospatial metadata with WKT data.
//! // Here, we associate CRS information with the WKT array, which will be maintained in the
//! // parsed representation.
//! let array_metadata = Arc::new(ArrayMetadata::from_authority_code("EPSG:4326".to_string()));
//! let wkt_array = WKTArray::new(arrow_arr, array_metadata);
//!
//! // Parse this WKT array to an `Arc<dyn NativeArray>`
//! let geometry_array: Arc<dyn NativeArray> =
//!     read_wkt(&wkt_array, CoordType::Separated, false).unwrap();
//!
//! // All parsed WKT data currently has `NativeType::Geometry`, because there's no way to know in
//! // advance what the geometry type of the WKT is.
//! assert!(matches!(
//!     geometry_array.data_type(),
//!     NativeType::Geometry(CoordType::Separated)
//! ));
//!
//! // Now we can downcast the dynamic reference to a concrete `GeometryArray`, and access a value
//! // as a `geo::Geometry`
//! let geometry_array_ref = geometry_array.as_ref();
//! let downcasted: &GeometryArray = geometry_array_ref.as_geometry();
//! matches!(
//!     downcasted.value_as_geo(0),
//!     geo::Geometry::Point(geo::Point(geo::Coord { x: 30.0, y: 10.0 }))
//! );
//!
//! // Then we can write back to WKT
//! let wkt_array_again: WKTArray<i32> = downcasted.as_ref().to_wkt().unwrap();
//! assert_eq!(wkt_array_again.into_inner().value(0), "POINT(30 10)")
//! ```

mod reader;
mod writer;

pub use reader::read_wkt;
pub use writer::ToWKT;
