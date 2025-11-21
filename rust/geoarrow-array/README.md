# geoarrow-array

GeoArrow array definitions.

The central type in Apache Arrow are arrays, which are a known-length sequence of values all having the same type. This crate provides concrete implementations of each type defined in the [GeoArrow specification], as well as a [GeoArrowArray] trait that can be used for type-erasure.

[GeoArrow specification]: https://github.com/geoarrow/geoarrow

In order to minimize overhead of dynamic downcasting, the array types in this crate are defined "natively" and there's a `O(1)` conversion process that needs to happen to convert between a GeoArrow array type and an [`arrow`][arrow_array] array type.

## Building a GeoArrow Array

Use [builders][builder] to construct GeoArrow arrays. These builders offer a push-based interface to construct arrays from a series of objects that implement [`geo-traits`][geo_traits].

```rust
# use geo_traits::{CoordTrait, PointTrait};
# use geoarrow_array::array::PointArray;
# use geoarrow_array::builder::PointBuilder;
# use geoarrow_array::scalar::Point;
# use geoarrow_array::GeoArrowArrayAccessor;
# use geoarrow_schema::{Dimension, PointType};
#
let point_type = PointType::new(Dimension::XY, Default::default());
let mut builder = PointBuilder::new(point_type);

builder.push_point(Some(&geo_types::point!(x: 0., y: 1.)));
builder.push_point(Some(&geo_types::point!(x: 2., y: 3.)));
builder.push_point(Some(&geo_types::point!(x: 4., y: 5.)));

let array: PointArray = builder.finish();

let point_0: Point<'_> = array.get(0).unwrap().unwrap();
assert_eq!(point_0.coord().unwrap().x_y(), (0., 1.));
```

Converting a builder to an array via `finish()` is always `O(1)`.

## Converting to and from [`arrow`][arrow_array] Arrays

The `geoarrow` crates depend on and are designed to be used in combination with the upstream [Arrow][arrow_array] crates. As such, we have easy integration to convert between representations of each crate.

Note that an [`Array`] or [`ArrayRef`] only maintains information about the physical [`DataType`] and will lose any extension type information. Because of this, it's **imperative to store an [`Array`] and [`Field`] together** since the [`Field`] persists the Arrow [extension metadata]. A [`RecordBatch`] holds an [`Array`] and [`Field`] together for each column, so a [`RecordBatch`] will persist extension metadata.

### Converting to GeoArrow Arrays

If you have an [`Array`] and [`Field`] but don't know the geometry type of the array, you can use [`from_arrow_array`][array::from_arrow_array]:

```rust
# use std::sync::Arc;
#
# use arrow_array::Array;
# use arrow_schema::Field;
# use geoarrow_array::array::{from_arrow_array, PointArray};
# use geoarrow_array::cast::AsGeoArrowArray;
# use geoarrow_array::GeoArrowArray;
# use geoarrow_schema::GeoArrowType;
#
fn use_from_arrow_array(array: &dyn Array, field: &Field) {
    let geoarrow_array: Arc<dyn GeoArrowArray> = from_arrow_array(array, field).unwrap().unwrap();
    match geoarrow_array.data_type() {
        GeoArrowType::Point(_) => {
            let array: &PointArray = geoarrow_array.as_point();
        }
        _ => todo!("handle other geometry types"),
    }
}
```

If you know the geometry type of your array, you can use one of its `TryFrom` implementations to convert directly to that type. This means you don't have to downcast on the GeoArrow side from an `Arc<dyn GeoArrowArray>`.

```rust
# use arrow_array::Array;
# use arrow_schema::Field;
# use geoarrow_array::array::PointArray;
#
fn convert_to_point_array(array: &dyn Array, field: &Field) {
    let point_array = PointArray::try_from((array, field)).unwrap();
}
```

### Converting to [arrow][arrow_array] Arrays

You can use the [`to_array_ref`][GeoArrowArray::to_array_ref] or [`into_array_ref`][GeoArrowArray::into_array_ref] methods on [`GeoArrowArray`] to convert to an [`ArrayRef`].

Alternatively, if you have a concrete GeoArrow array type, you can use [`IntoArray`][crate::IntoArrow] to convert to a concrete arrow array type.

The easiest way today to access an arrow [`Field`] is to use [`IntoArray::extension_type`][crate::IntoArrow::extension_type] and then call `to_field` on the result. We like to make this process simpler in the future.

## Downcasting a GeoArrow array

Arrays are often passed around as a dynamically typed `&dyn GeoArrowArray` or [`Arc<dyn GeoArrowArray>`][GeoArrowArray].

While these arrays can be passed directly to compute functions, it is often the case that you wish to interact with the concrete arrays directly.

This requires downcasting to the concrete type of the array. Use the [`cast::AsGeoArrowArray`] extension trait to do this ergonomically.

```rust
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArrayAccessor, GeoArrowArray};

fn iter_line_string_array(array: &dyn GeoArrowArray) {
    for row in array.as_line_string().iter() {
        // do something with each row
    }
}
```

[`Array`]: arrow_array::Array
[`ArrayRef`]: arrow_array::ArrayRef
[`DataType`]: arrow_schema::DataType
[`Field`]: arrow_schema::Field
[`RecordBatch`]: arrow_array::RecordBatch
[extension metadata]: https://arrow.apache.org/docs/format/Columnar.html#format-metadata-extension-types
