#![allow(deprecated)]

use crate::array::geometry::GeometryArray;
use crate::array::{CoordBuffer, InterleavedCoordBuffer, SeparatedCoordBuffer};
use crate::error::Result;
use crate::trait_::GeometryArraySelfMethods;
use arrow_array::OffsetSizeTrait;
use geodesy::prelude::*;
use geodesy::Coor4D;
use geodesy::Direction;

/// Wrapper object for applying coordinate operations slices
struct InterleavedCoordsGeodesy<'a>(&'a mut [f64]);

impl CoordinateSet for InterleavedCoordsGeodesy<'_> {
    fn len(&self) -> usize {
        self.0.len() / 2
    }

    fn get_coord(&self, index: usize) -> Coor4D {
        Coor4D([self.0[index * 2], self.0[index * 2 + 1], 0., 0.])
    }

    fn set_coord(&mut self, index: usize, value: &Coor4D) {
        let x = value[0];
        let y = value[1];
        self.0[index * 2] = x;
        self.0[index * 2 + 1] = y;
    }
}

/// Wrapper object for applying coordinate operations slices
struct SeparatedCoordsGeodesy<'a> {
    x: &'a mut [f64],
    y: &'a mut [f64],
}

impl CoordinateSet for SeparatedCoordsGeodesy<'_> {
    fn len(&self) -> usize {
        self.x.len()
    }

    fn get_coord(&self, index: usize) -> Coor4D {
        Coor4D([self.x[index], self.y[index], 0., 0.])
    }

    fn set_coord(&mut self, index: usize, value: &Coor4D) {
        self.x[index] = value[0];
        self.y[index] = value[1];
    }
}

fn reproject_coords(
    coords: &CoordBuffer,
    definition: &str,
    direction: Direction,
) -> Result<CoordBuffer> {
    let mut context = Minimal::new();
    // TODO: fix error handling
    let operation = context.op(definition).unwrap();

    let new_coords = match coords {
        CoordBuffer::Interleaved(coords) => {
            let mut cloned_coords = coords.coords.to_vec();

            let mut geodesy_coords = InterleavedCoordsGeodesy(&mut cloned_coords);
            context
                .apply(operation, direction, &mut geodesy_coords)
                .unwrap();

            CoordBuffer::Interleaved(InterleavedCoordBuffer::new(cloned_coords.into()))
        }
        CoordBuffer::Separated(separated_coords) => {
            let mut x_coords = separated_coords.x.to_vec();
            let mut y_coords = separated_coords.x.to_vec();

            let mut geodesy_coords = SeparatedCoordsGeodesy {
                x: &mut x_coords,
                y: &mut y_coords,
            };
            context
                .apply(operation, direction, &mut geodesy_coords)
                .unwrap();
            CoordBuffer::Separated(SeparatedCoordBuffer::new(x_coords.into(), y_coords.into()))
        }
    };

    Ok(new_coords)
}

/// Reproject coordinates
///

// NOTE: In the future this should probably take care to _not_ reproject coordinates that are set to null via the arrow validity bitmask. That could probably lead to
pub fn reproject<O: OffsetSizeTrait>(
    array: &GeometryArray<O>,
    definition: &str,
    direction: Direction,
) -> Result<GeometryArray<O>> {
    match array {
        GeometryArray::Point(arr) => {
            let new_coords = reproject_coords(&arr.coords, definition, direction)?;
            Ok(GeometryArray::Point(arr.clone().with_coords(new_coords)))
        }
        GeometryArray::LineString(arr) => {
            let new_coords = reproject_coords(&arr.coords, definition, direction)?;
            Ok(GeometryArray::LineString(
                arr.clone().with_coords(new_coords),
            ))
        }
        GeometryArray::Polygon(arr) => {
            let new_coords = reproject_coords(&arr.coords, definition, direction)?;
            Ok(GeometryArray::Polygon(arr.clone().with_coords(new_coords)))
        }
        GeometryArray::MultiPoint(arr) => {
            let new_coords = reproject_coords(&arr.coords, definition, direction)?;
            Ok(GeometryArray::MultiPoint(
                arr.clone().with_coords(new_coords),
            ))
        }
        GeometryArray::MultiLineString(arr) => {
            let new_coords = reproject_coords(&arr.coords, definition, direction)?;
            Ok(GeometryArray::MultiLineString(
                arr.clone().with_coords(new_coords),
            ))
        }
        GeometryArray::MultiPolygon(arr) => {
            let new_coords = reproject_coords(&arr.coords, definition, direction)?;
            Ok(GeometryArray::MultiPolygon(
                arr.clone().with_coords(new_coords),
            ))
        }
        GeometryArray::Rect(_arr) => todo!(),
    }
}
