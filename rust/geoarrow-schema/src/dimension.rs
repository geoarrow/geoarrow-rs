use std::collections::HashSet;

use arrow_schema::{ArrowError, Field, Fields};

/// The dimension of the geometry array.
///
/// [Dimension] implements [TryFrom] for integers:
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dimension {
    /// Two-dimensional.
    XY,

    /// Three-dimensional.
    XYZ,

    /// XYM (2D with measure).
    XYM,

    /// XYZM (3D with measure).
    XYZM,
}

impl Dimension {
    pub(crate) fn from_interleaved_field(field: &Field) -> Self {
        match field.name().as_str() {
            "xy" => Dimension::XY,
            "xyz" => Dimension::XYZ,
            "xym" => Dimension::XYM,
            "xyzm" => Dimension::XYZM,
            _ => panic!("Invalid interleaved field name: {}", field.name()),
        }
    }

    pub(crate) fn from_separated_field(fields: &Fields) -> Self {
        if fields.len() == 2 {
            Self::XY
        } else if fields.len() == 3 {
            let field_names: HashSet<&str> =
                HashSet::from_iter(fields.iter().map(|f| f.name().as_str()));
            if field_names.contains("m") {
                Self::XYM
            } else {
                Self::XYZ
            }
        } else if fields.len() == 4 {
            Self::XYZM
        } else {
            panic!(
                "Invalid number of fields for separated coordinates: {}",
                fields.len()
            );
        }
    }

    /// Returns the number of dimensions.
    pub fn size(&self) -> usize {
        match self {
            Dimension::XY => 2,
            Dimension::XYZ => 3,
            Dimension::XYM => 3,
            Dimension::XYZM => 4,
        }
    }
}

impl From<Dimension> for geo_traits::Dimensions {
    fn from(value: Dimension) -> Self {
        match value {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
            Dimension::XYM => geo_traits::Dimensions::Xym,
            Dimension::XYZM => geo_traits::Dimensions::Xyzm,
        }
    }
}

impl TryFrom<geo_traits::Dimensions> for Dimension {
    // TODO: switch to our own error
    type Error = ArrowError;

    fn try_from(value: geo_traits::Dimensions) -> std::result::Result<Self, Self::Error> {
        match value {
            geo_traits::Dimensions::Xy | geo_traits::Dimensions::Unknown(2) => Ok(Dimension::XY),
            geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Unknown(3) => Ok(Dimension::XYZ),
            geo_traits::Dimensions::Xym => Ok(Dimension::XYM),
            geo_traits::Dimensions::Xyzm => Ok(Dimension::XYZM),
            _ => Err(ArrowError::SchemaError(format!(
                "Unsupported dimension {:?}",
                value
            ))),
        }
    }
}
