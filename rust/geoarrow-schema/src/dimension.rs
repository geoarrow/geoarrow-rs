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
            geo_traits::Dimensions::Xyzm | geo_traits::Dimensions::Unknown(4) => {
                Ok(Dimension::XYZM)
            }
            _ => Err(ArrowError::SchemaError(format!(
                "Unsupported dimension {:?}",
                value
            ))),
        }
    }
}

#[cfg(test)]
mod test {
    use std::iter::zip;

    use arrow_schema::DataType;

    use super::*;

    #[test]
    fn from_interleaved() {
        assert!(matches!(
            Dimension::from_interleaved_field(&Field::new("xy", DataType::Null, false)),
            Dimension::XY
        ));

        assert!(matches!(
            Dimension::from_interleaved_field(&Field::new("xyz", DataType::Null, false)),
            Dimension::XYZ
        ));

        assert!(matches!(
            Dimension::from_interleaved_field(&Field::new("xym", DataType::Null, false)),
            Dimension::XYM
        ));

        assert!(matches!(
            Dimension::from_interleaved_field(&Field::new("xyzm", DataType::Null, false)),
            Dimension::XYZM
        ));
    }

    #[test]
    #[should_panic(expected = "Invalid interleaved field name: banana")]
    fn from_bad_interleaved() {
        Dimension::from_interleaved_field(&Field::new("banana", DataType::Null, false));
    }

    fn test_fields(dims: &[&str]) -> Fields {
        dims.iter()
            .map(|dim| Field::new(*dim, DataType::Null, false))
            .collect()
    }

    #[test]
    fn from_separated() {
        assert!(matches!(
            Dimension::from_separated_field(&test_fields(&["x", "y"])),
            Dimension::XY
        ));

        assert!(matches!(
            Dimension::from_separated_field(&test_fields(&["x", "y", "z"])),
            Dimension::XYZ
        ));

        assert!(matches!(
            Dimension::from_separated_field(&test_fields(&["x", "y", "m"])),
            Dimension::XYM
        ));

        assert!(matches!(
            Dimension::from_separated_field(&test_fields(&["x", "y", "z", "m"])),
            Dimension::XYZM
        ));
    }

    #[test]
    #[should_panic(expected = "Invalid number of fields for separated coordinates: 1")]
    fn from_bad_separated() {
        Dimension::from_separated_field(&test_fields(&["x"]));
    }

    #[test]
    fn geotraits_dimensions() {
        let geoarrow_dims = [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ];
        let geotraits_dims = [
            geo_traits::Dimensions::Xy,
            geo_traits::Dimensions::Xyz,
            geo_traits::Dimensions::Xym,
            geo_traits::Dimensions::Xyzm,
        ];

        for (geoarrow_dim, geotraits_dim) in zip(geoarrow_dims, geotraits_dims) {
            let into_geotraits_dim: geo_traits::Dimensions = geoarrow_dim.into();
            assert_eq!(into_geotraits_dim, geotraits_dim);

            let into_geoarrow_dim: Dimension = geotraits_dim.try_into().unwrap();
            assert_eq!(into_geoarrow_dim, geoarrow_dim);

            assert_eq!(geoarrow_dim.size(), geotraits_dim.size());
        }

        let dims2: Dimension = geo_traits::Dimensions::Unknown(2).try_into().unwrap();
        assert_eq!(dims2, Dimension::XY);

        let dims3: Dimension = geo_traits::Dimensions::Unknown(3).try_into().unwrap();
        assert_eq!(dims3, Dimension::XYZ);

        let dims4: Dimension = geo_traits::Dimensions::Unknown(4).try_into().unwrap();
        assert_eq!(dims4, Dimension::XYZM);

        let dims_err: Result<Dimension, ArrowError> = geo_traits::Dimensions::Unknown(0).try_into();
        assert_eq!(
            dims_err.unwrap_err().to_string(),
            "Schema error: Unsupported dimension Unknown(0)"
        );
    }
}
