use arrow_schema::extension::ExtensionType;

use crate::Metadata;

pub trait GeoArrowExtensionType: ExtensionType {
    fn metadata(&self) -> &Metadata;
}
