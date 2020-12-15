use crate::object::property::Property;

pub(crate) struct ObjectInfo {
    pub properties: Vec<Property>,
    pub static_size: usize,
    pub first_dynamic_property_index: Option<usize>,
}

impl ObjectInfo {
    pub fn new(properties: Vec<Property>) -> ObjectInfo {
        let static_size = Self::calculate_static_size(&properties);
        let first_dynamic_property_index = Self::find_first_dynamic_property_index(&properties);
        ObjectInfo {
            properties,
            static_size,
            first_dynamic_property_index,
        }
    }

    fn calculate_static_size(properties: &[Property]) -> usize {
        let last_property = properties.last().unwrap();
        last_property.offset + last_property.data_type.get_static_size()
    }

    fn find_first_dynamic_property_index(properties: &[Property]) -> Option<usize> {
        properties
            .iter()
            .enumerate()
            .filter(|(_, property)| property.data_type.is_dynamic())
            .map(|(i, _)| i)
            .next()
    }

    /*pub fn verify_object(&self, object: &[u8]) -> bool {
        if let Some(first_dynamic_index) = self.first_dynamic_property_index {
            if object.len() < self.static_size {
                return false;
            }

            let mut dynamic_offset = self.static_size;
            for property in self.properties.iter().skip(first_dynamic_index) {
                if !property.is_null(object) {
                    let offset = property.get_data_offset(object);
                    if offset != dynamic_offset {
                        return false;
                    }

                    let length = property.get_length(object);
                    dynamic_offset += length;
                }
            }

            object.len() == dynamic_offset
        } else {
            object.len() == self.static_size
        }
    }*/
}
#[cfg(test)]
mod tests {
    use crate::object::data_type::DataType;
    use crate::object::object_info::ObjectInfo;
    use crate::object::property::Property;

    #[test]
    fn test_calculate_static_size() {
        let properties1 = vec![
            Property::new(DataType::Bool, 0),
            Property::new(DataType::Int, 2),
        ];
        let properties2 = vec![
            Property::new(DataType::Bool, 0),
            Property::new(DataType::String, 1),
            Property::new(DataType::Bytes, 9),
            Property::new(DataType::Double, 9),
        ];

        assert_eq!(ObjectInfo::calculate_static_size(&properties1), 6);
        assert_eq!(ObjectInfo::calculate_static_size(&properties2), 17);
    }

    #[test]
    fn test_find_first_dynamic_property_index() {
        let static_properties = vec![
            Property::new(DataType::Bool, 0),
            Property::new(DataType::Int, 1),
        ];
        let mixed_properties = vec![
            Property::new(DataType::Bool, 0),
            Property::new(DataType::String, 1),
        ];
        let dynamic_properties = vec![Property::new(DataType::String, 0)];

        assert_eq!(
            ObjectInfo::find_first_dynamic_property_index(&static_properties),
            None
        );
        assert_eq!(
            ObjectInfo::find_first_dynamic_property_index(&mixed_properties),
            Some(1)
        );
        assert_eq!(
            ObjectInfo::find_first_dynamic_property_index(&dynamic_properties),
            Some(0)
        );
    }

    /*
    #[test]
    fn test_verify_object() {
        let static_properties = vec![Property::new(DataType::Bool, 0), Property::new(DataType::Int, 1)];
        let string_property = vec![Property::new(DataType::String, 0)];

        let mixed_properties = vec![
            Property::new(DataType::Bool, 0),
            Property::new(DataType::String, 1),
            Property::new(DataType::Bytes, 9),
        ];

        fn col(properties: &[Property]) -> IsarCollection {
            IsarCollection::new(0, properties.to_vec(), vec![], vec![], DUMMY_DB)
        }

        assert_eq!(col(&static_properties).verify_object(&[]), false);
        assert_eq!(col(&static_properties).verify_object(&[1, 4]), false);
        assert_eq!(col(&static_properties).verify_object(&[0; 9]), true);
        assert_eq!(col(&static_properties).verify_object(&[0; 10]), false);

        assert_eq!(col(&string_property).verify_object(&[]), false);
        assert_eq!(col(&string_property).verify_object(&[0; 8]), true);
        assert_eq!(col(&string_property).verify_object(&[0; 9]), false);
        assert_eq!(
            col(&string_property).verify_object(&[8, 0, 0, 0, 3, 0, 0, 0, 60, 61, 62]),
            true
        );
        assert_eq!(
            col(&string_property).verify_object(&[1, 0, 0, 0, 3, 0, 0, 0, 60, 61, 62]),
            false
        );
        assert_eq!(
            col(&string_property).verify_object(&[9, 0, 0, 0, 1, 0, 0, 0, 60, 61]),
            false
        );

        assert_eq!(col(&mixed_properties).verify_object(&[]), false);
        assert_eq!(col(&mixed_properties).verify_object(&[0; 17]), true);
        assert_eq!(col(&mixed_properties).verify_object(&[0; 18]), false);
        assert_eq!(
            col(&mixed_properties).verify_object(&[
                2, 17, 0, 0, 0, 1, 0, 0, 0, 18, 0, 0, 0, 3, 0, 0, 0, 63, 60, 61, 62
            ]),
            true
        );
        assert_eq!(
            col(&mixed_properties).verify_object(&[
                2, 17, 0, 0, 0, 1, 0, 0, 0, 18, 0, 0, 0, 3, 0, 0, 0, 63, 60, 61, 62, 63
            ]),
            false
        );
        assert_eq!(
            col(&mixed_properties).verify_object(&[
                2, 17, 0, 0, 0, 1, 0, 0, 0, 17, 0, 0, 0, 3, 0, 0, 0, 63, 60, 61, 62
            ]),
            false
        );
    }
     */
}
