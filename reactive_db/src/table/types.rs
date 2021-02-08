use crate::{hooks::transforms::Transform, types::DataType};


#[derive(Clone, Ord, Eq, PartialOrd, PartialEq)]
pub struct Column {
    pub data_type: DataType,
    pub name: String,
    pub indexed: bool,
    pub index_loc: usize,
}
#[derive(Clone, Ord, Eq, PartialOrd, PartialEq)]
pub enum TableType {
    Source,
    Derived(Transform),
}


impl Column {
    pub fn new(name: String, data_type: DataType) -> Column {
        Column {
            data_type: data_type,
            name: name,
            indexed: false,
            index_loc: 0,
        }
    }
}