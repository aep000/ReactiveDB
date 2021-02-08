use std::io;

use crate::types::{Entry, EntryValue};

pub trait Table{
    fn insert(&mut self, entry: Entry) -> io::Result<Option<Entry>>;

    fn delete(
        &mut self,
        search_column_name: String,
        value: &EntryValue,
    ) -> io::Result<Vec<Entry>>;

    fn find_one(
        &mut self,
        search_column_name: String,
        value: &EntryValue,
    ) -> io::Result<Option<Entry>>;

    fn get_all(
        &mut self,
        search_column_name: String,
        value: EntryValue,
    ) -> io::Result<Vec<Entry>>;

    fn less_than(
        &mut self,
        search_column_name: String,
        value: EntryValue,
        equals: bool,
    ) -> io::Result<Vec<Entry>>;

    fn greater_than(
        &mut self,
        search_column_name: String,
        value: EntryValue,
    ) -> io::Result<Vec<Entry>>;

    fn get_output_tables(&mut self) -> &mut Vec<String>;

    fn get_input_tables(&mut self) -> &mut Vec<String>;
}