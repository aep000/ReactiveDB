use std::collections::HashMap;

use crate::{actions::workspace::Workspace, hooks::hook::Hook, types::{CommitedEdit, Entry, EntryValue}};


pub trait DB {
    fn delete_all(
        &mut self,
        table: &str,
        column: String,
        key: EntryValue,
        hooks: &mut HookMap,
        workspace: &Workspace
    ) -> Result<Vec<CommitedEdit>, String>;

    fn insert_entry(
        &mut self,
        table: &str,
        entry: Entry,
        source_table: Option<&str>,
        hooks: &mut HookMap,
        workspace: &Workspace
    ) -> Result<Vec<CommitedEdit>, String>;

    fn find_one(
        &mut self,
        table: &str,
        column: String,
        key: EntryValue
    ) -> Result<Option<Entry>, String>;

    fn less_than_search(
        &mut self,
        table: &String,
        column: String,
        key: EntryValue,
    ) -> Result<Vec<Entry>, String>;

    fn get_all(
        &mut self,
        table: &String,
        column: String,
        key: EntryValue,
    ) -> Result<Vec<Entry>, String>;

    fn greater_than_search(
        &mut self,
        table: &str,
        column: String,
        key: EntryValue,
    ) -> Result<Vec<Entry>, String>;

    fn get_output_tables(&mut self, table_name: &str) -> Vec<String>;
}

pub type HookMap = HashMap<String, Vec<Box<dyn Hook>>>;
