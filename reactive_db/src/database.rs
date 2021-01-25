use crate::{actions::workspace::Workspace, types::CommitedEdit};
use crate::hooks::hook::{Hook, Event};
use crate::types::{Entry, DBEdit, EditType};
use crate::EntryValue;
use crate::Table;
use std::collections::HashMap;


pub type HookMap = HashMap<String, Vec<Box<dyn Hook>>>;

pub struct Database{
    pub tables: HashMap<String, Table>,
}

impl Database {

    pub fn new(
        tables: HashMap<String, Table>,
    ) -> Database {
        return Database {
            tables,
        }
    }

    pub fn delete_all(
        &mut self,
        table: &str,
        column: String,
        key: EntryValue,
        hooks: &mut HookMap,
        workspace: &Workspace
    ) -> Result<Vec<CommitedEdit>, String> {

        let edit = DBEdit::new(table.to_owned(), EditType::Delete(column, key));
        let new_edits = self.execute_hooks(table, Event::PreDelete, Some(vec![edit]), None, hooks, workspace);
        let (current_table_edits, other_edits) = split_vec(new_edits, |db_edit|->bool {
            db_edit.table.eq(table)
        });
        let mut commited_edits = self.execute_edits(other_edits, Some(table), hooks, workspace)?;

        let table_obj = match self.tables.get_mut(table) {
            Some(t) => t,
            None => Err(format!("Unable to find table {}", table))?,
        };
        for current_table_delete in current_table_edits {
            let (column_to_match, value_to_match) = match current_table_delete.edit_params {
                EditType::Delete(column, val) => (column, val),
                EditType::Insert(_) => panic!("Recieved Insert During Delete"),
                _ => panic!("Recieved Update During Insert"),
            };
            match table_obj.delete(column_to_match, &value_to_match) {
                Ok(deleted) => {
                    commited_edits.append(&mut (deleted.iter().map(|entry|{
                        CommitedEdit::new(table.to_owned(), entry.clone())
                    }).collect()))
                }
                Err(e) => Err(format!("Error when deleting for entries {}", e))?,
            };
        }

        let new_edits = self.execute_hooks(table, Event::PostDelete, None, Some(commited_edits.clone()), hooks, workspace);
        let mut additional_edits = self.execute_edits(new_edits, Some(table), hooks, workspace)?;
        commited_edits.append(&mut additional_edits);
        return Ok(commited_edits);
    }

    // TODO Abstract similar functionality to delete above
    pub fn insert_entry(
        &mut self,
        table: &str,
        entry: Entry,
        source_table: Option<&str>,
        hooks: &mut HookMap,
        workspace: &Workspace
    ) -> Result<Vec<CommitedEdit>, String> {
        let edit = DBEdit::new(table.to_owned(), EditType::Insert(entry));

        let new_edits = self.execute_hooks(table, Event::PreInsert(source_table.and_then(|e|{Some(e.to_owned())})), Some(vec![edit]), None, hooks, workspace);

        let (current_table_edits, other_edits) = split_vec(new_edits, |db_edit|->bool {
            db_edit.table.eq(table)
        });
        let mut commited_edits = self.execute_edits(other_edits, Some(table), hooks, workspace)?;


        let mut entries_to_insert = vec![];
        for current_table_edit in current_table_edits {
            entries_to_insert.push( match current_table_edit.edit_params {
                EditType::Insert(entry) => entry,
                EditType::Delete(_,_) => panic!("Recieved Delete During Insert"),
                EditType::Update(entry, column, value) => {
                    //panic!("Recieved Update During Insert");
                    // TODO Handle Unreported Deletes on edit
                    self.delete_all(table, column, value, hooks, workspace);
                    entry
                }
            });
        }

        match self.tables.get_mut(table) {
            Some(t) => {
                for entry_to_insert in entries_to_insert {
                    match t.insert(entry_to_insert) {
                        Ok(inserted_entry_results) => match inserted_entry_results {
                            Some(inserted_entry) =>{
                                commited_edits.push(CommitedEdit::new(table.to_owned(), inserted_entry))
                            }
                            None => {}
                        },
                        Err(e) => Err(format!("Error when inserting entry {}", e))?,
                    }
                }
            }
            None => Err(format!("Unable to find table {}", table))?,
        };

        let new_edits = self.execute_hooks(table, Event::PostInsert(None), None, Some(commited_edits.clone()), hooks, workspace);
        let mut additional_edits = self.execute_edits(new_edits, Some(table), hooks, workspace)?;
        commited_edits.append(&mut additional_edits);
        return Ok(commited_edits);
    }

    pub fn find_one(
        &mut self,
        table: &str,
        column: String,
        key: EntryValue
    ) -> Result<Option<Entry>, String> {
        let table_obj = match self.tables.get_mut(table) {
            Some(t) => t,
            None => Err(format!("Unable to find table {}", table))?,
        };
        match table_obj.find_one(column, &key) {
            Ok(r) => Ok(r),
            Err(e) => Err(format!("Error when searching for entry {}", e)),
        }
    }

    pub fn less_than_search(
        &mut self,
        table: &String,
        column: String,
        key: EntryValue,
    ) -> Result<Vec<Entry>, String> {
        let table_obj = match self.tables.get_mut(table) {
            Some(t) => t,
            None => Err(format!("Unable to find table {}", table))?,
        };
        match table_obj.less_than(column, key, false) {
            Ok(r) => Ok(r),
            Err(e) => Err(format!("Error when searching for entry {}", e)),
        }
    }

    pub fn get_all(
        &mut self,
        table: &String,
        column: String,
        key: EntryValue,
    ) -> Result<Vec<Entry>, String> {
        let table_obj = match self.tables.get_mut(table) {
            Some(t) => t,
            None => Err(format!("Unable to find table {}", table))?,
        };
        match table_obj.get_all(column, key) {
            Ok(r) => Ok(r),
            Err(e) => Err(format!("Error when searching for entry {:?}", e)),
        }
    }

    pub fn greater_than_search(
        &mut self,
        table: &str,
        column: String,
        key: EntryValue,
    ) -> Result<Vec<Entry>, String> {
        let table_obj = match self.tables.get_mut(table) {
            Some(t) => t,
            None => Err(format!("Unable to find table {}", table))?,
        };
        match table_obj.greater_than(column, key) {
            Ok(r) => Ok(r),
            Err(e) => Err(format!("Error when searching for entry {:?}", e)),
        }
    }

    fn execute_edits(&mut self, edits: Vec<DBEdit>, source_table: Option<&str>, hooks: &mut HookMap, workspace: &Workspace) -> Result<Vec<CommitedEdit>, String>{
        let mut changes = vec![];
        for edit in edits {
            let mut entries = match edit.edit_params {
                EditType::Insert(entry) => {
                    self.insert_entry(&edit.table, entry, source_table, hooks, workspace)?
                },
                EditType::Delete(column, value) => {
                    self.delete_all(&edit.table, column, value, hooks, workspace)?
                },
                EditType::Update(entry,column, value) => {
                    let mut edits = self.delete_all(&edit.table, column, value, hooks, workspace)?;
                    edits.append(&mut self.insert_entry(&edit.table, entry, source_table, hooks, workspace)?);
                    edits
                }

            };
            changes.append(&mut entries);
        }
        return Ok(changes);
    }

    //TODO: Filter non-supported hooks
    fn execute_hooks(&mut self, table: &str, event: Event, requested_edits: Option<Vec<DBEdit>>, commited_edits: Option<Vec<CommitedEdit>>, hooks: &mut HookMap, workspace: &Workspace) -> Vec<DBEdit>{
        let mut current_table_edits = requested_edits;
        let mut downstream_edits = vec![];
        for hook in hooks.get_mut(table).unwrap_or(&mut Vec::new()) {
            let new_edits = hook.execute(event.clone(), current_table_edits.clone(), commited_edits.clone(), self, workspace.clone());
            match new_edits {
                Some(edits) => {
                    let( c_t_edits, mut d_s_edits) = split_vec(edits, |db_edit|->bool {
                        db_edit.table.eq(table)
                    });
                    current_table_edits = Some(c_t_edits);
                    downstream_edits.append(&mut d_s_edits);

                }
                None => {}
            };
        }
        let mut unwrapped_final_edits = current_table_edits.unwrap_or(vec![]);
        unwrapped_final_edits.append(&mut downstream_edits);
        return unwrapped_final_edits;
    }
}

fn split_vec<T, F>(values: Vec<T>, predicate: F)->(Vec<T>, Vec<T>)
    where F: Fn(&T)->bool {
        let mut t_list = vec![];
        let mut f_list =vec![];
        for value in values{
            if predicate(&value){
                t_list.push(value);
            }
            else{
                f_list.push(value);
            }
        }
        return (t_list, f_list);
} 