use uuid::Uuid;
use crate::{actions::workspace::Workspace, table::{multi_source_table::MultiSourceTable, table_trait::Table}};
use crate::constants;
use crate::types::CommitedEdit;
use crate::table::storage_manager_table::StorageManagerTable;
use crate::hooks::hook::{Hook, Event};
use crate::types::{Entry, DBEdit, EditType};
use crate::EntryValue;
use std::collections::HashMap;

use super::db_trait::{DB, HookMap};

pub struct Database{
    pub tables: HashMap<String, MultiSourceTable>,
    transaction_manager: TransactionManager
}

impl DB for Database {
    fn delete_all(
        &mut self,
        table: &str,
        column: String,
        key: EntryValue,
        hooks: &mut HookMap,
        workspace: &Workspace
    ) -> Result<Vec<CommitedEdit>, String> {
        let transaction_id = self.transaction_manager.start_transaction();
        self.hidden_delete_all(table, column, key, hooks, workspace, transaction_id, true)
    }

    // TODO Abstract similar functionality to delete above
    fn insert_entry(
        &mut self,
        table: &str,
        entry: Entry,
        source_table: Option<&str>,
        hooks: &mut HookMap,
        workspace: &Workspace
    ) -> Result<Vec<CommitedEdit>, String> {
        let transaction_id = self.transaction_manager.start_transaction();
        self.hidden_insert(table, entry, source_table, hooks, workspace, transaction_id, true)
    }

    fn find_one(
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

    fn less_than_search(
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

    fn get_all(
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

    fn greater_than_search(
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

    fn get_output_tables(&mut self, table_name: &str) -> Vec<String> {
        let table = self.tables.get_mut(table_name).unwrap();
        table.get_output_tables().clone()
    }
}

impl Database {

    pub fn new(
        tables: HashMap<String, MultiSourceTable>,
    ) -> Database {
        return Database {
            tables,
            transaction_manager: TransactionManager::new()
        }
    }

    fn hidden_insert(
        &mut self,
        table: &str,
        entry: Entry,
        source_table: Option<&str>,
        hooks: &mut HookMap,
        workspace: &Workspace,
        transaction_id: Uuid,
        hooks_enabled: bool
    ) -> Result<Vec<CommitedEdit>, String> {
        let edit = DBEdit::new(table.to_owned(), EditType::Insert(entry));
        let mut commited_edits = vec![];
        let mut current_table_edits = vec![];
        if hooks_enabled {
            let new_edits = self.execute_hooks(table, Event::PreInsert(source_table.and_then(|e|{Some(e.to_owned())})), Some(vec![edit]), None, hooks, workspace);

            let (tmp_current_table_edits, other_edits) = split_vec(new_edits, |db_edit|->bool {
                db_edit.table.eq(table)
            });
            commited_edits = self.execute_edits(other_edits, Some(table), hooks, workspace, transaction_id, hooks_enabled)?;
            current_table_edits = tmp_current_table_edits;
        }
        else {
            current_table_edits.push(edit);
        }


        let mut entries_to_insert = vec![];
        for current_table_edit in current_table_edits {
            self.transaction_manager.add_pending_edit(transaction_id, current_table_edit.clone());
            entries_to_insert.push( match current_table_edit.edit_params {
                EditType::Insert(entry) => entry,
                EditType::Delete(_,_) => panic!("Recieved Delete During Insert"),
                EditType::Update(entry, column, value) => {
                    //panic!("Recieved Update During Insert");
                    // TODO Handle Unreported Deletes on edit
                    self.hidden_delete_all(table, column, value, hooks, workspace, transaction_id, hooks_enabled)?;
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
                                let entry_id = inserted_entry.get(constants::ROW_ID_COLUMN_NAME).unwrap().clone();
                                let invert_edit = DBEdit::new(table.to_owned(), EditType::Delete(constants::ROW_ID_COLUMN_NAME.to_string(), entry_id));
                                self.transaction_manager.edit_complete(transaction_id, vec![invert_edit]);
                                commited_edits.push(CommitedEdit::new(table.to_owned(), inserted_entry))
                            }
                            None => {}
                        },
                        Err(e) => {
                            self.walk_back_edits(hooks, workspace, transaction_id);
                            return Err(format!("Error when inserting entry {}", e));
                        },
                    }
                }
            }
            None => Err(format!("Unable to find table {}", table))?,
        };
        if hooks_enabled {
            let new_edits = self.execute_hooks(table, Event::PostInsert(None), None, Some(commited_edits.clone()), hooks, workspace);
            let mut additional_edits = self.execute_edits(new_edits, Some(table), hooks, workspace, transaction_id, hooks_enabled)?;
            commited_edits.append(&mut additional_edits);
        }
        return Ok(commited_edits);
    }

    fn hidden_delete_all(
        &mut self,
        table: &str,
        column: String,
        key: EntryValue,
        hooks: &mut HookMap,
        workspace: &Workspace,
        transaction_id: Uuid,
        hooks_enabled: bool
    ) -> Result<Vec<CommitedEdit>, String> {

        let edit = DBEdit::new(table.to_owned(), EditType::Delete(column, key));
        let mut commited_edits = vec![];
        let mut current_table_edits = vec![];
        if hooks_enabled {
            let new_edits = self.execute_hooks(table, Event::PreDelete, Some(vec![edit]), None, hooks, workspace);
            let (tmp_current_table_edits, other_edits) = split_vec(new_edits, |db_edit|->bool {
                db_edit.table.eq(table)
            });
            commited_edits = self.execute_edits(other_edits, Some(table), hooks, workspace, transaction_id, true)?;
            current_table_edits = tmp_current_table_edits;
        }
        else {
            current_table_edits.push(edit);
        }

        let table_obj = match self.tables.get_mut(table) {
            Some(t) => t,
            None => {
                self.walk_back_edits(hooks, workspace, transaction_id);
                return Err(format!("Unable to find table {}", table));
            },
        };
        for current_table_delete in current_table_edits {
            self.transaction_manager.add_pending_edit(transaction_id, current_table_delete.clone());
            let (column_to_match, value_to_match) = match current_table_delete.edit_params {
                EditType::Delete(column, val) => (column, val),
                EditType::Insert(_) => {
                    self.walk_back_edits(hooks, workspace, transaction_id);
                    panic!("Recieved Insert During Delete");
                },
                EditType::Update(_, _, _) => {
                    self.walk_back_edits(hooks, workspace, transaction_id);
                    panic!("Recieved Update During Insert")
                },
            };
            let mut invert_edits:Vec<DBEdit> = vec![];
            match table_obj.delete(column_to_match, &value_to_match) {
                Ok(deleted) => {
                    invert_edits = deleted.clone().iter().map(|deleted_entry| {
                        DBEdit::new(table.to_owned(), EditType::Insert(deleted_entry.to_owned()))
                    }).collect();
                    

                    commited_edits.append(&mut (deleted.iter().map(|entry|{
                        CommitedEdit::new(table.to_owned(), entry.clone())
                    }).collect()))
                }
                Err(e) => {
                    self.walk_back_edits(hooks, workspace, transaction_id);
                    return Err(format!("Error when deleting for entries {}", e));
                },
            };
            self.transaction_manager.edit_complete(transaction_id, invert_edits);
        }
        if hooks_enabled {
            let new_edits = self.execute_hooks(table, Event::PostDelete, None, Some(commited_edits.clone()), hooks, workspace);
            let mut additional_edits = self.execute_edits(new_edits, Some(table), hooks, workspace, transaction_id, true)?;
            commited_edits.append(&mut additional_edits);
        }


        return Ok(commited_edits);
    }

    fn execute_edits(&mut self, edits: Vec<DBEdit>, source_table: Option<&str>, hooks: &mut HookMap, workspace: &Workspace, transaction_id: Uuid, hooks_enabled: bool) -> Result<Vec<CommitedEdit>, String>{
        let mut changes = vec![];
        for edit in edits {
            let mut entries = match edit.edit_params {
                EditType::Insert(entry) => {
                    self.hidden_insert(&edit.table, entry, source_table, hooks, workspace, transaction_id, hooks_enabled)?
                },
                EditType::Delete(column, value) => {
                    self.hidden_delete_all(&edit.table, column, value, hooks, workspace, transaction_id, hooks_enabled)?
                },
                EditType::Update(entry,column, value) => {
                    let mut edits = self.hidden_delete_all(&edit.table, column, value, hooks, workspace, transaction_id, hooks_enabled)?;
                    edits.append(&mut self.hidden_insert(&edit.table, entry, source_table, hooks, workspace, transaction_id, hooks_enabled)?);
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
                    let( c_t_edits, mut d_s_edits) = split_vec(edits, |db_edit: &DBEdit|->bool {
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

    fn walk_back_edits(
        &mut self, 
        hooks: &mut HookMap,
        workspace: &Workspace,
        transaction_id: Uuid,
    ){
        let invert = self.transaction_manager.get_invert_of_transaction(transaction_id);
        if let Some(fix_edits) = invert {
            self.execute_edits(fix_edits, None, hooks, workspace, transaction_id, false).unwrap();
        }
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

struct TransactionManager {
    edits_left: HashMap<Uuid, (Vec<DBEdit>, usize)>,
    invert_edits: HashMap<Uuid, Vec<DBEdit>>
}

// TODO Make this part of the graceful shutdown
impl TransactionManager {
    pub fn new() -> TransactionManager {
        TransactionManager {
            edits_left: HashMap::new(),
            invert_edits: HashMap::new()
        }
    }

    pub fn add_pending_edit(&mut self, transaction_id: Uuid, edit: DBEdit) {
        if let Some((edit_list, number_uncommited)) = self.edits_left.get_mut(&transaction_id){
            edit_list.push(edit);
            *number_uncommited += 1;
        }
    }

    pub fn edit_complete(&mut self, transaction_id: Uuid, mut invert_edit: Vec<DBEdit>) {
        let mut delete = false;
        if let Some((_, number_uncommited)) = self.edits_left.get_mut(&transaction_id){
            *number_uncommited -= 1;
            if *number_uncommited == 0 {
                delete = true;
            }
        }
        if delete {
            self.edits_left.remove(&transaction_id);
            self.invert_edits.remove(&transaction_id);
        }
        else {
            if let Some(invert_edits) = self.invert_edits.get_mut(&transaction_id){
                invert_edits.append(&mut invert_edit);
            }
        }
    }

    pub fn get_invert_of_transaction(&mut self, transaction_id: Uuid) -> Option<Vec<DBEdit>> {
        self.edits_left.remove(&transaction_id);
        self.invert_edits.remove(&transaction_id)
    }

    pub fn start_transaction(&mut self) -> Uuid {
        let id = Uuid::new_v4();
        self.edits_left.insert(id.clone(), (vec![], 0));
        self.invert_edits.insert(id.clone(), vec![]);
        id
    }

}