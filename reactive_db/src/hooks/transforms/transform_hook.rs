use crate::{actions::workspace::Workspace, types::CommitedEdit};
use crate::types::{DBEdit, EditType};
use crate::constants::SOURCE_ENTRY_ID;
use crate::constants::ROW_ID_COLUMN_NAME;
use super::Transform;
use crate::Database;
use crate::hooks::hook::{Event, Hook};

pub struct TransformHook {
    transform: Transform,
    table: String,  
}

impl TransformHook {
    pub fn new(transform: Transform, table: String) -> TransformHook {
        return TransformHook {
            transform,
            table,
        }
    }

    fn get_output_tables(&self, table: &String, db: &mut Database) -> Vec<String>{
        db.tables.get(table).unwrap().output_tables.clone()
    }

}

impl Hook for TransformHook {
    fn execute(&mut self, event: Event, proposed_edits:Option<Vec<DBEdit>>, commited_edits: Option<Vec<CommitedEdit>>, db: &mut Database, workspace: Workspace) -> Option<Vec<DBEdit>> {
        let mut output = vec![];
        match event {
            // Handle Current Edit
            Event::PreInsert(source_table) => {
                let unwrapped_edits = proposed_edits.unwrap();
                for edit in unwrapped_edits {
                    let entry = match edit.edit_params {
                        EditType::Insert(entry) => entry,
                        EditType::Delete(_,_) => panic!("Error on transform: delete edit applied on insert"),
                        _ => panic!("Recieved Update During Insert"),

                    };
                    let transformed_entry = self.transform.execute(entry, &self.table, db, source_table.as_ref(), workspace.clone());
                    match transformed_entry {
                        Some(edit) => {
                            output.push(edit);
                        },
                        None => {}
                    };
                }
            },
            //Handle Down Stream Edits
            Event::PostInsert(_) => {
                let downstream_tables = self.get_output_tables(&self.table, db);
                for commited_edit in commited_edits.unwrap() {
                    for d_s_table in downstream_tables.clone() {
                        if commited_edit.table == self.table {
                            output.push(DBEdit::new(d_s_table, EditType::Insert(commited_edit.entry.clone())));
                        }
                    }
                }
            },
            Event::PostDelete => {
                let output_tables = self.get_output_tables(&self.table, db);
                for output_table in output_tables {
                    for edit in commited_edits.clone().unwrap() {
                        let id = edit.entry.get(ROW_ID_COLUMN_NAME).unwrap().clone();
                        output.push(DBEdit::new(output_table.clone(), EditType::Delete(SOURCE_ENTRY_ID.to_string(), id)))
                    }
                }
            }
            Event::PreDelete => {
                return proposed_edits;
            }
        }

        return Some(output);
    }
    fn get_events(&self) -> Vec<Event> { vec![Event::PreInsert(None), Event::PostDelete] }
    fn get_table(&self) -> String { self.table.clone() }
}