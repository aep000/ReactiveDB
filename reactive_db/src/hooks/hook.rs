use crate::{actions::workspace::Workspace, database::{db_trait::DB}, types::CommitedEdit};
use crate::types::DBEdit;


#[derive(Clone, Ord, Eq, PartialOrd, PartialEq, Debug, Hash)]
pub enum Event {
    PreInsert(Option<String>),
    PostInsert(Option<String>),
    PreDelete,
    PostDelete
}
pub trait Hook {
    fn execute(&mut self, event: Event, proposed_edits:Option<Vec<DBEdit>>, commited_edits: Option<Vec<CommitedEdit>>, db: &mut dyn DB, workspace: Workspace) -> Option<Vec<DBEdit>>;
    fn get_events(&self) -> Vec<Event>;
    fn get_table(&self) -> String;
}

pub trait ObserverHook {
    fn execute(&mut self, event: Event);
}

pub trait EditorHook {
    fn execute(&mut self, event: Event) -> Vec<DBEdit>;
}