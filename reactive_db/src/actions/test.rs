
#[cfg(test)]
mod tests {
    use cpython::{ObjectProtocol, PyDict, PyResult, Python};
    use reactivedb_rust_client::types::{Entry, EntryBuilder, EntryValue};

    use crate::actions::{Action, workspace::Workspace};

    #[test]
    fn test_action() -> Result<(), String> {
        let action = Action::new("test_action".to_string(), "test".to_string());
        let entry = EntryBuilder::new().column("grade", EntryValue::Integer(12)).build();
        let new_entry: Entry = action.serde_run_no_client(entry, Workspace::new("./workspace/".to_string()).get_absolute_workspace_path()?)?;
        assert_eq!(new_entry, EntryBuilder::new().column("grade", EntryValue::Integer(2)).build());
        return Ok(());
    }
}