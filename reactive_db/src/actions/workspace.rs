use std::{path::Path};

#[derive(Debug, Clone)]
pub struct Workspace {
    workspace_path: String
}

impl Workspace {
    pub fn new(workspace_path: String) -> Workspace {
        Workspace {
            workspace_path: workspace_path
        }
    }

    pub fn get_absolute_workspace_path(&self) -> Result<String, String>{
        let path_obj = Path::new(self.workspace_path.as_str());
        let path_buf = path_obj
            .canonicalize()
            .map_err(
                |e| {
                    format!("Error getting absolute path for given workspace path: {}", e)
                }
            )?;

        Ok(path_buf.to_str().unwrap().to_string())
    }
}