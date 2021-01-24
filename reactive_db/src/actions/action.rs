use cpython::{FromPyObject, ObjectProtocol, PyObject, PyResult, Python, ToPyObject};
use serde::{Serialize, de::DeserializeOwned};

#[derive(Debug, Clone, Ord, Eq, PartialOrd, PartialEq)]
pub struct Action {
    file: String,
    function: String
}

#[allow(dead_code)]
impl Action {
    pub fn new(file: String, function: String) -> Action {
        Action {
            file,
            function
        }
    }

    pub fn run<R>(&self, request_body: R, workspace_dir: String) -> Result<(), String>
    where R: ToPyObject
    {
        let gil = Python::acquire_gil();
        // TODO handle python errors
        self.wrapped_run(gil.python(), request_body, workspace_dir, 1108).unwrap();

        return Ok(());
    }

    pub fn serde_run_no_client<I, R>(&self, value: I, workspace_dir: String) -> Result<R, String> 
        where I: Serialize+DeserializeOwned,
            R: Serialize+DeserializeOwned 
    {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let new_value = SerdeToPyObjectWrapper::new(value);
        let resulting_obj: SerdeToPyObjectWrapper<R> = self.no_client_wrapped_run(py, new_value.to_py_object(py), workspace_dir).unwrap().extract(py).unwrap();
        return Ok(resulting_obj.expose());
    }

    fn no_client_wrapped_run(&self, py: Python, request_body: PyObject, workspace_dir: String) -> PyResult<PyObject> {
        let sys = py.import("sys")?;
        sys.get(py, "path")?.call_method(py, "append", (workspace_dir.as_str(), ), None)?;
        
        let user_module = py.import(self.file.as_str())?;
        let function_to_call = user_module.get(py, self.function.as_str())?;

        function_to_call.call(py, (request_body,), None)
    }

    fn wrapped_run<R>(&self, py: Python, request_body: R, workspace_dir: String, port: usize) -> PyResult<()> 
    where R: ToPyObject
    {
        let sys = py.import("sys")?;
        sys.get(py, "path")?.call_method(py, "append", (workspace_dir.as_str(), ), None)?;
        // TODO Bundle this with the database
        let rdb_client_module = py.import("reactive_db_client")?;
        let client_class = rdb_client_module.get(py, "ClientSync")?;
        // TODO Make URL customizable
        let client_obj = client_class.call(py, ("127.0.0.1", port), None)?;
        
        let user_module = py.import(self.file.as_str())?;
        let function_to_call = user_module.get(py, self.function.as_str())?;
        //let builder = EntryBuilder::new().column("Key1", EntryValue::Str("Hello".to_string())).column("Key2", EntryValue::Decimal(Decimal::from_str("1.5"))).build();

        function_to_call.call(py, (request_body, client_obj), None)?;


        return Ok(());
    }
}

struct SerdeToPyObjectWrapper<T> where T: Serialize + DeserializeOwned, {
    obj: T
}

impl<T: Serialize + DeserializeOwned> SerdeToPyObjectWrapper<T> {
    pub fn new(obj: T) -> SerdeToPyObjectWrapper<T>{
        return SerdeToPyObjectWrapper {
            obj
        };
    }

    pub fn expose(self) -> T {
        return self.obj;
    }
}

impl<T: Serialize + DeserializeOwned> ToPyObject for SerdeToPyObjectWrapper<T> {
    type ObjectType = PyObject;

    fn to_py_object(&self, py: Python) -> Self::ObjectType {
        cpython::serde::to_py_object(py, &self.obj).unwrap()
    }
}

impl<'s, T: Serialize + DeserializeOwned> FromPyObject<'s> for SerdeToPyObjectWrapper<T> {
    fn extract(py: Python, obj: &'s PyObject) -> PyResult<Self> {
        let p: PyObject = obj.to_py_object(py);
        let inner: T = cpython::serde::from_py_object(py, p).unwrap();
        return Ok(SerdeToPyObjectWrapper {
            obj: inner
        });
    }
}
