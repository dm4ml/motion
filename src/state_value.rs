// This file is not used yet. It is a placeholder for a future feature.

use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyType};

#[pyclass(subclass)]
pub struct StateValue;

#[pymethods]
impl StateValue {
    #[new]
    pub fn new() -> Self {
        StateValue {}
    }

    #[classmethod]
    pub fn load(_cls: &PyType, _data: &PyBytes) -> PyResult<()> {
        Err(PyNotImplementedError::new_err(
            "The 'load' method has not been implemented.",
        ))
    }

    pub fn save(&self, _py: Python) -> PyResult<&PyBytes> {
        Err(PyNotImplementedError::new_err(
            "The 'save' method has not been implemented.",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_implemented() {
        pyo3::Python::with_gil(|py| {
            let state_object = py.get_type::<StateValue>();
            let result = state_object.call_method1("load", ("some_data",));
            assert!(result.is_err());

            let obj = state_object.call0().unwrap();
            let result = obj.call_method0("save");
            assert!(result.is_err());
        });
    }
}