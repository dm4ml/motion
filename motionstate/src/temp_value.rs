use pyo3::prelude::*;
use pyo3::PyObject;

#[pyclass]
pub struct TempValue {
    pub value: PyObject,
    pub ttl: u64,
}

#[pymethods]
impl TempValue {
    #[new]
    pub fn new(py: Python, value: PyObject, ttl: u64) -> Self {
        TempValue {
            value: value.into_py(py),
            ttl,
        }
    }

    #[getter]
    pub fn value(&self, py: Python) -> PyObject {
        self.value.clone_ref(py)
    }

    #[getter]
    pub fn ttl(&self) -> u64 {
        self.ttl
    }

    #[setter]
    pub fn set_ttl(&mut self, new_ttl: u64) {
        self.ttl = new_ttl;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::types::IntoPyDict;

    #[test]
    fn test_tempvalue_creation() {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let d = [("TempValue", py.get_type::<TempValue>())].into_py_dict(py);
        let instance: PyObject = py
            .eval("TempValue(value='hello', ttl=100)", Some(d), None)
            .unwrap()
            .extract()
            .unwrap();
        let tempvalue: &TempValue = instance.extract(py).unwrap();
        assert_eq!(tempvalue.ttl, 100);
    }
}
