pub mod state_value;
use state_value::StateValue;

use pyo3::exceptions;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString};
use redis::Commands;
use std::collections::HashMap;

/*
TODO:
* Increment version when calling set_bulk
* Use proper keys with component and instance names
* Construct redis_url out of redis params
 */

#[pyclass]
pub struct State {
    component_name: String,
    instance_id: String,
    client: redis::Client,
    cache: HashMap<String, Vec<u8>>,
}

#[pymethods]
impl State {
    #[new]
    pub fn new(component_name: String, instance_id: String, redis_url: &str) -> PyResult<Self> {
        let client = redis::Client::open(redis_url).map_err(|err| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Redis connection error: {}",
                err
            ))
        })?;
        Ok(State {
            component_name,
            instance_id,
            client,
            cache: HashMap::new(),
        })
    }

    pub fn set(&mut self, py: Python, key: &str, value: &PyAny) -> PyResult<()> {
        let mut con = self.client.get_connection().unwrap();
        let serialized_data = serialize_value(py, value)?;

        // Insert the key and value into the cache
        self.cache.insert(key.to_string(), serialized_data.clone());
        // Insert the key and value into Redis
        con.set(key, serialized_data).map_err(|err| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Redis set error: {}", err))
        })?;

        Ok(())
    }

    pub fn bulk_set(&mut self, py: Python, items: &PyDict) -> PyResult<()> {
        let mut con = self.client.get_connection().unwrap();
        let mut pipeline = redis::pipe();

        // Iterate over the items in the dictionary
        for (key, value) in items {
            let key_str = key.extract::<String>()?;
            let serialized_data = serialize_value(py, value)?;

            // Insert the key and value into the cache
            self.cache.insert(key_str.clone(), serialized_data.clone());
            // Insert the key and value into the pipeline
            //pipeline.set::<_, _, ()>(key_str, serialized_data);
            pipeline.cmd("SET").arg(key_str).arg(serialized_data);
        }

        // Execute the pipeline, throwing a Python error if it fails
        pipeline.query::<()>(&mut con).map_err(|err| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Redis bulk set error: {}",
                err
            ))
        })?;

        Ok(())
    }

    pub fn get(&mut self, py: Python, key: &str) -> PyResult<PyObject> {
        // If the key is in the cache, return it
        if let Some(value) = self.cache.get(key) {
            return deserialize_value(py, value);
        }

        // Otherwise, fetch it from Redis
        let mut con = self.client.get_connection().unwrap();
        let result_data: redis::RedisResult<Option<Vec<u8>>> = con.get(key);

        match result_data {
            Ok(Some(data)) => {
                // Insert the key and value into the cache
                self.cache.insert(key.to_string(), data.clone());
                // Deserialize the value
                deserialize_value(py, &data)
            }
            Ok(None) => Err(PyErr::new::<exceptions::PyKeyError, _>("Key not found")),
            Err(err) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Redis get error: {}",
                err
            ))),
        }
    }

    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }
}

// Serialization Helpers
const MARKER_LIST: u8 = 0x01;
const MARKER_DICT: u8 = 0x02;
const MARKER_STATE_VALUE: u8 = 0x03;

fn cloudpickle_serialize(py: Python, value: &PyAny) -> PyResult<Vec<u8>> {
    let cloudpickle = py.import("cloudpickle")?;
    let bytes = cloudpickle
        .getattr("dumps")?
        .call1((value,))?
        .extract::<&PyBytes>()?;
    Ok(bytes.as_bytes().to_vec())
}

fn cloudpickle_deserialize(py: Python, value: &[u8]) -> PyResult<PyObject> {
    let cloudpickle = py.import("cloudpickle")?;
    let bytes_value = PyBytes::new(py, value);
    let obj = cloudpickle.getattr("loads")?.call1((bytes_value,))?;
    Ok(obj.into())
}

fn serialize_value(py: Python, value: &PyAny) -> PyResult<Vec<u8>> {
    if value.is_instance_of::<PyInt>()
        || value.is_instance_of::<PyFloat>()
        || value.is_instance_of::<PyString>()
    {
        Ok(value.str()?.to_string().into_bytes())
    } else if value.is_instance_of::<PyDict>() {
        let mut serialized = vec![MARKER_DICT];
        serialized.extend(serialize_dict(py, value)?);
        Ok(serialized)
    } else if value.is_instance_of::<PyList>() {
        let list = value.downcast::<PyList>()?;
        let mut serialized = vec![MARKER_LIST];
        for item in list.iter() {
            let serialized_item = serialize_value(py, item)?;
            serialized.extend((serialized_item.len() as u64).to_le_bytes().iter());
            serialized.extend(serialized_item);
        }
        Ok(serialized)
    } else if value.is_instance_of::<StateValue>() {
        let mut serialized = vec![MARKER_STATE_VALUE];

        let saved_data = value.call_method0("save")?;

        // Check if the saved_data is bytes
        let bytes_data = saved_data.downcast::<PyBytes>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "'save' method must return a bytes object",
            )
        })?;

        serialized.extend(bytes_data.as_bytes());
        Ok(serialized)
    } else {
        cloudpickle_serialize(py, value)
    }
}

fn serialize_dict(py: Python, value: &PyAny) -> PyResult<Vec<u8>> {
    let dict = value.downcast::<PyDict>()?;
    let mut serialized = Vec::new();

    for (key, val) in dict {
        let key_bytes = serialize_value(py, key)?;
        let val_bytes = serialize_value(py, val)?;
        serialized.extend((key_bytes.len() as u64).to_le_bytes().iter());
        serialized.extend(key_bytes);
        serialized.extend((val_bytes.len() as u64).to_le_bytes().iter());
        serialized.extend(val_bytes);
    }

    Ok(serialized)
}

fn extract_next_slice<'a>(value: &'a [u8], cursor: &mut usize) -> Option<&'a [u8]> {
    if *cursor + 8 <= value.len() {
        let len = u64::from_le_bytes(value[*cursor..*cursor + 8].try_into().unwrap()) as usize;
        *cursor += 8;
        if *cursor + len <= value.len() {
            let result = &value[*cursor..*cursor + len];
            *cursor += len;
            return Some(result);
        }
    }
    None
}

fn deserialize_value(py: Python, value: &[u8]) -> PyResult<PyObject> {
    if value.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Empty data",
        ));
    }

    let mut cursor = 0;
    match value[cursor] {
        MARKER_STATE_VALUE => {
            cursor += 1;
            let state_value_data = &value[cursor..];
            let state_value_type = py.get_type::<StateValue>();
            let result = state_value_type.call_method1("load", (state_value_data,))?;
            Ok(result.into())
        }
        MARKER_LIST => {
            cursor += 1;
            let list = pyo3::types::PyList::empty(py);
            while let Some(item_bytes) = extract_next_slice(value, &mut cursor) {
                let item = deserialize_value(py, item_bytes)?;
                list.append(item)?;
            }
            Ok(list.into())
        }
        MARKER_DICT => {
            cursor += 1;
            let dict = PyDict::new(py);
            while let (Some(key_bytes), Some(val_bytes)) = (
                extract_next_slice(value, &mut cursor),
                extract_next_slice(value, &mut cursor),
            ) {
                let key = deserialize_value(py, key_bytes)?;
                let val = deserialize_value(py, val_bytes)?;
                dict.set_item(key, val)?;
            }
            Ok(dict.into())
        }
        _ => {
            if let Ok(decoded) = std::str::from_utf8(value) {
                if let Ok(int_value) = decoded.parse::<i64>() {
                    return Ok(int_value.into_py(py));
                } else if let Ok(float_value) = decoded.parse::<f64>() {
                    return Ok(float_value.into_py(py));
                }
                Ok(decoded.to_string().into_py(py))
            } else {
                cloudpickle_deserialize(py, value)
            }
        }
    }
}

#[pymodule]
fn motionstate(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<State>()?;
    m.add_class::<StateValue>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;

    #[test]
    fn state_init_with_valid_url() {
        let _state = State::new(
            "component".to_string(),
            "instance".to_string(),
            "redis://127.0.0.1:6381",
        )
        .unwrap();
    }

    #[test]
    fn state_init_with_invalid_url() {
        let result = State::new(
            "component".to_string(),
            "instance".to_string(),
            "invalid_url",
        );
        assert!(result.is_err());
    }

    #[test]
    fn cache_test() {
        let gil = Python::acquire_gil();
        let py = gil.python();

        let mut state = State::new(
            "component".to_string(),
            "instance".to_string(),
            "redis://127.0.0.1:6381",
        )
        .unwrap();

        // Set a value to Redis
        let _ = state
            .bulk_set(py, [("test_key", 42)].into_py_dict(py))
            .unwrap();

        // Clear cache to simulate fetching from Redis
        state.clear_cache();
        let first_fetch = state.get(py, "test_key").unwrap();
        assert_eq!(first_fetch.extract::<i64>(py).unwrap(), 42);

        // This should be fetched from cache
        let second_fetch = state.get(py, "test_key").unwrap();
        assert_eq!(second_fetch.extract::<i64>(py).unwrap(), 42);
    }
}
