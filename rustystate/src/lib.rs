use pyo3::exceptions;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString};
use redis::Commands;
use std::collections::HashMap;

/*
TODO:
* Increment version when calling set_bulk
* Remove set method (unnecessary)
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

    pub fn set(&mut self, py: Python, key: String, value: &PyAny) -> PyResult<()> {
        let mut con = self.client.get_connection().unwrap();

        let serialized_data = serialize_value(py, value)?;

        self.cache.insert(key.clone(), serialized_data.clone());
        con.set::<_, _, ()>(key, serialized_data).unwrap();
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
}

// Serialization Helpers
const MARKER_LIST: u8 = 0x01;
const MARKER_DICT: u8 = 0x02;

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
    if value.is_instance::<PyInt>()?
        || value.is_instance::<PyFloat>()?
        || value.is_instance::<PyString>()?
    {
        Ok(value.str()?.to_string().into_bytes())
    } else if value.is_instance::<PyDict>()? {
        let mut serialized = vec![MARKER_DICT];
        serialized.extend(serialize_dict(py, value)?);
        Ok(serialized)
    } else if value.is_instance::<PyList>()? {
        let list = value.downcast::<PyList>()?;
        let mut serialized = vec![MARKER_LIST];
        for item in list.iter() {
            let serialized_item = serialize_value(py, item)?;
            serialized.extend((serialized_item.len() as u64).to_le_bytes().iter());
            serialized.extend(serialized_item);
        }
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

fn deserialize_value(py: Python, value: &[u8]) -> PyResult<PyObject> {
    if value.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Empty data",
        ));
    }

    // Check the marker first
    match value[0] {
        MARKER_LIST => {
            let list = pyo3::types::PyList::empty(py);
            let mut cursor = 1;
            while cursor < value.len() {
                let item_len =
                    u64::from_le_bytes(value[cursor..cursor + 8].try_into().unwrap()) as usize;
                cursor += 8;
                let item = deserialize_value(py, &value[cursor..cursor + item_len])?;
                list.append(item)?;
                cursor += item_len;
            }
            Ok(list.into())
        }
        MARKER_DICT => {
            let dict = PyDict::new(py);
            let mut cursor = 1;
            while cursor < value.len() {
                let key_len =
                    u64::from_le_bytes(value[cursor..cursor + 8].try_into().unwrap()) as usize;
                cursor += 8;
                let key = deserialize_value(py, &value[cursor..cursor + key_len])?;
                cursor += key_len;

                let val_len =
                    u64::from_le_bytes(value[cursor..cursor + 8].try_into().unwrap()) as usize;
                cursor += 8;
                let val = deserialize_value(py, &value[cursor..cursor + val_len])?;
                cursor += val_len;

                dict.set_item(key, val)?;
            }
            Ok(dict.into())
        }
        _ => {
            // Only if it's not a marked value, we try to interpret it as a string
            if let Ok(decoded) = std::str::from_utf8(value) {
                if let Ok(int_value) = decoded.parse::<i64>() {
                    return Ok(int_value.into_py(py));
                } else if let Ok(float_value) = decoded.parse::<f64>() {
                    return Ok(float_value.into_py(py));
                }
                return Ok(decoded.to_string().into_py(py));
            }

            // Default to cloudpickle deserialization for unrecognized patterns
            cloudpickle_deserialize(py, value)
        }
    }
}

// fn serialize_list(py: Python, value: &PyAny) -> PyResult<Option<Vec<u8>>> {
//     let list = value.downcast::<PyList>()?;
//     let mut serialized_items = Vec::new();

//     for item in list.iter() {
//         if item.is_instance::<PyInt>()? {
//             serialized_items.push(item.str()?.to_string());
//         } else if item.is_instance::<PyFloat>()? {
//             serialized_items.push(item.str()?.to_string());
//         } else if item.is_instance::<PyString>()? {
//             serialized_items.push(item.str()?.to_string());
//         } else {
//             // If the list contains non-primitive types, return None
//             return Ok(None);
//         }
//     }

//     // Joining the serialized items with a delimiter (e.g., `|`), you can choose another delimiter if you wish.
//     Ok(Some(serialized_items.join("|").into_bytes()))
// }

// fn serialize_value(py: Python, value: &PyAny) -> PyResult<Vec<u8>> {
//     if value.is_instance::<PyInt>()?
//         || value.is_instance::<PyFloat>()?
//         || value.is_instance::<PyString>()?
//     {
//         Ok(value.str()?.to_string().into_bytes())
//     } else if value.is_instance::<PyList>()? {
//         if let Some(serialized) = serialize_list(py, value)? {
//             Ok(serialized)
//         } else {
//             // If couldn't serialize as a list of primitives, use cloudpickle
//             let cloudpickle = py.import("cloudpickle")?;
//             let bytes = cloudpickle
//                 .getattr("dumps")?
//                 .call1((value,))?
//                 .extract::<&PyBytes>()?;
//             Ok(bytes.as_bytes().to_vec())
//         }
//     } else {
//         // Use cloudpickle for other types
//         let cloudpickle = py.import("cloudpickle")?;
//         let bytes = cloudpickle
//             .getattr("dumps")?
//             .call1((value,))?
//             .extract::<&PyBytes>()?;
//         Ok(bytes.as_bytes().to_vec())
//     }
// }

// fn deserialize_value(py: Python, value: &[u8]) -> PyResult<PyObject> {
//     if let Ok(decoded) = std::str::from_utf8(value) {
//         if let Ok(int_value) = decoded.parse::<i64>() {
//             Ok(int_value.into_py(py))
//         } else if let Ok(float_value) = decoded.parse::<f64>() {
//             Ok(float_value.into_py(py))
//         } else if decoded.contains("|") {
//             // Detecting our delimiter to identify lists
//             let items: Vec<_> = decoded
//                 .split('|')
//                 .map(|item| {
//                     if let Ok(int_value) = item.parse::<i64>() {
//                         int_value.into_py(py)
//                     } else if let Ok(float_value) = item.parse::<f64>() {
//                         float_value.into_py(py)
//                     } else {
//                         item.to_string().into_py(py)
//                     }
//                 })
//                 .collect();
//             Ok(PyList::new(py, &items).into())
//         } else {
//             Ok(decoded.to_string().into_py(py))
//         }
//     } else {
//         let cloudpickle = py.import("cloudpickle")?;
//         let bytes_value = PyBytes::new(py, value);
//         let obj = cloudpickle.getattr("loads")?.call1((bytes_value,))?;
//         Ok(obj.into())
//     }
// }

#[pymodule]
fn rustystate(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<State>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
