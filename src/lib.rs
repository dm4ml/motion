// pub mod state_value;
// use state_value::StateValue;

pub mod temp_value;
use temp_value::TempValue;

use pyo3::exceptions;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList};
use redis::Commands;
use redlock::RedLock;
use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
enum PyValue {
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<PyValue>),
    Dict(HashMap<String, PyValue>),
    // ... Add other types as needed.
}

#[pyclass]
pub struct StateAccessor {
    component_name: String,
    instance_id: String,
    lock_duration: usize,
    version: u64,
    client: redis::Client,
    cache: HashMap<String, PyObject>, // Stores deserialized values
    lock_manager: RedLock,
    max_lock_attempts: u32,
}

#[pymethods]
impl StateAccessor {
    #[new]
    pub fn new(
        component_name: String,
        instance_id: String,
        lock_duration: u64,
        redis_host: &str,
        redis_port: u16,
        redis_db: i64,
        redis_password: Option<&str>,
        redis_ssl: Option<bool>,
    ) -> PyResult<Self> {
        let use_ssl: bool = redis_ssl.unwrap_or(false);
        let protocol: &str = if use_ssl { "rediss" } else { "redis" };

        // Constructing the Redis URL with SSL consideration
        let redis_url = match redis_password {
            Some(password) => format!(
                "{}://:{}@{}:{}/{}",
                protocol, password, redis_host, redis_port, redis_db
            ),
            None => format!("{}://{}:{}/{}", protocol, redis_host, redis_port, redis_db),
        };

        let client = redis::Client::open(redis_url.clone()).map_err(|err| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Redis connection error: {}",
                err
            ))
        })?;

        // Read the version from Redis. If it doesn't exist, set it to 0.
        let mut con = client.get_connection().unwrap();
        let instancename = format!("MOTION_VERSION:{}__{}", component_name, instance_id);
        let version: u64 = con.get(&instancename).unwrap_or(0);

        // Create a lock manager
        let lock_manager = RedLock::new(vec![redis_url]);
        let max_lock_attempts = 3;

        Ok(StateAccessor {
            component_name,
            instance_id,
            lock_duration: lock_duration.try_into().unwrap(),
            version,
            client,
            cache: HashMap::new(),
            lock_manager,
            max_lock_attempts,
        })
    }

    #[getter]
    pub fn version(&self) -> PyResult<u64> {
        Ok(self.version)
    }

    pub fn set(&mut self, py: Python, key: &str, value: &PyAny) -> PyResult<()> {
        // Warning: This function does not check if the value is a TempValue.
        // But it is also never called from the Python side, so it's fine.
        let mut con = self.client.get_connection().unwrap();
        let serialized_data = Arc::new(serialize_value(py, value)?);

        // Create key name as MOTION_STATE:<component_name>__<instance_id>/<key>
        let keyname = format!(
            "MOTION_STATE:{}__{}/{}",
            self.component_name, self.instance_id, key
        );

        // Acquire the lock using rslock
        // Lockname will be MOTION_LOCK:<component_name>__<instance_id>
        let lock_name = format!("MOTION_LOCK:{}__{}", self.component_name, self.instance_id);
        let mut lock = None;

        // Loop until we get the lock
        for _ in 0..self.max_lock_attempts {
            match self
                .lock_manager
                .lock(lock_name.as_bytes(), self.lock_duration)
            {
                Ok(Some(l)) => {
                    lock = Some(l);
                    break;
                }
                Ok(None) => {
                    // Lock was not acquired. Sleep for 100ms and try again.
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                Err(e) => {
                    // Handle the Redis error, maybe return an error or log it.
                    return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to acquire lock due to Redis error: {}",
                        e
                    )));
                }
            }
        }
        if lock.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to acquire lock after {} attempts",
                self.max_lock_attempts
            )));
        }

        // Critical section
        // Insert the key and value into the cache
        self.cache.insert(keyname.clone(), value.into_py(py));

        // Increment the version and write it to Redis
        self.version += 1;

        // Insert the key and value into Redis through an atomic pipeline
        redis::pipe()
            .atomic()
            .set(keyname.clone(), &*serialized_data)
            .ignore()
            .set(
                format!(
                    "MOTION_VERSION:{}__{}",
                    self.component_name, self.instance_id
                ),
                self.version,
            )
            .ignore()
            .query(&mut con)
            .map_err(|err| {
                // Undo the cache insert and version increment
                self.cache.remove(&keyname);
                self.version -= 1;

                // Drop the lock
                self.lock_manager.unlock(lock.as_ref().unwrap());

                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Redis set data error: {}",
                    err
                ))
            })?;

        // Drop the lock
        self.lock_manager.unlock(lock.as_ref().unwrap());

        Ok(())
    }

    pub fn bulk_set(&mut self, py: Python, items: &PyDict, from_migration: bool) -> PyResult<()> {
        let mut con = self.client.get_connection().unwrap();

        // Preserialize all the data
        let mut serialized_items = Vec::with_capacity(items.len());
        for (key, value) in items.iter() {
            let keyname = format!(
                "MOTION_STATE:{}__{}/{}",
                self.component_name, self.instance_id, key
            );

            // If value is of type TempValue, we should serialize
            // the value inside it instead of the TempValue itself
            // and extract the TTL from the TempValue. On default,
            // the TTL will be None.
            // let (value_to_serialize, ttl): (PyObject, Option<u64>);
            if value.is_instance_of::<TempValue>() {
                let temp_value: PyRef<TempValue> = value.extract()?;
                // let value_to_serialize = &temp_value.value;
                let value_ref: &PyAny = temp_value.value.as_ref(py);
                let ttl = Some(temp_value.ttl);

                let serialized_data = Arc::new(serialize_value(py, value_ref)?);
                serialized_items.push((keyname, serialized_data, ttl));
            } else {
                let serialized_data = Arc::new(serialize_value(py, value)?);
                serialized_items.push((keyname, serialized_data, None));
            }

            // let serialized_data = Arc::new(serialize_value(py, value_to_serialize)?);
            // serialized_items.push((keyname, serialized_data, ttl));
        }

        let mut pipeline = redis::pipe();
        pipeline.atomic();

        // If not from_migration, acquire the lock using rslock
        // Lockname will be MOTION_LOCK:<component_name>__<instance_id>
        let mut lock = None;
        if !from_migration {
            let lock_name = format!("MOTION_LOCK:{}__{}", self.component_name, self.instance_id);

            // Loop until we get the lock
            for _ in 0..self.max_lock_attempts {
                match self
                    .lock_manager
                    .lock(lock_name.as_bytes(), self.lock_duration)
                {
                    Ok(Some(l)) => {
                        lock = Some(l);
                        break;
                    }
                    Ok(None) => {
                        // Lock was not acquired. Sleep for 100ms and try again.
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                    Err(e) => {
                        // Handle the Redis error, maybe return an error or log it.
                        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to acquire lock due to Redis error: {}",
                            e
                        )));
                    }
                }
            }
            if lock.is_none() {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to acquire lock after {} attempts",
                    self.max_lock_attempts
                )));
            }
        }

        // Critical section
        for (keyname, serialized_data, ttl) in serialized_items.iter() {
            let unserialized_value = items
                .get_item(keyname.replace(
                    &format!(
                        "MOTION_STATE:{}__{}/",
                        self.component_name, self.instance_id
                    ),
                    "",
                ))
                .unwrap();

            // Insert the key and value into the cache
            self.cache
                .insert(keyname.clone(), unserialized_value.into_py(py));

            // If ttl is not None, set the TTL
            if let Some(ttl) = ttl {
                pipeline
                    .cmd("SETEX")
                    .arg(keyname)
                    .arg(ttl)
                    .arg(&**serialized_data);
            } else {
                pipeline.cmd("SET").arg(keyname).arg(&**serialized_data);
            }
        }

        // Increment the version and write it to Redis
        self.version += 1;
        pipeline
            .set(
                format!(
                    "MOTION_VERSION:{}__{}",
                    self.component_name, self.instance_id
                ),
                self.version,
            )
            .ignore();

        // Execute the pipeline, throwing a Python error if it fails
        pipeline.query(&mut con).map_err(|err| {
            // Undo the cache insert and version increment
            for (key, _) in items {
                let keyname = format!(
                    "MOTION_STATE:{}__{}/{}",
                    self.component_name, self.instance_id, key
                );
                self.cache.remove(&keyname);
            }
            self.version -= 1;

            // Drop the lock if from_migration is false
            if !from_migration {
                self.lock_manager.unlock(lock.as_ref().unwrap());
            }

            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Redis bulk set error: {}",
                err
            ))
        })?;

        // Drop the lock if from_migration is false
        if !from_migration {
            self.lock_manager.unlock(lock.as_ref().unwrap());
        }

        Ok(())
    }

    pub fn get(&mut self, py: Python, key: &str) -> PyResult<PyObject> {
        // Create key name as MOTION_STATE:<component_name>__<instance_id>/<key>
        let keyname = format!(
            "MOTION_STATE:{}__{}/{}",
            self.component_name, self.instance_id, key
        );

        // Return the cached object if it exists
        if let Some(value) = self.cache.get(&keyname) {
            return Ok(value.clone_ref(py));
        }

        // Otherwise, fetch it from Redis
        let mut con = self.client.get_connection().unwrap();
        let result_data: redis::RedisResult<Option<Vec<u8>>> = con.get(&keyname);

        match result_data {
            Ok(Some(data)) => {
                // Deserialize the value
                let deserialized_value = deserialize_value(py, &data)?;

                // Insert the deserialized value into the cache
                self.cache
                    .insert(keyname.clone(), deserialized_value.clone_ref(py));

                Ok(deserialized_value)
            }
            Ok(None) => Err(PyErr::new::<exceptions::PyKeyError, _>("Key not found")),
            Err(err) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Redis get error: {}",
                err
            ))),
        }
    }

    pub fn items(&mut self, py: Python) -> PyResult<PyObject> {
        let items_list = pyo3::types::PyList::empty(py);
        let pattern = format!(
            "MOTION_STATE:{}__{}/{}",
            self.component_name, self.instance_id, "*"
        );

        let replaced_pattern = pattern.replace("*", "");
        let mut con = self.client.get_connection().unwrap();

        // Minimized Redis calls by fetching everything in one go.
        let keys: Vec<String> = con.keys(pattern).map_err(|err| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Redis keys error: {}", err))
        })?;

        for key in keys {
            let key_without_prefix = key.replace(&replaced_pattern, "");

            // Avoid cloning the key for Python conversion.
            let py_key = key_without_prefix.as_str().into_py(py);
            let value = self.get(py, &key_without_prefix)?;
            let tuple = pyo3::types::PyTuple::new(py, &[py_key, value]);
            items_list.append(tuple)?;
        }

        Ok(items_list.into())
    }

    pub fn keys(&self, _py: Python) -> PyResult<Vec<String>> {
        let pattern = format!(
            "MOTION_STATE:{}__{}/{}",
            self.component_name, self.instance_id, "*"
        );

        let mut con = self.client.get_connection().unwrap();
        let keys: Vec<String> = con.keys(pattern.clone()).map_err(|err| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Redis keys error: {}", err))
        })?;

        let replaced_pattern = pattern.replace("*", "");
        Ok(keys
            .into_iter()
            .map(|key| key.replace(&replaced_pattern, ""))
            .collect())
    }

    pub fn values(&mut self, py: Python) -> PyResult<PyObject> {
        let values_list = pyo3::types::PyList::empty(py);
        let keys = self.keys(py)?;
        for key in keys.iter() {
            let value = self.get(py, &key)?;
            values_list.append(value)?;
        }
        Ok(values_list.into())
    }

    pub fn clear_cache(&mut self) {
        self.cache.clear();

        // Reset version to whatever is in Redis
        let mut con = self.client.get_connection().unwrap();
        let version_key = format!(
            "MOTION_VERSION:{}__{}",
            self.component_name, self.instance_id
        );
        let version: u64 = con.get(version_key).unwrap_or(0);
        self.version = version;
    }
}

// Serialization Helpers

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

fn py_to_rust(value: &PyAny) -> PyResult<PyValue> {
    if let Ok(val) = value.extract::<i64>() {
        Ok(PyValue::Int(val))
    } else if let Ok(val) = value.extract::<f64>() {
        Ok(PyValue::Float(val))
    } else if let Ok(val) = value.extract::<String>() {
        Ok(PyValue::String(val))
    } else if let Ok(val) = value.downcast::<PyList>() {
        let list: Vec<_> = val
            .iter()
            .map(|item| py_to_rust(item))
            .collect::<Result<_, _>>()?;
        Ok(PyValue::List(list))
    } else if let Ok(val) = value.downcast::<PyDict>() {
        let mut dict = HashMap::new();
        for (key, val) in val.iter() {
            let key_str = key.extract::<String>()?;
            let val_rust = py_to_rust(val)?;
            dict.insert(key_str, val_rust);
        }
        Ok(PyValue::Dict(dict))
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Unsupported type for bincode serialization",
        ))
    }
}

fn rust_to_py(py: Python, value: &PyValue) -> PyResult<PyObject> {
    match value {
        PyValue::Int(val) => Ok(val.into_py(py)),
        PyValue::Float(val) => Ok(val.into_py(py)),
        PyValue::String(val) => Ok(val.into_py(py)),
        PyValue::List(val) => {
            let list = PyList::empty(py);
            for item in val {
                let py_item = rust_to_py(py, item)?;
                list.append(py_item)?;
            }
            Ok(list.into())
        }
        PyValue::Dict(val) => {
            let dict = PyDict::new(py);
            for (key, value) in val {
                let py_val = rust_to_py(py, value)?;
                dict.set_item(key, py_val)?;
            }
            Ok(dict.into())
        } // ... Handle other cases.
    }
}

fn serialize_value(py: Python, value: &PyAny) -> PyResult<Vec<u8>> {
    if let Ok(rust_value) = py_to_rust(value) {
        let serialized = bincode::serialize(&rust_value)
            .map_err(|e| PyErr::new::<exceptions::PyException, _>(format!("{}", e)))?;

        Ok(serialized)
    } else {
        // Fall back to cloudpickle if not any of the defined types
        let serialized = cloudpickle_serialize(py, value)?;

        Ok(serialized)
    }
}

fn deserialize_value(py: Python, value: &[u8]) -> PyResult<PyObject> {
    match bincode::deserialize::<PyValue>(value) {
        Ok(rust_value) => rust_to_py(py, &rust_value),
        Err(_) => {
            // Fall back to pickle if bincode deserialization fails
            let deserialized = cloudpickle_deserialize(py, value)?;
            Ok(deserialized.into())
        }
    }
}

#[pymodule]
fn motion(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<StateAccessor>()?;
    // m.add_class::<StateValue>()?;
    m.add_class::<TempValue>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::types::IntoPyDict;

    #[test]
    fn state_init_with_valid_url() {
        let _state = StateAccessor::new(
            "component".to_string(),
            "instance".to_string(),
            180 as u64,
            "127.0.0.1",
            6381,
            0,
            None,
        )
        .unwrap();
    }

    #[test]
    fn state_init_with_invalid_url() {
        let result = StateAccessor::new(
            "component".to_string(),
            "instance".to_string(),
            180 as u64,
            "invalid",
            6381,
            0,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn cache_test() {
        pyo3::Python::with_gil(|py| {
            let mut state = StateAccessor::new(
                "component".to_string(),
                "instance".to_string(),
                180 as u64,
                "127.0.0.1",
                6381,
                0,
                None,
            )
            .unwrap();

            // Set a value to Redis
            let _ = state
                .bulk_set(py, [("test_key", 42)].into_py_dict(py), false)
                .unwrap();

            // Clear cache to simulate fetching from Redis
            state.clear_cache();
            let first_fetch = state.get(py, "test_key").unwrap();
            assert_eq!(first_fetch.extract::<i64>(py).unwrap(), 42);

            // This should be fetched from cache
            let second_fetch = state.get(py, "test_key").unwrap();
            assert_eq!(second_fetch.extract::<i64>(py).unwrap(), 42);
        });
    }
}
