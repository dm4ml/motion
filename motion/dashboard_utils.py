import json
import os
from typing import Any, Dict, List

import redis

from motion.utils import get_redis_params, loadState, saveState

# Check if the environment variable is set
if os.getenv("MOTION_VICTORIAMETRICS_URL"):
    from prometheus_client import Counter, Gauge


def create_prometheus_metrics() -> List[Any]:
    # Initialize Prometheus metrics
    op_duration = Gauge(
        "op_duration_seconds",
        "Duration of operation in seconds",
        ["component_name", "instance_id", "op_name", "op_type"],
    )
    op_success = Counter(
        "op_success_total",
        "Total number of successful operations",
        ["component_name", "instance_id", "op_name", "op_type"],
    )
    op_failure = Counter(
        "op_failure_total",
        "Total number of failed operations",
        ["component_name", "instance_id", "op_name", "op_type"],
    )

    return [op_duration, op_success, op_failure]


def get_component_usage(component_name: str) -> Dict[str, Any]:
    # Retrieves the number of instances
    # and various statuses for a component
    rp = get_redis_params()
    redis_con = redis.Redis(**rp.dict())

    # Count number of keys that match the component name
    instance_keys = redis_con.keys(f"MOTION_VERSION:{component_name}__*")
    instance_ids = [key.decode("utf-8").split("__")[-1] for key in instance_keys]

    redis_con.close()

    return {
        "numInstances": len(instance_ids),
        "instanceIds": instance_ids,
    }


def get_component_instance_usage(
    component_name: str, instance_id: str, num_results: int = 100
) -> Dict[str, Any]:
    rp = get_redis_params()
    redis_con = redis.Redis(**rp.dict())

    result_keys = redis_con.keys(f"MOTION_LOG_STATUS:{component_name}__{instance_id}/*")

    # Group result keys by flow
    results_by_flow = {}
    all_results = []
    for result_key in result_keys:
        flow = result_key.decode("utf-8").split("/")[-1]

        # Peek at the first num_results results
        results = redis_con.lrange(result_key, 0, num_results - 1)

        # Count the number of successes, errors, and cache hits
        results = [json.loads(result.decode("utf-8")) for result in results]
        results_by_flow[flow] = len(results)

        # Add results to all_results but include a new key in each result
        # that indicates the flow
        for result in results:
            result["flow"] = flow
            all_results.append(result)

    version = int(redis_con.get(f"MOTION_VERSION:{component_name}__{instance_id}"))
    redis_con.close()

    return {
        "version": version,
        "resultsByFlow": results_by_flow,
        "allResults": all_results,
    }


def writeState(instance_name: str, new_updates: Dict[str, Any]) -> None:
    # Load state and version from redis
    # Establish a connection to the Redis server
    rp = get_redis_params()
    redis_con = redis.Redis(**rp.dict())

    state, version = loadState(redis_con, instance_name, None)
    if state is None:
        raise ValueError(f"Instance {instance_name} does not exist.")

    # Update the state
    state.update(new_updates)

    # Save the state
    saveState(state, version, redis_con, instance_name, None)

    # Close the connection to the Redis server
    redis_con.close()
