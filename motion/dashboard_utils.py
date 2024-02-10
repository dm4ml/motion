import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import redis
import requests

from motion.utils import get_redis_params, loadState, saveState


def query_victoriametrics(victoria_metrics_url: str, query: str) -> Any:
    """Query VictoriaMetrics using the given PromQL query."""
    params = {
        "query": query,
        "time": "now",  # Alternatively, you can specify a UNIX timestamp
    }
    response = requests.get(f"{victoria_metrics_url}/api/v1/query", params=params)
    return response.json()


def query_victoriametrics_with_ts(
    victoria_metrics_url: str, query: str, start: int, end: int, step: int
) -> Any:
    """Query VictoriaMetrics using the given PromQL query."""
    params = {"query": query, "start": start, "end": end, "step": step}
    response = requests.get(f"{victoria_metrics_url}/api/v1/query_range", params=params)  # type: ignore
    return response.json()


def calculate_percentage_change(old: int, new: int) -> float:
    if old == 0:
        return 0 if new == 0 else 100
    return ((new - old) / old) * 100


def calculate_color_and_tooltip(
    success_count: float, total_count: float
) -> Tuple[str, str]:
    if total_count == 0:
        return "gray", "Inactive"
    success_rate = success_count / total_count
    if success_rate >= 0.95:
        return "emerald", "Operational"
    elif 0.80 <= success_rate < 0.95:
        return "yellow", "Degraded"
    else:
        return "rose", "Downtime"


def get_interval_data(
    victoria_metrics_url: str, component_name: str, instance_id: Optional[str] = None
) -> Tuple[List[Dict[str, Any]], Optional[float]]:
    # Define time range
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    start_ts = int(start_time.timestamp())
    end_ts = int(end_time.timestamp())
    step = 30 * 60  # 30 minutes in seconds

    # Query for counts over the last 24 hours in 30-minute intervals
    if instance_id:
        success_query = f'sum(sum_over_time(motion_operation_success_count_value{{component="{component_name}", instance="{instance_id}"}}[30m]))'  # noqa: E501
        failure_query = f'sum(sum_over_time(motion_operation_failure_count_value{{component="{component_name}", instance="{instance_id}"}}[30m]))'  # noqa: E501

    else:
        success_query = f'sum(sum_over_time(motion_operation_success_count_value{{component="{component_name}"}}[30m]))'  # noqa: E501
        # Failure query
        failure_query = f'sum(sum_over_time(motion_operation_failure_count_value{{component="{component_name}"}}[30m]))'  # noqa: E501

    response = query_victoriametrics_with_ts(
        victoria_metrics_url, success_query, start_ts, end_ts, step
    )
    success_result = response.get("data", {}).get("result", [])

    response = query_victoriametrics_with_ts(
        victoria_metrics_url, failure_query, start_ts, end_ts, step
    )
    failure_result = response.get("data", {}).get("result", [])

    success_interval_counts = {
        timestamp: 0.0 for timestamp in range(start_ts, end_ts, 1800)
    }
    failure_interval_counts = {
        timestamp: 0.0 for timestamp in range(start_ts, end_ts, 1800)
    }

    # Assuming there's only one series in the result
    if success_result:
        for value in success_result[0].get("values", []):
            timestamp = int(value[0])
            count = float(value[1])
            success_interval_counts[timestamp] = count

    if failure_result:
        for value in failure_result[0].get("values", []):
            timestamp = int(value[0])
            count = float(value[1])
            failure_interval_counts[timestamp] = count

    # Convert the dictionary to a list of counts in order
    success_counts = list(success_interval_counts.values())
    failure_counts = list(failure_interval_counts.values())

    # Turn it into bars
    bars = []
    for success_count, failure_count in zip(success_counts, failure_counts):
        color, tooltip = calculate_color_and_tooltip(
            success_count, success_count + failure_count
        )
        bars.append({"color": color, "tooltip": tooltip})

    denominator = sum(success_counts) + sum(failure_counts)
    if denominator == 0:
        fraction_uptime = None
    else:
        fraction_uptime = sum(success_counts) / denominator * 100

    return bars, fraction_uptime


def get_component_usage(component_name: str) -> Dict[str, Any]:
    # Retrieves the number of instances
    # and various statuses for a component
    rp = get_redis_params()
    redis_con = redis.Redis(**rp.dict())

    # Count number of keys that match the component name
    instance_keys = redis_con.keys(f"MOTION_VERSION:{component_name}__*")
    instance_ids = [key.decode("utf-8").split("__")[-1] for key in instance_keys]

    redis_con.close()

    # Query victoria metrics if it exists
    flow_count_list = []
    status_counts = {"success": 0, "failure": 0}
    prev_status_counts = {"success": 0, "failure": 0}
    status_changes = {
        "success": {"value": float("inf"), "deltaType": "increase"},
        "failure": {"value": float("inf"), "deltaType": "increase"},
    }
    status_bar_data: List[Dict[str, Any]] = []
    fraction_uptime = None
    victoria_metrics_url = os.getenv("MOTION_VICTORIAMETRICS_URL")
    if victoria_metrics_url:
        # Count logs by flow
        promql_query = f'count(motion_operation_duration_seconds_value{{component="{component_name}"}}[24h]) by (flow)'  # noqa: E501

        response = query_victoriametrics(victoria_metrics_url, promql_query)
        print(response)

        # Extract list of flows and their counts
        if response["status"] == "success":
            for result in response["data"]["result"]:
                flow = result["metric"]["flow"]
                count = result["value"][1]
                flow_count_list.append({"flow": flow, "count": int(count)})

        # Count logs by success/failure
        status_promql_query = f'count(motion_operation_duration_seconds_value{{component="{component_name}"}}[24h]) by (status)'  # noqa: E501

        response = query_victoriametrics(victoria_metrics_url, status_promql_query)

        # Extract success/failure counts
        if response["status"] == "success":
            for result in response["data"]["result"]:
                status = result["metric"]["status"]
                count = result["value"][1]
                status_counts[status] = int(count)

        # Previous period status counts
        prev_status_promql_query = f'count(motion_operation_duration_seconds_value{{component="{component_name}"}}[24h] offset 24h) by (status)'  # noqa: E501
        prev_response = query_victoriametrics(
            victoria_metrics_url, prev_status_promql_query
        )

        # Extract success/failure counts
        if prev_response["status"] == "success":
            for result in prev_response["data"]["result"]:
                status = result["metric"]["status"]
                count = result["value"][1]
                prev_status_counts[status] = int(count)

        # Calculate percentage changes
        for status, count in status_counts.items():
            prev_count = prev_response.get(status, 0)
            change = calculate_percentage_change(prev_count, count)
            deltaType = "unchanged"
            if change > 0:
                deltaType = "increase"
            elif change < 0:
                deltaType = "decrease"
            status_changes[status] = {
                "value": f"{change:.2f}%",
                "deltaType": deltaType,
            }

        # Calculate interval data
        status_bar_data, fraction_uptime = get_interval_data(
            victoria_metrics_url, component_name
        )

    # Sort the flow counts by count
    flow_count_list = sorted(flow_count_list, key=lambda x: x["count"], reverse=True)

    return {
        "numInstances": len(instance_ids),
        "instanceIds": instance_ids,
        "flowCounts": flow_count_list,
        "statusCounts": status_counts,
        "statusChanges": status_changes,
        "statusBarData": status_bar_data,
        "fractionUptime": fraction_uptime,
    }


def get_component_instance_usage(
    component_name: str, instance_id: str, num_results: int = 100
) -> Dict[str, Any]:
    rp = get_redis_params()
    redis_con = redis.Redis(**rp.dict())

    try:
        version = int(redis_con.get(f"MOTION_VERSION:{component_name}__{instance_id}"))  # type: ignore
    except TypeError:
        raise ValueError(f"Instance {component_name}__{instance_id} does not exist.")

    redis_con.close()

    # Get the results by flow using the component name and instance id
    flowCounts = []
    statusBarData: List[Dict[str, Any]] = []
    fraction_uptime = None

    victoria_metrics_url = os.getenv("MOTION_VICTORIAMETRICS_URL")
    if victoria_metrics_url:
        # Count logs by flow
        promql_query = f'count(motion_operation_duration_seconds_value{{component="{component_name}",instance="{instance_id}"}}[24h]) by (flow)'  # noqa: E501

        response = query_victoriametrics(victoria_metrics_url, promql_query)

        # Extract list of flows and their counts
        if response["status"] == "success":
            for result in response["data"]["result"]:
                flow = result["metric"]["flow"]
                count = result["value"][1]
                flowCounts.append({"flow": flow, "count": int(count)})

        # Calculate interval data
        statusBarData, fraction_uptime = get_interval_data(
            victoria_metrics_url, component_name, instance_id
        )

    # Sort the flow counts by count
    flowCounts = sorted(flowCounts, key=lambda x: x["count"], reverse=True)

    return {
        "version": version,
        "flowCounts": flowCounts,
        "statusBarData": statusBarData,
        "fractionUptime": fraction_uptime,
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
