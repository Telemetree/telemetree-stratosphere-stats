import json
import logging
from datetime import datetime, timedelta
from typing import Any

from src.telegram.telegram_constants import (
    TELEGRAM_DATA_SUPPORTED_KEYS,
    TELEGRAM_GRAPH_SUPPORTED_KEYS,
    TELEGRAM_NAME_MAP,
)

logger = logging.getLogger("telemetree-stratosphere-stats.telegram.utils")


def is_processable_graph(key: str, graph: dict[str, Any]) -> bool:
    if key not in TELEGRAM_GRAPH_SUPPORTED_KEYS:
        return False

    try:
        graph_type = graph.get("_", None)

        if graph_type is None or graph_type != "StatsGraph":
            return False

        return True
    except Exception as e:
        logger.error("Error processing graph: %s", e)
        return False


def is_channel_state(key: str, value: dict[str, Any]) -> bool:
    if key not in TELEGRAM_DATA_SUPPORTED_KEYS:
        return False

    try:
        value_type = value.get("_", None)

        if value_type is None or value_type != "StatsAbsValueAndPrev":
            return False

        return True
    except Exception as e:
        logger.error("Error processing channel state: %s", e)
        return False


def process_graph_data(graph: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Extract graph data from telethon response
    """
    x_axis_name = "date"
    entries: list[dict[str, Any]] = []

    graph_json = graph.get("json", None)
    assert graph_json is not None, "Graph JSON is not found"

    graph_data = json.loads(graph_json["data"])

    columns = graph_data.get("columns", None)

    names = graph_data.get("names", None)
    x_axis = columns[0]

    num_days = len(x_axis)
    total_entries = len(columns)

    for day in range(1, num_days, 1):
        daily_entry: dict[str, Any] = {}

        for entry in range(total_entries):
            if entry == 0:
                entry_name = x_axis_name
                value = datetime.fromtimestamp(columns[entry][day] / 1000).strftime(
                    "%Y-%m-%d"
                )
            else:
                code_name = columns[entry][0]
                entry_name = names[code_name]
                value = columns[entry][day]

            daily_entry[entry_name] = value

        entries.append(daily_entry)

    return entries


def process_abs_value_and_prev(key: str, value: dict[str, Any]) -> list[dict[str, Any]]:
    today_date = datetime.now().strftime("%Y-%m-%d")
    today_value = value.get("current", None)
    assert today_value is not None, "Today value is not found"

    today_response = {
        f"{today_date}": today_value,
        f"{TELEGRAM_NAME_MAP[key]}": today_value,
    }

    sevendays_ago_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    sevendays_ago_value = value.get("previous", None)
    assert sevendays_ago_value is not None, "Sevendays ago value is not found"

    sevendays_ago_response = {
        f"{sevendays_ago_date}": sevendays_ago_value,
        f"{TELEGRAM_NAME_MAP[key]}": sevendays_ago_value,
    }

    return [today_response, sevendays_ago_response]
