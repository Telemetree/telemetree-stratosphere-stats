from typing import Any

import pandas as pd

from src.notion.notion_constants import (
    CHANNEL_TIMESERIES_DATE_ID,
    CHANNEL_TIMESERIES_HANDLE_ID,
    CHANNEL_TIMESERIES_JOINED_ID,
    CHANNEL_TIMESERIES_LEFT_ID,
    CHANNEL_TIMESERIES_MUTE_ID,
    CHANNEL_TIMESERIES_TOTAL_FOLLOWERS_ID,
    STATE_DATE_ID,
    STATE_FOLLOWERS_ID,
    STATE_HANDLE_ID,
    STATE_REACTIONS_ID,
    STATE_SHARES_ID,
    STATE_VIEWS_ID,
)


def process_state_data(
    date: str, handle: str, followers: int, reactions: int, views: int, shares: int
) -> dict[str, Any]:
    """
    Process the channel state data and transform into Notion expected format.
    """

    assert followers is not None and followers >= 0, (
        "Followers must be a positive number"
    )
    assert reactions is not None and reactions >= 0, (
        "Reactions must be a positive number"
    )
    assert views is not None and views >= 0, "Views must be a positive number"
    assert shares is not None and shares >= 0, "Shares must be a positive number"

    assert date is not None and handle is not None, "Date and handle must be provided"

    state_data = {
        "Followers Per Post": {
            "id": STATE_FOLLOWERS_ID,
            "type": "number",
            "number": followers,
        },
        "Reactions Per Post": {
            "id": STATE_REACTIONS_ID,
            "type": "number",
            "number": reactions,
        },
        "Date": {
            "id": STATE_DATE_ID,
            "type": "date",
            "date": {"start": date, "end": None, "time_zone": None},
        },
        "Views Per Post": {"id": STATE_VIEWS_ID, "type": "number", "number": views},
        "Shares Per Post": {"id": STATE_SHARES_ID, "type": "number", "number": shares},
        "Handle": {
            "id": STATE_HANDLE_ID,
            "type": "title",
            "title": [
                {
                    "type": "text",
                    "text": {"content": handle, "link": None},
                    "annotations": {
                        "bold": False,
                        "italic": False,
                        "strikethrough": False,
                        "underline": False,
                        "code": False,
                        "color": "default",
                    },
                    "plain_text": handle,
                    "href": None,
                }
            ],
        },
    }

    return state_data


def process_timeseries_data(
    date: str, handle: str, joined: int, mute: int, left: int, followers: int
) -> dict[str, Any]:
    """
    Process the channel timeseries data and transform into Notion expected format.
    """
    assert joined is not None and joined >= 0, "Joined must be a positive number"
    assert mute is not None and mute >= 0, "Mute must be a positive number"
    assert left is not None and left >= 0, "Left must be a positive number"
    assert followers is not None and followers >= 0, (
        "Followers must be a positive number"
    )

    assert isinstance(date, str), "Date must be a string"
    assert isinstance(handle, str), "Handle must be a string"

    assert date is not None and handle is not None, "Date and handle must be provided"
    timeseries_data = {
        "Joined": {
            "id": CHANNEL_TIMESERIES_JOINED_ID,
            "type": "number",
            "number": joined,
        },
        "Date": {
            "id": CHANNEL_TIMESERIES_DATE_ID,
            "type": "date",
            "date": {"start": date, "end": None, "time_zone": None},
        },
        "Mute": {"id": CHANNEL_TIMESERIES_MUTE_ID, "type": "number", "number": mute},
        "Left": {"id": CHANNEL_TIMESERIES_LEFT_ID, "type": "number", "number": left},
        "Total followers": {
            "id": CHANNEL_TIMESERIES_TOTAL_FOLLOWERS_ID,
            "type": "number",
            "number": followers,
        },
        "Handle": {
            "id": CHANNEL_TIMESERIES_HANDLE_ID,
            "type": "title",
            "title": [
                {
                    "type": "text",
                    "text": {"content": handle, "link": None},
                    "annotations": {
                        "bold": False,
                        "italic": False,
                        "strikethrough": False,
                        "underline": False,
                        "code": False,
                        "color": "default",
                    },
                    "plain_text": handle,
                    "href": None,
                }
            ],
        },
    }

    return timeseries_data


def process_channels_list_data(results: list[dict[str, Any]]) -> list[str]:
    """
    Process the channels list data and transform into Notion expected format.
    """
    assert results is not None, "Results are not set"
    channels_to_parse = []

    for channel in results:
        properties = channel.get("properties", None)
        assert properties is not None, "Properties are not set"

        handle = properties.get("Handle", None)
        assert handle is not None, "Handle is not set"

        handle_value = handle.get("title", None)
        assert handle_value is not None, "Handle value is not set"

        handle_value = handle_value[0].get("plain_text", None)
        assert handle_value is not None, "Handle value is not set"

        channels_to_parse.append(handle_value)

    return channels_to_parse


def format_telegram_state_data(  # noqa: C901
    results_dict: dict[str, Any], handle: str
) -> pd.DataFrame:
    """
    Format telegram stats response for state_data DataFrame.
    """
    rows = []

    all_dates = set()
    for metric_key, metric_data in results_dict.items():
        for entry in metric_data:
            # Extract date (assuming first key is the date)
            date_key = next(iter(entry.keys()))
            if (
                date_key != metric_key.replace("_", " ").title()
            ):  # Skip the duplicate value key
                all_dates.add(date_key)

    # Process each date
    for date in sorted(all_dates):
        row = {
            "date": date,
            "handle": handle,
            "followers": None,
            "reactions": None,
            "views": None,
            "shares": None,
        }

        # Extract values for this date from each metric
        for metric_key, metric_data in results_dict.items():
            for entry in metric_data:
                if date in entry:
                    if metric_key == "followers":
                        row["followers"] = entry[date]
                    elif metric_key == "reactions_per_post":
                        row["reactions"] = entry[date]
                    elif metric_key == "views_per_post":
                        row["views"] = entry[date]
                    elif metric_key == "shares_per_post":
                        row["shares"] = entry[date]

        rows.append(row)

    # Create DataFrame
    state_data = pd.DataFrame(rows)

    # Convert date column to datetime if needed
    state_data["date"] = pd.to_datetime(state_data["date"])

    return state_data


def format_telegram_timeseries_data(
    results_dict: dict[str, Any], handle: str
) -> pd.DataFrame:
    """
    Format telegram stats response for timeseries_data DataFrame.
    """

    all_dates = set()
    for graph_data in results_dict.values():
        for entry in graph_data:
            all_dates.add(entry["date"])

    all_dates = sorted(all_dates)

    timeseries_data = pd.DataFrame(
        {"date": pd.to_datetime(all_dates), "handle": handle}
    )

    if "growth_graph" in results_dict:
        growth_df = pd.DataFrame(results_dict["growth_graph"])
        growth_df["date"] = pd.to_datetime(growth_df["date"])
        growth_df = growth_df.rename(columns={"Total followers": "followers"})
        timeseries_data = timeseries_data.merge(
            growth_df[["date", "followers"]], on="date", how="left"
        )
    else:
        timeseries_data["followers"] = 0

    if "followers_graph" in results_dict:
        followers_df = pd.DataFrame(results_dict["followers_graph"])
        followers_df["date"] = pd.to_datetime(followers_df["date"])
        followers_df = followers_df.rename(columns={"Joined": "joined", "Left": "left"})
        timeseries_data = timeseries_data.merge(
            followers_df[["date", "joined", "left"]], on="date", how="left"
        )
    else:
        timeseries_data["joined"] = 0
        timeseries_data["left"] = 0

    if "mute_graph" in results_dict:
        mute_df = pd.DataFrame(results_dict["mute_graph"])
        mute_df["date"] = pd.to_datetime(mute_df["date"])
        mute_df = mute_df.rename(columns={"Muted": "mute"})
        timeseries_data = timeseries_data.merge(
            mute_df[["date", "mute"]], on="date", how="left"
        )
    else:
        timeseries_data["mute"] = 0

    # Fill any remaining NaN values with 0 and ensure correct column order
    timeseries_data = timeseries_data.fillna(0)
    timeseries_data = timeseries_data[
        ["date", "handle", "joined", "mute", "left", "followers"]
    ]

    return timeseries_data
