import logging
from typing import TYPE_CHECKING

import pandas as pd
import tqdm

from src.notion.notion_constants import (
    CHANNEL_STATE_DATABASE_ID,
    CHANNEL_TIMESERIES_DATABASE_ID,
)
from src.notion.notion_utils import (
    format_telegram_state_data,
    format_telegram_timeseries_data,
    process_state_data,
    process_timeseries_data,
)
from src.telegram.telegram_utils import (
    is_channel_state,
    is_processable_graph,
    process_abs_value_and_prev,
    process_graph_data,
)

if TYPE_CHECKING:
    from src.notion.notion_client import NotionClient
    from src.telegram.telegram_client import TelegramUserClient

logger = logging.getLogger("orchestration")


async def process_telegram_channel(
    telegram_client: "TelegramUserClient",
    channel_name: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Processing Telegram channel %s", channel_name)

    telegram_stats = await telegram_client.get_stats(channel_name)
    telegram_stats_dict = telegram_stats.to_dict()

    state_data = {}
    timeseries_data = {}

    for key, value in telegram_stats_dict.items():
        if is_channel_state(key, value):
            results = process_abs_value_and_prev(key, value)
            state_data[key] = results
        elif is_processable_graph(key, value):
            results = process_graph_data(value)
            timeseries_data[key] = results

    state_data = format_telegram_state_data(state_data, channel_name)
    timeseries_data = format_telegram_timeseries_data(timeseries_data, channel_name)

    return state_data, timeseries_data


async def upload_state_data_to_notion(
    notion_client: "NotionClient",
    state_data: pd.DataFrame,
) -> None:
    state_data_dict = state_data.to_dict(orient="records")
    length = len(state_data_dict)

    logger.info("Uploading %s entries of state to Notion", length)
    for idx, entry in tqdm.tqdm(enumerate(state_data_dict)):
        date = entry.get("date", None)
        date_from_timestamp = date.strftime("%Y-%m-%d")
        date_str = str(date_from_timestamp)
        handle = entry.get("handle", None)

        logger.info(
            "%s/%s - Processing date %s for handle %s",
            idx + 1,
            length,
            date,
            handle,
        )
        followers = entry.get("followers", 0)
        reactions = entry.get("reactions", 0)
        views = entry.get("views", 0)
        shares = entry.get("shares", 0)

        if date is None or handle is None:
            logger.error("Date or handle is not set for entry %s", entry)
            continue

        is_present = await notion_client.is_present(
            CHANNEL_STATE_DATABASE_ID, handle, date_str
        )

        if is_present:
            logger.info(
                "Entry for date %s for handle %s is already present in Notion",
                date,
                handle,
            )
            continue
        else:
            logger.info(
                "Entry for date %s for handle %s is not present in Notion, adding...",
                date,
                handle,
            )

        formated_body = process_state_data(
            date_str, handle, followers, reactions, views, shares
        )
        await notion_client.add_database_entry(CHANNEL_STATE_DATABASE_ID, formated_body)

    logger.info("State data uploaded to Notion")


async def upload_timeseries_data_to_notion(
    notion_client: "NotionClient",
    timeseries_data: pd.DataFrame,
) -> None:
    timeseries_data_dict = timeseries_data.to_dict(orient="records")
    length = len(timeseries_data_dict)

    logger.info("Uploading %s entries of timeseries to Notion", length)
    for idx, entry in tqdm.tqdm(enumerate(timeseries_data_dict)):
        date = entry.get("date", None)
        date_from_timestamp = date.strftime("%Y-%m-%d")
        date_str = str(date_from_timestamp)
        handle = entry.get("handle", None)

        logger.info(
            "%s/%s - Processing date %s for handle %s",
            idx + 1,
            length,
            date,
            handle,
        )

        joined = entry.get("joined", 0)
        mute = entry.get("mute", 0)
        left = entry.get("left", 0)
        followers = entry.get("followers", 0)

        if date is None or handle is None:
            logger.error("Date or handle is not set for entry %s", entry)
            continue

        is_present = await notion_client.is_present(
            CHANNEL_TIMESERIES_DATABASE_ID, handle, date_str
        )

        if is_present:
            logger.debug(
                "Entry for date %s for handle %s is already present in Notion",
                date,
                handle,
            )
            continue
        else:
            logger.debug(
                "Entry for date %s for handle %s is not present in Notion, adding...",
                date,
                handle,
            )

        formated_body = process_timeseries_data(
            date_str, handle, joined, mute, left, followers
        )
        await notion_client.add_database_entry(
            CHANNEL_TIMESERIES_DATABASE_ID, formated_body
        )


async def orchestrate():
    telegram_client = TelegramUserClient()
    notion_client = NotionClient()

    channels_to_process = await notion_client.get_channels_to_parse()

    logger.info("Channels to process: %s", channels_to_process)

    for channel_name in channels_to_process:
        logger.info("Processing channel %s", channel_name)

        channel_state, channel_timeseries = await process_telegram_channel(
            telegram_client, channel_name
        )

        await upload_state_data_to_notion(notion_client, channel_state)
        await upload_timeseries_data_to_notion(notion_client, channel_timeseries)

    logger.info("All channels processed")
