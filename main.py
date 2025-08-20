import asyncio
import logging

import click
import tqdm
import uvloop

from src.notion.notion_client import NotionClient
from src.orchestration import (
    process_telegram_channel,
    run_checks,
    upload_state_data_to_notion,
    upload_timeseries_data_to_notion,
)
from src.shared.logging_utils import configure_logging
from src.telegram.telegram_client import TelegramUserClient

logger = logging.getLogger("main")


async def run_async(debug: bool = False):
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    configure_logging(level="DEBUG" if debug else "WARNING")

    click.echo("Running .env checks")
    run_checks()
    click.echo("All .env checks passed")

    click.echo("Initializing clients")

    telegram_client = TelegramUserClient()
    notion_client = NotionClient()

    click.echo("Fetching channels to process...")

    channels_to_process = await notion_client.get_channels_to_parse()

    click.echo(f"Channels to process: {channels_to_process}")

    for channel_name in tqdm.tqdm(channels_to_process):
        click.echo(f"Processing channel {channel_name}")

        channel_state, channel_timeseries = await process_telegram_channel(
            telegram_client, channel_name
        )

        click.echo(f"Uploading {len(channel_state)} state entries to Notion...")
        await upload_state_data_to_notion(notion_client, channel_state)

        click.echo(
            f"Uploading {len(channel_timeseries)} timeseries entries to Notion..."
        )
        await upload_timeseries_data_to_notion(notion_client, channel_timeseries)

    click.echo("All channels processed")


@click.command()
@click.option("--debug", is_flag=True, help="Run in debug mode", default=False)
def run(debug: bool = False):
    asyncio.run(run_async(debug))


if __name__ == "__main__":
    run()
