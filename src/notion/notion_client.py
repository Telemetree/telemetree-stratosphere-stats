import os
from typing import Any, cast

from dotenv import load_dotenv
from notion_client import AsyncClient

from src.notion.notion_constants import CHANNELS_LIST_DATABASE_ID

load_dotenv()


class NotionClient:
    def __init__(self) -> None:
        api_key = os.getenv("NOTION_API_KEY")
        assert api_key is not None, "NOTION_API_KEY is not set"
        self.client = AsyncClient(auth=api_key)

    async def query_database(self, database_id: str) -> None:
        assert database_id is not None, "Database ID is not set"
        assert isinstance(database_id, str), "Database ID is not a string"
        assert len(database_id) > 0, "Database ID is empty"
        client = cast(AsyncClient, self.client)

        results = await client.databases.query(database_id=database_id)

        return results

    async def add_database_entry(self, database_id: str, data: dict[str, Any]) -> None:
        assert database_id is not None, "Database ID is not set"
        assert isinstance(database_id, str), "Database ID is not a string"
        assert len(database_id) > 0, "Database ID is empty"
        assert data is not None, "Data is not set"

        client = cast(AsyncClient, self.client)

        results = await client.pages.create(
            parent={"database_id": database_id},
            properties=data,
        )

        return results

    async def get_channels_to_parse(self) -> list[str]:
        assert CHANNELS_LIST_DATABASE_ID is not None, (
            "Channels list database ID is not set"
        )
        assert isinstance(CHANNELS_LIST_DATABASE_ID, str), (
            "Channels list database ID is not a string"
        )
        assert len(CHANNELS_LIST_DATABASE_ID) > 0, "Channels list database ID is empty"

        client = cast(AsyncClient, self.client)

        results = await client.databases.query(database_id=CHANNELS_LIST_DATABASE_ID)

        return results
