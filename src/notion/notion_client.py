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

    async def is_present(self, database_id: str, handle: str, date: str) -> bool:
        """
        Checks if a page with the given handle and date is present in the database.
        """
        assert (
            database_id is not None
            and isinstance(database_id, str)
            and len(database_id) > 0
        ), "Database ID is not set"
        assert handle is not None and isinstance(handle, str) and len(handle) > 0, (
            "Handle is not set"
        )
        assert date is not None and isinstance(date, str) and len(date) > 0, (
            "Date is not set"
        )

        client = cast(AsyncClient, self.client)

        response = await client.databases.query(
            database_id=database_id,
            filter={
                "and": [
                    {"property": "Date", "date": {"equals": date}},
                    {"property": "Handle", "rich_text": {"equals": handle}},
                ],
            },
        )

        results = response.get("results", None)
        is_empty = results is None or len(results) == 0

        return not is_empty

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
