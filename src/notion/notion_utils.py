from typing import Any

from src.notion.notion_constants import (
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
