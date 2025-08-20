import logging
import os
from typing import cast

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import InputPeerUser, User
from telethon.tl.types.stats import BroadcastStats, MegagroupStats

from src.telegram.telegram_constants import TELEGRAM_SESSION_NAME

logger = logging.getLogger("telegram_client")

load_dotenv()


class TelegramUserClient:
    """
    Telegram user client
    """

    def __init__(self, session_name: str = TELEGRAM_SESSION_NAME) -> None:
        api_id_env = os.getenv("TELEGRAM_API_ID", None)
        api_hash_env = os.getenv("TELEGRAM_API_HASH", None)

        assert api_id_env is not None, "TELEGRAM_API_ID is not set"
        assert api_id_env.isdigit(), "TELEGRAM_API_ID is not a number"
        assert api_hash_env is not None, "TELEGRAM_API_HASH is not set"

        try:
            api_id: int = int(api_id_env)
            api_hash: str = api_hash_env
        except ValueError:
            logger.error("TELEGRAM_API_ID is not a number")
            raise ValueError("TELEGRAM_API_ID is not a number") from None
        except Exception as e:
            logger.error("Error initializing Telegram client: %s", e)
            raise e from None

        self.client: TelegramClient = TelegramClient(
            session_name,
            api_id,
            api_hash,
        )

    async def is_admin(self, channel_name: str) -> bool:
        """
        Check if the logged in user is an admin in a channel
        """
        assert self.client is not None, "Client is not initialized"
        assert isinstance(self.client, TelegramClient), "Client is not a TelegramClient"
        client = cast(TelegramClient, self.client)

        async with client:
            logger.info("Checking if user is an admin in %s", channel_name)
            user_object = await client.get_me()
            try:
                permissions = await client.get_permissions(channel_name, user_object)

                is_allowed = permissions is not None and permissions.is_admin

                logger.info(
                    "Logged in user is %s in %s",
                    "an admin" if is_allowed else "not an admin",
                    channel_name,
                )

                return is_allowed
            except Exception as e:
                logger.error(
                    "Error checking if user is an admin in %s: %s",
                    channel_name,
                    e,
                )
                return False

    async def get_me(self) -> User | InputPeerUser:
        """
        Get the current user
        """
        assert self.client is not None, "Client is not initialized"
        assert isinstance(self.client, TelegramClient), "Client is not a TelegramClient"
        client = cast(TelegramClient, self.client)

        async with client:
            return await client.get_me()

    async def get_stats(self, channel_name: str) -> BroadcastStats | MegagroupStats:
        """
        Get the stats for a channel or megagroup

        Usual Telegram restrictions apply (eg megagroup must have >500 members
        to have stats)
        """
        assert channel_name is not None, "Channel name is not set"
        assert self.client is not None, "Client is not initialized"
        assert isinstance(self.client, TelegramClient), "Client is not a TelegramClient"

        client = cast(TelegramClient, self.client)

        async with client:
            try:
                resolved_channel = await client.get_input_entity(channel_name)
            except Exception as e:
                logger.error("Error getting channel stats: %s", e)
                raise e from None

            try:
                channel_stats = await client.get_stats(resolved_channel)
                channel_stats = cast(BroadcastStats | MegagroupStats, channel_stats)
            except Exception as e:
                logger.error("Error getting channel stats: %s", e)
                raise e from None

            return channel_stats

    async def process_broadcast_stats(self, broadcast_stats: BroadcastStats) -> None:
        """
        Process the channel stats
        """
        pass

    async def process_megagroup_stats(self, megagroup_stats: MegagroupStats) -> None:
        """
        Process the megagroup stats
        """
        pass
