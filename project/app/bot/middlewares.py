from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

from aiogram import BaseMiddleware, Bot
from aiogram.types import CallbackQuery, Message, PreCheckoutQuery, TelegramObject

from ..config import config
from ..db.queries import CustomerRepository
from ..services.sanitizer import is_suspicious_user
from ..services.translation import TranslationManager

logger = logging.getLogger(__name__)


class EnsureCustomerMiddleware(BaseMiddleware):
    def __init__(self, customer_repo: CustomerRepository) -> None:
        super().__init__()
        self.customer_repo = customer_repo

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Any],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user = None
        lang_code = config.default_language
        if isinstance(event, Message):
            user = event.from_user
            lang_code = event.from_user.language_code or lang_code
        elif isinstance(event, CallbackQuery):
            user = event.from_user
            lang_code = event.from_user.language_code or lang_code
        elif isinstance(event, PreCheckoutQuery):
            user = event.from_user
            lang_code = event.from_user.language_code or lang_code

        if user:
            try:
                existing = await self.customer_repo.find_by_telegram_id(user.id)
                if existing:
                    await self.customer_repo.update_fields(existing.id, {"username": user.username})
                else:
                    created = await self.customer_repo.find_or_create(user.id, lang_code)
                    await self.customer_repo.update_fields(
                        created.id,
                        {"language": lang_code, "language_selected": 0, "username": user.username},
                    )
            except Exception as err:  # noqa: BLE001
                logger.exception("failed to ensure customer: %s", err)

        return await handler(event, data)


class SuspiciousUserMiddleware(BaseMiddleware):
    def __init__(self, bot: Bot, tm: TranslationManager) -> None:
        super().__init__()
        self.bot = bot
        self.tm = tm

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Any],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        tg_user = None
        chat_id = None
        lang_code = config.default_language

        if isinstance(event, Message):
            tg_user = event.from_user
            chat_id = event.chat.id
            lang_code = event.from_user.language_code or lang_code
        elif isinstance(event, CallbackQuery):
            tg_user = event.from_user
            chat_id = event.message.chat.id if event.message else None
            lang_code = event.from_user.language_code or lang_code
        elif isinstance(event, PreCheckoutQuery):
            tg_user = event.from_user
            chat_id = event.from_user.id
            lang_code = event.from_user.language_code or lang_code

        if tg_user:
            if tg_user.id in config.blocked_telegram_ids:
                await self._deny(chat_id, lang_code)
                return None
            if tg_user.id in config.whitelisted_telegram_ids:
                return await handler(event, data)
            if is_suspicious_user(tg_user.username, tg_user.first_name, tg_user.last_name):
                await self._deny(chat_id, lang_code)
                return None

        return await handler(event, data)

    async def _deny(self, chat_id: Optional[int], lang_code: str) -> None:
        if not chat_id:
            return
        try:
            await self.bot.send_message(
                chat_id,
                self.tm.get_text(lang_code, "access_denied"),
                parse_mode="HTML",
            )
        except Exception as err:  # noqa: BLE001
            logger.warning("failed to send deny message: %s", err)
