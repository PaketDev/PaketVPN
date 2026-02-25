from __future__ import annotations

import asyncio
import html
import logging
import math
import random
import string
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

from aiogram import Bot, Router, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    KeyboardButtonRequestUsers,
    Message,
    PreCheckoutQuery,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    UserShared,
    UsersShared,
)

from ...config import config
from ...db.queries import (
    Customer,
    CustomerRepository,
    DuoPurchaseMemberRepository,
    GiftNotificationRepository,
    PriceSettingRepository,
    PromoRepository,
    PurchaseRepository,
    ReferralRepository,
)
from ...services.business import PaymentService, SyncService, StatsService
from ...services.translation import TranslationManager
from ..keyboards.inline import payment_methods_keyboard, price_keyboard, start_keyboard
from ..middlewares import EnsureCustomerMiddleware, SuspiciousUserMiddleware

logger = logging.getLogger(__name__)

CallbackBuy = "buy"
CallbackSell = "sell"
CallbackStart = "start"
CallbackConnect = "connect"
CallbackConnectInstructions = "connect_instructions"
CallbackSettings = "settings"
CallbackSettingsToggleNotifications = "settings_toggle_notifications"
CallbackSettingsToggleBroadcast = "settings_toggle_broadcast"
CallbackSettingsLanguage = "settings_language"
CallbackLanguage = "lang"
CallbackPayment = "payment"
CallbackTrial = "trial"
CallbackActivateTrial = "activate_trial"
CallbackReferral = "referral"
CallbackPromo = "promo"
CallbackPromoCancel = "promo_cancel"
CallbackPromoAdminCreate = "promo_admin_create"
CallbackPromoAdminList = "promo_admin_list"
CallbackPromoAdmin = "promo_admin"
CallbackPromoTypeDays = "promo_type_days"
CallbackPromoTypeGb = "promo_type_gb"
CallbackStats = "stats_panel"
CallbackReferralList = "referral_list"
CallbackCaptcha = "captcha"
CallbackAdminPanel = "admin_panel"
CallbackAdminUsers = "admin_users"
CallbackAdminUsersSummary = "admin_users_summary"
CallbackAdminUsersNew = "admin_users_new"
CallbackAdminUsersFind = "admin_users_find"
CallbackAdminUsersDelete = "admin_users_delete"
CallbackAdminSubs = "admin_subs"
CallbackAdminSubsExtend = "admin_subs_extend"
CallbackAdminSubsForever = "admin_subs_forever"
CallbackAdminSubsDisable = "admin_subs_disable"
CallbackAdminBroadcast = "admin_broadcast"
CallbackAdminBroadcastStart = "admin_broadcast_start"
CallbackAdminBroadcastSend = "admin_broadcast_send"
CallbackAdminBroadcastCancel = "admin_broadcast_cancel"
CallbackAdminBroadcastAudience = "admin_broadcast_audience"
CallbackAdminBroadcastTest = "admin_broadcast_test"
CallbackAdminBroadcastButton = "admin_broadcast_button"
CallbackAdminBroadcastButtonToggle = "admin_broadcast_button_toggle"
CallbackAdminBroadcastButtonStyle = "admin_broadcast_button_style"
CallbackAdminBroadcastButtonText = "admin_broadcast_button_text"
CallbackAdminBroadcastButtonUrl = "admin_broadcast_button_url"
CallbackAdminBroadcastButtonEmoji = "admin_broadcast_button_emoji"
CallbackAdminBroadcastButtonEmojiClear = "admin_broadcast_button_emoji_clear"
CallbackAdminPrices = "admin_prices"
CallbackAdminPriceEdit = "admin_price_edit"
CallbackAdminReportTraffic = "admin_report_traffic"
CallbackAdminReportFinance = "admin_report_finance"
CallbackGiftMenu = "gift_menu"
CallbackGiftSelect = "gift_select"
CallbackDuoMembers = "duo_members"
CallbackAdminGift = "admin_gift"
CallbackAdminGiftDuration = "admin_gift_duration"
CallbackAdminGiftTag = "admin_gift_tag"

PRICE_FIELD_ORDER = [
    "price_1",
    "price_3",
    "price_6",
    "price_12",
    "price_duo",
    "price_family",
    "stars_price_1",
    "stars_price_3",
    "stars_price_6",
    "stars_price_12",
    "topup_10_price_stars",
    "topup_20_price_stars",
    "topup_50_price_stars",
]

PRICE_FIELD_LABELS = {
    "price_1": "1 мес (RUB)",
    "price_3": "3 мес (RUB)",
    "price_6": "6 мес (RUB)",
    "price_12": "12 мес (RUB)",
    "price_duo": "Duo (RUB)",
    "price_family": "Family (RUB)",
    "stars_price_1": "1 мес (Stars)",
    "stars_price_3": "3 мес (Stars)",
    "stars_price_6": "6 мес (Stars)",
    "stars_price_12": "12 мес (Stars)",
    "topup_10_price_stars": "TopUp 10GB (Stars)",
    "topup_20_price_stars": "TopUp 20GB (Stars)",
    "topup_50_price_stars": "TopUp 50GB (Stars)",
}


def parse_callback_data(data: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    if "?" not in data:
        return result
    _, query = data.split("?", 1)
    for param in query.split("&"):
        if "=" in param:
            key, value = param.split("=", 2)
            result[key] = value
    return result


def build_connect_text(customer: Customer, lang: str, tm: TranslationManager, traffic_text: str) -> str:
    now = datetime.utcnow()
    info_parts = []
    if customer.expire_at:
        if now < customer.expire_at:
            if customer.expire_at.year >= 2050:
                info_parts.append(tm.get_text(lang, "subscription_forever"))
            else:
                days_left = max(0, int((customer.expire_at - now).total_seconds() // 86400))
                formatted = customer.expire_at.strftime("%d.%m.%Y %H:%M")
                info_parts.append(tm.get_text(lang, "subscription_active") % (formatted, days_left))
            if traffic_text:
                info_parts.append(traffic_text)
            if customer.subscription_link and not config.mini_app_url and not config.is_web_app_link:
                info_parts.append(tm.get_text(lang, "subscription_link") % customer.subscription_link)
        else:
            info_parts.append(tm.get_text(lang, "no_subscription"))
    else:
        info_parts.append(tm.get_text(lang, "no_subscription"))
    return "\n".join(info_parts)


def build_connect_instructions_text(customer: Customer, lang: str, tm: TranslationManager) -> str:
    info_parts = [tm.get_text(lang, "connect_instructions")]
    if customer.subscription_link:
        info_parts.append(tm.get_text(lang, "connect_instruction_link_note"))
    if customer.subscription_link and not config.mini_app_url and not config.is_web_app_link:
        info_parts.append(tm.get_text(lang, "subscription_link") % customer.subscription_link)
    return "\n\n".join(info_parts)


async def get_traffic_usage(sync_service: SyncService, customer: Customer, tm: TranslationManager, lang: str) -> tuple[str, int, int, bool]:
    try:
        used, limit, ok = await sync_service.get_traffic_usage(customer.telegram_id)
        if not ok or limit <= 0:
            return "", used, limit, False
        used_gb = round(used / 1_073_741_824, 2)
        limit_gb = round(limit / 1_073_741_824, 2)
        remaining_gb = max(0.0, round(limit_gb - used_gb, 2))
        text = tm.get_text(lang, "traffic_usage") % (used_gb, limit_gb, remaining_gb)
        if used >= limit:
            text = text + "\n" + tm.get_text(lang, "traffic_exceeded")
        return text, used, limit, True
    except Exception as err:  # noqa: BLE001
        logger.debug("traffic usage check failed: %s", err)
        return "", 0, 0, False


def setup_router(
    bot: Bot,
    tm: TranslationManager,
    payment_service: PaymentService,
    sync_service: SyncService,
    customer_repo: CustomerRepository,
    purchase_repo: PurchaseRepository,
    referral_repo: ReferralRepository,
    promo_repo: PromoRepository,
    price_repo: PriceSettingRepository,
    gift_notification_repo: GiftNotificationRepository,
    duo_member_repo: DuoPurchaseMemberRepository,
    stats_service: StatsService,
    bot_username: str,
) -> Router:
    router = Router()
    router.message.middleware(SuspiciousUserMiddleware(bot, tm))
    router.callback_query.middleware(SuspiciousUserMiddleware(bot, tm))
    router.message.middleware(EnsureCustomerMiddleware(customer_repo))
    router.callback_query.middleware(EnsureCustomerMiddleware(customer_repo))
    router.pre_checkout_query.middleware(SuspiciousUserMiddleware(bot, tm))
    router.pre_checkout_query.middleware(EnsureCustomerMiddleware(customer_repo))

    pending_promo: Set[int] = set()
    promo_admin_state: Dict[int, Dict[str, Any]] = {}
    panel_state: Dict[int, Dict[str, Any]] = {}
    gift_state: Dict[int, Dict[str, Any]] = {}
    pending_captcha: Dict[int, Dict[str, Any]] = {}

    def _button_emoji_id(lang: str, key: str) -> Optional[str]:
        value = tm.get_text(lang, f"{key}_emoji_id")
        if not value or value == f"{key}_emoji_id":
            return None
        return value

    def _timezone() -> ZoneInfo:
        try:
            return ZoneInfo(config.stats_timezone)
        except Exception:  # noqa: BLE001
            return ZoneInfo("Asia/Yekaterinburg")

    def _stats_day_in_ekb() -> date:
        tz = _timezone()
        now_local = datetime.now(tz)
        return (now_local - timedelta(days=1)).date()

    def _local_day_bounds_utc(target_date: date) -> Tuple[datetime, datetime]:
        tz = _timezone()
        start_local = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=tz)
        end_local = start_local + timedelta(days=1)
        start_utc = start_local.astimezone(timezone.utc).replace(tzinfo=None)
        end_utc = end_local.astimezone(timezone.utc).replace(tzinfo=None)
        return start_utc, end_utc

    async def _send_log_message(text: str) -> None:
        if not config.log_chat_ids:
            return
        for chat_id in config.log_chat_ids:
            try:
                await bot.send_message(chat_id, text, parse_mode="HTML")
            except Exception as err:  # noqa: BLE001
                logger.warning("failed to send log to chat=%s: %s", chat_id, err)

    def _is_admin(user_id: int) -> bool:
        return user_id in config.notify_telegram_ids or (config.admin_telegram_id > 0 and user_id == config.admin_telegram_id)

    def _admin_main_keyboard(lang: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text=tm.get_text(lang, "admin_users_button"), callback_data=CallbackAdminUsers),
                    InlineKeyboardButton(text=tm.get_text(lang, "admin_subscriptions_button"), callback_data=CallbackAdminSubs),
                ],
                [
                    InlineKeyboardButton(text=tm.get_text(lang, "admin_broadcast_button"), callback_data=CallbackAdminBroadcast),
                    InlineKeyboardButton(text=tm.get_text(lang, "admin_prices_button"), callback_data=CallbackAdminPrices),
                ],
                [InlineKeyboardButton(text=tm.get_text(lang, "agift_button"), callback_data=CallbackAdminGift)],
                [
                    InlineKeyboardButton(text=tm.get_text(lang, "admin_report_traffic_button"), callback_data=CallbackAdminReportTraffic),
                    InlineKeyboardButton(text=tm.get_text(lang, "admin_report_finance_button"), callback_data=CallbackAdminReportFinance),
                ],
                [
                    InlineKeyboardButton(text=tm.get_text(lang, "promo_admin_create"), callback_data=CallbackPromoAdminCreate),
                    InlineKeyboardButton(text=tm.get_text(lang, "promo_admin_list"), callback_data=CallbackPromoAdminList),
                ],
                [InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackStart, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))],
            ]
        )

    def _admin_users_keyboard(lang: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=tm.get_text(lang, "admin_users_summary_button"), callback_data=CallbackAdminUsersSummary)],
                [InlineKeyboardButton(text=tm.get_text(lang, "admin_users_new_button"), callback_data=CallbackAdminUsersNew)],
                [InlineKeyboardButton(text=tm.get_text(lang, "admin_users_find_button"), callback_data=CallbackAdminUsersFind)],
                [InlineKeyboardButton(text=tm.get_text(lang, "admin_users_delete_button"), callback_data=CallbackAdminUsersDelete)],
                [InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackAdminPanel, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))],
            ]
        )

    def _admin_subs_keyboard(lang: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=tm.get_text(lang, "admin_sub_extend_button"), callback_data=CallbackAdminSubsExtend)],
                [InlineKeyboardButton(text=tm.get_text(lang, "admin_sub_forever_button"), callback_data=CallbackAdminSubsForever)],
                [InlineKeyboardButton(text=tm.get_text(lang, "admin_sub_disable_button"), callback_data=CallbackAdminSubsDisable)],
                [InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackAdminPanel, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))],
            ]
        )

    def _broadcast_default_state() -> Dict[str, Any]:
        return {
            "step": "broadcast_idle",
            "broadcast_audience": "broadcast_enabled",
            "broadcast_source_chat_id": None,
            "broadcast_source_message_id": None,
            "button_enabled": False,
            "button_text": "",
            "button_url": "",
            "button_style": "primary",
            "button_emoji_id": "",
        }

    def _ensure_broadcast_state(user_id: int) -> Dict[str, Any]:
        state = panel_state.get(user_id, {})
        defaults = _broadcast_default_state()
        defaults.update(state)
        panel_state[user_id] = defaults
        return defaults

    def _broadcast_audience_keys() -> List[str]:
        return ["broadcast_enabled", "active", "inactive", "all"]

    def _broadcast_button_style_keys() -> List[str]:
        return ["primary", "success", "danger"]

    def _broadcast_audience_title(lang: str, audience: str) -> str:
        return tm.get_text(lang, f"admin_broadcast_audience_{audience}")

    def _broadcast_button_style_title(lang: str, style: str) -> str:
        return tm.get_text(lang, f"admin_broadcast_button_style_{style}")

    def _broadcast_source_ready(state: Dict[str, Any]) -> bool:
        return bool(state.get("broadcast_source_chat_id") and state.get("broadcast_source_message_id"))

    def _broadcast_button_ready(state: Dict[str, Any]) -> bool:
        if not state.get("button_enabled"):
            return True
        return bool((state.get("button_text") or "").strip() and (state.get("button_url") or "").strip())

    def _broadcast_panel_text(lang: str, state: Dict[str, Any]) -> str:
        msg_state = (
            tm.get_text(lang, "admin_broadcast_message_ready")
            if _broadcast_source_ready(state)
            else tm.get_text(lang, "admin_broadcast_message_missing")
        )
        if not state.get("button_enabled"):
            btn_state = tm.get_text(lang, "admin_broadcast_button_disabled")
        elif _broadcast_button_ready(state):
            btn_state = tm.get_text(lang, "admin_broadcast_button_ready")
        else:
            btn_state = tm.get_text(lang, "admin_broadcast_button_incomplete")
        audience = state.get("broadcast_audience", "broadcast_enabled")
        return (
            f"{tm.get_text(lang, 'admin_broadcast_title')}\n\n"
            f"{tm.get_text(lang, 'admin_broadcast_message_line')}: <b>{msg_state}</b>\n"
            f"{tm.get_text(lang, 'admin_broadcast_audience_line')}: <b>{_broadcast_audience_title(lang, audience)}</b>\n"
            f"{tm.get_text(lang, 'admin_broadcast_button_line')}: <b>{btn_state}</b>"
        )

    def _broadcast_button_settings_text(lang: str, state: Dict[str, Any]) -> str:
        enabled = bool(state.get("button_enabled"))
        emoji = (state.get("button_emoji_id") or "").strip()
        emoji_value = f"<code>{html.escape(emoji)}</code>" if emoji else "—"
        url_value = html.escape((state.get("button_url") or "").strip()) or "—"
        text_value = html.escape((state.get("button_text") or "").strip()) or "—"
        style = state.get("button_style", "primary")
        status = tm.get_text(lang, "settings_on") if enabled else tm.get_text(lang, "settings_off")
        return (
            f"{tm.get_text(lang, 'admin_broadcast_button_title')}\n\n"
            f"{tm.get_text(lang, 'admin_broadcast_button_enabled_line')}: <b>{status}</b>\n"
            f"{tm.get_text(lang, 'admin_broadcast_button_text_line')}: <b>{text_value}</b>\n"
            f"{tm.get_text(lang, 'admin_broadcast_button_url_line')}: <b>{url_value}</b>\n"
            f"{tm.get_text(lang, 'admin_broadcast_button_style_line')}: <b>{_broadcast_button_style_title(lang, style)}</b>\n"
            f"{tm.get_text(lang, 'admin_broadcast_button_emoji_line')}: {emoji_value}"
        )

    def _broadcast_button_markup(state: Dict[str, Any]) -> Optional[InlineKeyboardMarkup]:
        if not state.get("button_enabled"):
            return None
        text = (state.get("button_text") or "").strip()
        url = (state.get("button_url") or "").strip()
        if not text or not url:
            return None
        style = state.get("button_style", "primary")
        button_kwargs: Dict[str, Any] = {"text": text, "url": url}
        if style in {"primary", "success", "danger"}:
            button_kwargs["style"] = style
        emoji = (state.get("button_emoji_id") or "").strip()
        if emoji:
            button_kwargs["icon_custom_emoji_id"] = emoji
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(**button_kwargs)]])

    def _admin_broadcast_keyboard(lang: str, state: Dict[str, Any]) -> InlineKeyboardMarkup:
        audience = state.get("broadcast_audience", "broadcast_enabled")
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=tm.get_text(lang, "admin_broadcast_start_button"), callback_data=CallbackAdminBroadcastStart)],
                [
                    InlineKeyboardButton(
                        text=tm.get_text(lang, "admin_broadcast_audience_button") % _broadcast_audience_title(lang, audience),
                        callback_data=f"{CallbackAdminBroadcastAudience}?mode=cycle",
                    )
                ],
                [InlineKeyboardButton(text=tm.get_text(lang, "admin_broadcast_button_menu_button"), callback_data=CallbackAdminBroadcastButton)],
                [
                    InlineKeyboardButton(text=tm.get_text(lang, "admin_broadcast_test_button"), callback_data=CallbackAdminBroadcastTest),
                    InlineKeyboardButton(text=tm.get_text(lang, "admin_broadcast_send_button"), callback_data=CallbackAdminBroadcastSend),
                ],
                [
                    InlineKeyboardButton(text=tm.get_text(lang, "admin_broadcast_cancel_button"), callback_data=CallbackAdminBroadcastCancel),
                    InlineKeyboardButton(
                        text=tm.get_text(lang, "back_button"),
                        callback_data=CallbackAdminPanel,
                        style="primary",
                        icon_custom_emoji_id=_button_emoji_id(lang, "back_button"),
                    ),
                ],
            ]
        )

    def _admin_broadcast_button_keyboard(lang: str, state: Dict[str, Any]) -> InlineKeyboardMarkup:
        style = state.get("button_style", "primary")
        status_key = "admin_broadcast_button_toggle_on" if state.get("button_enabled") else "admin_broadcast_button_toggle_off"
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=tm.get_text(lang, status_key), callback_data=CallbackAdminBroadcastButtonToggle)],
                [
                    InlineKeyboardButton(text=tm.get_text(lang, "admin_broadcast_button_text_button"), callback_data=CallbackAdminBroadcastButtonText),
                    InlineKeyboardButton(text=tm.get_text(lang, "admin_broadcast_button_url_button"), callback_data=CallbackAdminBroadcastButtonUrl),
                ],
                [
                    InlineKeyboardButton(text=tm.get_text(lang, "admin_broadcast_button_emoji_button"), callback_data=CallbackAdminBroadcastButtonEmoji),
                    InlineKeyboardButton(text=tm.get_text(lang, "admin_broadcast_button_emoji_clear_button"), callback_data=CallbackAdminBroadcastButtonEmojiClear),
                ],
                [
                    InlineKeyboardButton(
                        text=tm.get_text(lang, "admin_broadcast_button_style_button") % _broadcast_button_style_title(lang, style),
                        callback_data=CallbackAdminBroadcastButtonStyle,
                    )
                ],
                [InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackAdminBroadcast, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))],
            ]
        )

    def _language_keyboard(lang: str, back_callback: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="Русский", callback_data=f"{CallbackLanguage}?v=ru&b={back_callback}"),
                    InlineKeyboardButton(text="English", callback_data=f"{CallbackLanguage}?v=en&b={back_callback}"),
                ],
                [
                    InlineKeyboardButton(
                        text=tm.get_text(lang, "back_button"),
                        callback_data=back_callback,
                        style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"),
                    )
                ],
            ]
        )

    def _settings_keyboard(customer: Customer, lang: str) -> InlineKeyboardMarkup:
        notifications_state = tm.get_text(lang, "settings_on") if customer.notifications_enabled else tm.get_text(lang, "settings_off")
        broadcast_state = tm.get_text(lang, "settings_on") if customer.broadcast_enabled else tm.get_text(lang, "settings_off")
        notifications_emoji_key = "settings_notifications_on" if customer.notifications_enabled else "settings_notifications_off"
        broadcast_emoji_key = "settings_broadcast_on" if customer.broadcast_enabled else "settings_broadcast_off"
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=tm.get_text(lang, "settings_language_button"),
                        callback_data=CallbackSettingsLanguage,
                        icon_custom_emoji_id=_button_emoji_id(lang, "settings_language_button"),
                    )
                ],
                [
                    InlineKeyboardButton(
                        text=tm.get_text(lang, "settings_notifications_button") % notifications_state,
                        callback_data=CallbackSettingsToggleNotifications,
                        icon_custom_emoji_id=_button_emoji_id(lang, notifications_emoji_key),
                    )
                ],
                [
                    InlineKeyboardButton(
                        text=tm.get_text(lang, "settings_broadcast_button") % broadcast_state,
                        callback_data=CallbackSettingsToggleBroadcast,
                        icon_custom_emoji_id=_button_emoji_id(lang, broadcast_emoji_key),
                    )
                ],
                [InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackStart, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))],
            ]
        )

    async def _admin_prices_text(lang: str) -> str:
        rows = await price_repo.list_all()
        if not rows:
            return tm.get_text(lang, "admin_prices_empty")
        lines = []
        for key in PRICE_FIELD_ORDER:
            matched = next((item for item in rows if item.key == key), None)
            if not matched:
                continue
            label = PRICE_FIELD_LABELS.get(key, key)
            lines.append(f"• <b>{label}</b>: <code>{matched.value}</code>")
        return "\n".join(lines)

    def _admin_prices_keyboard(lang: str) -> InlineKeyboardMarkup:
        rows: List[List[InlineKeyboardButton]] = []
        for key in PRICE_FIELD_ORDER:
            label = PRICE_FIELD_LABELS.get(key, key)
            rows.append([InlineKeyboardButton(text=label, callback_data=f"{CallbackAdminPriceEdit}?key={key}")])
        rows.append([InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackAdminPanel, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    def _pricing_text(lang: str) -> str:
        return tm.get_text(lang, "pricing_info")

    def _selected_plan_text(lang: str, month: int, plan: str, amount: int) -> str:
        if plan == "duo":
            plan_name = "Duo"
        elif plan == "family":
            plan_name = "Family"
        else:
            plan_name = tm.get_text(lang, f"month_{month}")
        return tm.get_text(lang, "payment_method_prompt") % (plan_name, amount)

    def _topup_text(lang: str, gb: int, stars: int) -> str:
        tpl = tm.get_text(lang, "add_traffic_dynamic")
        if "%s" in tpl:
            try:
                return tpl % (gb, stars)
            except Exception:  # noqa: BLE001
                pass
        return f"➕ {gb} GB ({stars}⭐)"

    def _gift_duration_keyboard(lang: str) -> InlineKeyboardMarkup:
        rows: List[List[InlineKeyboardButton]] = []
        months = (1, 3, 6, 12)
        current_row: List[InlineKeyboardButton] = []
        for month in months:
            price = int(getattr(config, f"price_{month}", 0))
            if price <= 0:
                continue
            current_row.append(
                InlineKeyboardButton(
                    text=f"{tm.get_text(lang, f'month_{month}')} · {price}₽",
                    callback_data=f"{CallbackGiftSelect}?month={month}",
                )
            )
            if len(current_row) == 2:
                rows.append(current_row)
                current_row = []
        if current_row:
            rows.append(current_row)
        rows.append([InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackBuy, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    def _agift_duration_keyboard(lang: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text=tm.get_text(lang, "gift_1_day"), callback_data=f"{CallbackAdminGiftDuration}?days=1"),
                    InlineKeyboardButton(text=tm.get_text(lang, "gift_3_days"), callback_data=f"{CallbackAdminGiftDuration}?days=3"),
                    InlineKeyboardButton(text=tm.get_text(lang, "gift_14_days"), callback_data=f"{CallbackAdminGiftDuration}?days=14"),
                ],
                [
                    InlineKeyboardButton(text=tm.get_text(lang, "month_1"), callback_data=f"{CallbackAdminGiftDuration}?month=1"),
                    InlineKeyboardButton(text=tm.get_text(lang, "month_3"), callback_data=f"{CallbackAdminGiftDuration}?month=3"),
                    InlineKeyboardButton(text=tm.get_text(lang, "month_6"), callback_data=f"{CallbackAdminGiftDuration}?month=6"),
                ],
                [InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackAdminPanel, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))],
            ]
        )

    def _agift_tag_keyboard(lang: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text=tm.get_text(lang, "agift_tag_sub_button"), callback_data=f"{CallbackAdminGiftTag}?tag=sub"),
                    InlineKeyboardButton(text=tm.get_text(lang, "agift_tag_gift_button"), callback_data=f"{CallbackAdminGiftTag}?tag=gift"),
                ],
                [InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackAdminGift, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))],
            ]
        )

    def _gift_user_picker_keyboard(lang: str, request_id: int) -> ReplyKeyboardMarkup:
        return ReplyKeyboardMarkup(
            keyboard=[
                [
                    KeyboardButton(
                        text=tm.get_text(lang, "gift_pick_user_button"),
                        request_users=KeyboardButtonRequestUsers(
                            request_id=request_id,
                            user_is_bot=False,
                            max_quantity=1,
                            request_name=True,
                            request_username=True,
                        ),
                    )
                ]
            ],
            resize_keyboard=True,
            one_time_keyboard=True,
        )

    def _extract_shared_user(message: Message) -> Tuple[Optional[int], Optional[str]]:
        if message.users_shared and message.users_shared.users:
            first = message.users_shared.users[0]
            display = first.username or first.first_name or str(first.user_id)
            if display and not display.startswith("@") and first.username:
                display = f"@{display}"
            return int(first.user_id), display
        if message.user_shared:
            shared: UserShared = message.user_shared
            return int(shared.user_id), str(shared.user_id)
        return None, None

    async def _deliver_pending_gift_notifications(telegram_id: int) -> None:
        pending = await gift_notification_repo.list_pending_by_recipient(telegram_id, limit=20)
        if not pending:
            return
        delivered_ids: List[int] = []
        for item in pending:
            try:
                await bot.send_message(telegram_id, item.message, parse_mode="HTML")
                delivered_ids.append(item.id)
            except Exception as err:  # noqa: BLE001
                logger.warning("failed to deliver pending gift notification id=%s: %s", item.id, err)
        if delivered_ids:
            await gift_notification_repo.mark_delivered(delivered_ids)

    async def _gift_payment_keyboard(
        lang: str,
        month: int,
        recipient_id: int,
        allow_stars: bool,
    ) -> InlineKeyboardMarkup:
        buttons: List[List[InlineKeyboardButton]] = []
        if config.crypto_pay_enabled:
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=tm.get_text(lang, "crypto_button"),
                        callback_data=f"payment?m={month}&i=c&g=1&u={recipient_id}",
                        icon_custom_emoji_id=_button_emoji_id(lang, "crypto_button"),
                    )
                ]
            )
        if config.yookasa_enabled:
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=tm.get_text(lang, "card_button"),
                        callback_data=f"payment?m={month}&i=y&g=1&u={recipient_id}",
                    )
                ]
            )
        if config.platega_enabled and config.platega_merchant_id and config.platega_secret:
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=tm.get_text(lang, "sbp_button") or "SBP",
                        callback_data=f"payment?m={month}&i=p&g=1&u={recipient_id}",
                        icon_custom_emoji_id=_button_emoji_id(lang, "sbp_button"),
                    )
                ]
            )
        if allow_stars and config.telegram_stars_enabled:
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=tm.get_text(lang, "stars_button"),
                        callback_data=f"payment?m={month}&i=t&g=1&u={recipient_id}",
                        icon_custom_emoji_id=_button_emoji_id(lang, "stars_button"),
                    )
                ]
            )
        buttons.append([InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackGiftMenu, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))])
        return InlineKeyboardMarkup(inline_keyboard=buttons)

    async def _is_stars_allowed_for_customer(telegram_id: int) -> bool:
        allow_stars = config.telegram_stars_enabled
        if config.require_paid_purchase_for_stars:
            customer = await customer_repo.find_by_telegram_id(telegram_id)
            if not customer:
                return False
            paid = await purchase_repo.find_successful_paid_purchase_by_customer(customer.id)
            return paid is not None
        return allow_stars

    async def _start_duo_member_pick_flow(
        callback: CallbackQuery,
        lang: str,
        month: int,
        amount: int,
    ) -> None:
        request_id = random.randint(1_000_000, 9_999_999)
        gift_state[callback.from_user.id] = {
            "mode": "duo",
            "month": month,
            "amount": amount,
            "request_id": request_id,
            "member_ids": [],
            "member_names": [],
        }
        await callback.message.answer(
            tm.get_text(lang, "duo_pick_user_prompt"),
            reply_markup=_gift_user_picker_keyboard(lang, request_id),
            parse_mode="HTML",
        )

    def _parse_referrer_id(start_text: Optional[str], own_telegram_id: int) -> Optional[int]:
        if not start_text:
            return None
        parts = start_text.split(maxsplit=1)
        if len(parts) < 2:
            return None
        start_arg = parts[1].strip()
        if not start_arg.startswith("ref_"):
            return None
        code = start_arg.replace("ref_", "", 1)
        try:
            referrer_id = int(code)
        except ValueError:
            logger.warning("invalid referral code: %s", code)
            return None
        if referrer_id == own_telegram_id:
            return None
        return referrer_id

    def _build_captcha_challenge(lang: str) -> Tuple[str, InlineKeyboardMarkup, str, str]:
        pool = [
            ("cherry", "🍒"),
            ("strawberry", "🍓"),
            ("grape", "🍇"),
            ("lemon", "🍋"),
            ("melon", "🍉"),
            ("apple", "🍎"),
        ]
        options = random.sample(pool, 3)
        target_key, target_emoji = random.choice(options)
        random.shuffle(options)
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text=options[0][1], callback_data=f"{CallbackCaptcha}:{options[0][0]}"),
                    InlineKeyboardButton(text=options[1][1], callback_data=f"{CallbackCaptcha}:{options[1][0]}"),
                    InlineKeyboardButton(text=options[2][1], callback_data=f"{CallbackCaptcha}:{options[2][0]}"),
                ]
            ]
        )
        prompt_tpl = tm.get_text(lang, "captcha_prompt")
        prompt_text = prompt_tpl % target_emoji if "%s" in prompt_tpl else prompt_tpl
        return prompt_text, markup, target_key, target_emoji

    async def _notify_owners_about_first_start(
        customer: Customer,
        username: Optional[str],
        referrer_customer: Optional[Customer],
        referrer_id_raw: Optional[int],
    ) -> None:
        username_text = f"@{username}" if username else "-"
        ref_text = "—"
        if referrer_customer:
            ref_username = f"@{referrer_customer.username}" if referrer_customer.username else "-"
            ref_text = f"{ref_username} (id {referrer_customer.telegram_id})"
        elif referrer_id_raw:
            ref_text = f"id {referrer_id_raw}"
        text = (
            "🆕 <b>Первый вход в бота</b>\n"
            f"ID: <code>{customer.telegram_id}</code>\n"
            f"Username: {html.escape(username_text)}\n"
            f"Реферал: {html.escape(ref_text)}\n"
            f"Язык: <b>{html.escape(customer.language or config.default_language)}</b>\n"
            f"Время (UTC): <b>{datetime.utcnow().strftime('%d.%m.%Y %H:%M:%S')}</b>"
        )
        await _send_log_message(text)

    async def _notify_owners_about_trial(
        customer: Customer,
        username: Optional[str],
        referrer_customer: Optional[Customer],
    ) -> None:
        username_text = f"@{username}" if username else "-"
        until = customer.expire_at.strftime("%d.%m.%Y %H:%M") if customer.expire_at else "-"
        ref_text = "—"
        if referrer_customer:
            ref_username = f"@{referrer_customer.username}" if referrer_customer.username else "-"
            ref_text = f"{ref_username} (id {referrer_customer.telegram_id})"
        text = (
            "🎁 <b>Активирован пробный период</b>\n"
            f"ID: <code>{customer.telegram_id}</code>\n"
            f"Username: {html.escape(username_text)}\n"
            f"До: <b>{until}</b>\n"
            f"Реферал: {html.escape(ref_text)}\n"
            f"Время (UTC): <b>{datetime.utcnow().strftime('%d.%m.%Y %H:%M:%S')}</b>"
        )
        await _send_log_message(text)

    def _is_valid_broadcast_button_url(url: str) -> bool:
        lowered = (url or "").strip().lower()
        return lowered.startswith("https://") or lowered.startswith("http://") or lowered.startswith("tg://")

    async def _resolve_broadcast_target_ids(audience: str) -> List[int]:
        now_utc = datetime.utcnow()
        if audience == "all":
            return await customer_repo.list_all_telegram_ids()
        if audience == "active":
            return await customer_repo.list_active_telegram_ids(now_utc)
        if audience == "inactive":
            return await customer_repo.list_inactive_telegram_ids(now_utc)
        return await customer_repo.list_broadcast_enabled_telegram_ids()

    async def _copy_broadcast_message(target_chat_id: int, state: Dict[str, Any]) -> None:
        source_chat_id = state.get("broadcast_source_chat_id")
        source_message_id = state.get("broadcast_source_message_id")
        if not source_chat_id or not source_message_id:
            raise RuntimeError("broadcast source message is not set")
        await bot.copy_message(
            chat_id=target_chat_id,
            from_chat_id=int(source_chat_id),
            message_id=int(source_message_id),
            reply_markup=_broadcast_button_markup(state),
        )

    async def _save_broadcast_source_message(message: Message, lang: str) -> None:
        state = _ensure_broadcast_state(message.from_user.id)
        state["broadcast_source_chat_id"] = message.chat.id
        state["broadcast_source_message_id"] = message.message_id
        state["step"] = "broadcast_idle"
        await message.answer(
            tm.get_text(lang, "admin_broadcast_message_saved"),
            reply_markup=_admin_broadcast_keyboard(lang, state),
            parse_mode="HTML",
        )
        try:
            await _copy_broadcast_message(message.from_user.id, state)
        except Exception as err:  # noqa: BLE001
            logger.warning("broadcast preview copy failed: %s", err)
            await message.answer(tm.get_text(lang, "admin_broadcast_preview_failed"), parse_mode="HTML")

    async def _show_start_home(message: Message, customer: Customer, lang: str) -> None:
        customer = await payment_service.refresh_customer_subscription(customer)
        markup = start_keyboard(customer, lang, tm)
        temp_msg = await message.answer("...", reply_markup=ReplyKeyboardRemove(remove_keyboard=True))
        try:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=temp_msg.message_id)
        except Exception:
            pass
        await message.answer(
            tm.get_text(lang, "greeting"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=markup),
            parse_mode="HTML",
        )
        await _deliver_pending_gift_notifications(customer.telegram_id)

    @router.message(CommandStart())
    async def start_command(message: Message) -> None:
        lang = message.from_user.language_code or config.default_language
        existing_customer = await customer_repo.find_by_telegram_id(message.chat.id)
        is_new_customer = existing_customer is None
        customer = existing_customer
        if is_new_customer:
            customer = await customer_repo.find_or_create(message.chat.id, lang)

        if is_new_customer:
            parsed_referrer_id = _parse_referrer_id(message.text, customer.telegram_id)
            prompt_text, captcha_markup, target_key, _ = _build_captcha_challenge(lang)
            pending_captcha[customer.telegram_id] = {"referrer_id": parsed_referrer_id, "target": target_key}
            await message.answer(
                prompt_text,
                reply_markup=captcha_markup,
                parse_mode="HTML",
            )
            return

        if customer.telegram_id in pending_captcha:
            prompt_text, captcha_markup, target_key, _ = _build_captcha_challenge(lang)
            pending_captcha[customer.telegram_id]["target"] = target_key
            await message.answer(
                prompt_text,
                reply_markup=captcha_markup,
                parse_mode="HTML",
            )
            return

        lang = customer.language or lang
        if not customer.language_selected:
            await message.answer(
                tm.get_text(lang, "language_choose_prompt"),
                reply_markup=_language_keyboard(lang, CallbackStart),
                parse_mode="HTML",
            )
            return

        await _show_start_home(message, customer, lang)

    @router.callback_query(F.data.startswith(f"{CallbackCaptcha}:"))
    async def captcha_callback(callback: CallbackQuery) -> None:
        customer = await customer_repo.find_by_telegram_id(callback.from_user.id)
        if not customer:
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        state = pending_captcha.get(customer.telegram_id)
        if not state:
            await callback.answer()
            return
        selected = callback.data.split(":", 1)[1] if ":" in callback.data else ""
        target_key = state.get("target", "")
        if selected != target_key:
            prompt_text, captcha_markup, new_target_key, target_emoji = _build_captcha_challenge(lang)
            state["target"] = new_target_key
            wrong_tpl = tm.get_text(lang, "captcha_wrong")
            wrong_text = wrong_tpl % target_emoji if "%s" in wrong_tpl else wrong_tpl
            await callback.answer(wrong_text, show_alert=True)
            await callback.message.edit_text(prompt_text, reply_markup=captcha_markup, parse_mode="HTML")
            return

        pending_captcha.pop(customer.telegram_id, None)
        parsed_referrer_id = state.get("referrer_id")
        referrer_customer: Optional[Customer] = None
        if parsed_referrer_id and parsed_referrer_id != customer.telegram_id:
            existing_for_referee = await referral_repo.find_by_referee(customer.telegram_id)
            existing_referral = await referral_repo.find_by_pair(parsed_referrer_id, customer.telegram_id)
            referrer_customer = await customer_repo.find_by_telegram_id(parsed_referrer_id)
            if referrer_customer and not existing_referral and not existing_for_referee:
                await referral_repo.create(parsed_referrer_id, customer.telegram_id)
                try:
                    await payment_service.grant_referral_signup_bonus(referrer_customer.telegram_id)
                except Exception as err:  # noqa: BLE001
                    logger.warning("failed to grant signup referral bonus: %s", err)
                try:
                    await bot.send_message(
                        referrer_customer.telegram_id,
                        tm.get_text(referrer_customer.language, "referral_new_referral")
                        % (
                            f"@{callback.from_user.username}"
                            if callback.from_user.username
                            else f"id {callback.from_user.id}",
                            config.referral_signup_days,
                            config.referral_purchase_days,
                        ),
                        parse_mode="HTML",
                    )
                except Exception as err:  # noqa: BLE001
                    logger.debug("failed to send referral notify: %s", err)
                logger.info("referral created referrer=%s referee=%s", parsed_referrer_id, customer.telegram_id)

        await _notify_owners_about_first_start(
            customer=customer,
            username=callback.from_user.username,
            referrer_customer=referrer_customer,
            referrer_id_raw=parsed_referrer_id,
        )
        lang = customer.language or lang
        if not customer.language_selected:
            await callback.message.edit_text(
                tm.get_text(lang, "language_choose_prompt"),
                reply_markup=_language_keyboard(lang, CallbackStart),
                parse_mode="HTML",
            )
            await callback.answer(tm.get_text(lang, "captcha_ok"))
            return

        customer = await payment_service.refresh_customer_subscription(customer)
        markup = start_keyboard(customer, lang, tm)
        await callback.message.edit_text(
            tm.get_text(lang, "greeting"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=markup),
            parse_mode="HTML",
        )
        await _deliver_pending_gift_notifications(customer.telegram_id)
        await callback.answer(tm.get_text(lang, "captcha_ok"))

    @router.callback_query(F.data == CallbackStart)
    async def start_callback(callback: CallbackQuery) -> None:
        customer = await customer_repo.find_by_telegram_id(callback.from_user.id)
        if not customer:
            return
        lang = customer.language or callback.from_user.language_code or config.default_language
        if not customer.language_selected:
            await callback.message.edit_text(
                tm.get_text(lang, "language_choose_prompt"),
                reply_markup=_language_keyboard(lang, CallbackStart),
                parse_mode="HTML",
            )
            await callback.answer()
            return
        customer = await payment_service.refresh_customer_subscription(customer)
        markup = start_keyboard(customer, lang, tm)
        await callback.message.edit_text(
            tm.get_text(lang, "greeting"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=markup),
            parse_mode="HTML",
        )
        await _deliver_pending_gift_notifications(customer.telegram_id)
        await callback.answer()

    @router.callback_query(F.data.startswith(f"{CallbackLanguage}?"))
    async def language_callback(callback: CallbackQuery) -> None:
        params = parse_callback_data(callback.data)
        selected = (params.get("v") or "").lower()
        back = params.get("b") or CallbackStart
        if selected not in {"ru", "en"}:
            await callback.answer()
            return
        customer = await customer_repo.find_by_telegram_id(callback.from_user.id)
        if not customer:
            await callback.answer()
            return
        await customer_repo.update_fields(
            customer.id,
            {"language": selected, "language_selected": 1},
        )
        customer = await customer_repo.find_by_telegram_id(callback.from_user.id)
        lang = selected
        if back == CallbackSettings:
            await callback.message.edit_text(
                tm.get_text(lang, "settings_title"),
                reply_markup=_settings_keyboard(customer, lang),
                parse_mode="HTML",
            )
        else:
            customer = await payment_service.refresh_customer_subscription(customer)
            markup = start_keyboard(customer, lang, tm)
            await callback.message.edit_text(
                tm.get_text(lang, "greeting"),
                reply_markup=InlineKeyboardMarkup(inline_keyboard=markup),
                parse_mode="HTML",
            )
            await _deliver_pending_gift_notifications(customer.telegram_id)
        await callback.answer()

    @router.callback_query(F.data == CallbackSettings)
    async def settings_callback(callback: CallbackQuery) -> None:
        customer = await customer_repo.find_by_telegram_id(callback.from_user.id)
        if not customer:
            await callback.answer()
            return
        lang = customer.language or callback.from_user.language_code or config.default_language
        await callback.message.edit_text(
            tm.get_text(lang, "settings_title"),
            reply_markup=_settings_keyboard(customer, lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackSettingsLanguage)
    async def settings_language_callback(callback: CallbackQuery) -> None:
        customer = await customer_repo.find_by_telegram_id(callback.from_user.id)
        if not customer:
            await callback.answer()
            return
        lang = customer.language or callback.from_user.language_code or config.default_language
        await callback.message.edit_text(
            tm.get_text(lang, "language_choose_prompt"),
            reply_markup=_language_keyboard(lang, CallbackSettings),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackSettingsToggleNotifications)
    async def settings_toggle_notifications_callback(callback: CallbackQuery) -> None:
        customer = await customer_repo.find_by_telegram_id(callback.from_user.id)
        if not customer:
            await callback.answer()
            return
        new_value = 0 if customer.notifications_enabled else 1
        await customer_repo.update_fields(customer.id, {"notifications_enabled": new_value})
        customer = await customer_repo.find_by_telegram_id(callback.from_user.id)
        lang = customer.language or callback.from_user.language_code or config.default_language
        await callback.message.edit_text(
            tm.get_text(lang, "settings_title"),
            reply_markup=_settings_keyboard(customer, lang),
            parse_mode="HTML",
        )
        await callback.answer(
            tm.get_text(lang, "settings_notifications_enabled")
            if customer.notifications_enabled
            else tm.get_text(lang, "settings_notifications_disabled")
        )

    @router.callback_query(F.data == CallbackSettingsToggleBroadcast)
    async def settings_toggle_broadcast_callback(callback: CallbackQuery) -> None:
        customer = await customer_repo.find_by_telegram_id(callback.from_user.id)
        if not customer:
            await callback.answer()
            return
        new_value = 0 if customer.broadcast_enabled else 1
        await customer_repo.update_fields(customer.id, {"broadcast_enabled": new_value})
        customer = await customer_repo.find_by_telegram_id(callback.from_user.id)
        lang = customer.language or callback.from_user.language_code or config.default_language
        await callback.message.edit_text(
            tm.get_text(lang, "settings_title"),
            reply_markup=_settings_keyboard(customer, lang),
            parse_mode="HTML",
        )
        await callback.answer(
            tm.get_text(lang, "settings_broadcast_enabled")
            if customer.broadcast_enabled
            else tm.get_text(lang, "settings_broadcast_disabled")
        )

    @router.message(Command("connect"))
    async def connect_command(message: Message) -> None:
        customer = await customer_repo.find_by_telegram_id(message.chat.id)
        if not customer:
            return
        customer = await payment_service.refresh_customer_subscription(customer)
        lang = message.from_user.language_code or config.default_language
        traffic_text, used, limit, has_usage = await get_traffic_usage(sync_service, customer, tm, lang)
        markup = await _connect_markup(customer, lang, tm, used, limit, has_usage)
        await message.answer(
            build_connect_text(customer, lang, tm, traffic_text),
            reply_markup=markup,
            parse_mode="HTML",
        )

    @router.callback_query(F.data == CallbackConnect)
    async def connect_callback(callback: CallbackQuery) -> None:
        customer = await customer_repo.find_by_telegram_id(callback.message.chat.id)
        if not customer:
            return
        customer = await payment_service.refresh_customer_subscription(customer)
        lang = callback.from_user.language_code or config.default_language
        traffic_text, used, limit, has_usage = await get_traffic_usage(sync_service, customer, tm, lang)
        markup = await _connect_markup(customer, lang, tm, used, limit, has_usage)
        await callback.message.edit_text(
            build_connect_text(customer, lang, tm, traffic_text),
            reply_markup=markup,
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackConnectInstructions)
    async def connect_instructions_callback(callback: CallbackQuery) -> None:
        customer = await customer_repo.find_by_telegram_id(callback.message.chat.id)
        if not customer:
            return
        customer = await payment_service.refresh_customer_subscription(customer)
        lang = callback.from_user.language_code or config.default_language
        await callback.message.edit_text(
            build_connect_instructions_text(customer, lang, tm),
            reply_markup=_connect_instructions_markup(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.message(Command("promo"))
    async def promo_admin_menu(message: Message) -> None:
        if not _is_admin(message.from_user.id):
            return
        lang = message.from_user.language_code or config.default_language
        panel_state.pop(message.from_user.id, None)
        promo_admin_state.pop(message.from_user.id, None)
        pending_promo.discard(message.from_user.id)
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text=tm.get_text(lang, "promo_admin_create"), callback_data=CallbackPromoAdminCreate),
                    InlineKeyboardButton(text=tm.get_text(lang, "promo_admin_list"), callback_data=CallbackPromoAdminList),
                ],
                [
                    InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackAdminPanel, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button")),
                ],
            ]
        )
        await message.answer(tm.get_text(lang, "promo_admin_prompt"), reply_markup=keyboard, parse_mode="HTML")

    @router.message(Command("stats"))
    async def stats_command(message: Message) -> None:
        if not _is_admin(message.from_user.id):
            return
        lang = message.from_user.language_code or config.default_language
        day = _stats_day_in_ekb()
        text = await stats_service.build_report_for_local_day(day)
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=tm.get_text(lang, "stats_refresh_button"), callback_data=CallbackStats)],
                [InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackStart, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))],
            ]
        )
        await message.answer(text, reply_markup=keyboard, parse_mode="HTML")

    @router.message(Command("admin"))
    async def admin_command(message: Message) -> None:
        if not _is_admin(message.from_user.id):
            return
        lang = message.from_user.language_code or config.default_language
        panel_state.pop(message.from_user.id, None)
        promo_admin_state.pop(message.from_user.id, None)
        pending_promo.discard(message.from_user.id)
        await message.answer(
            tm.get_text(lang, "admin_panel_title"),
            reply_markup=_admin_main_keyboard(lang),
            parse_mode="HTML",
        )

    @router.callback_query(F.data == CallbackAdminPanel)
    async def admin_panel_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        panel_state.pop(callback.from_user.id, None)
        promo_admin_state.pop(callback.from_user.id, None)
        pending_promo.discard(callback.from_user.id)
        await callback.message.edit_text(
            tm.get_text(lang, "admin_panel_title"),
            reply_markup=_admin_main_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminUsers)
    async def admin_users_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        await callback.message.edit_text(
            tm.get_text(lang, "admin_users_title"),
            reply_markup=_admin_users_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminUsersSummary)
    async def admin_users_summary_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        now_utc = datetime.utcnow()
        total_users = await customer_repo.count_all()
        active_users = await customer_repo.count_active(now_utc)
        expired_users = max(0, total_users - active_users)
        today_local = datetime.now(_timezone()).date()
        start_utc, end_utc = _local_day_bounds_utc(today_local)
        new_today = await customer_repo.count_new_in_period(start_utc, end_utc)
        text = (
            f"{tm.get_text(lang, 'admin_users_summary_title')}\n\n"
            f"{tm.get_text(lang, 'admin_users_total_label')}: <b>{total_users}</b>\n"
            f"{tm.get_text(lang, 'admin_users_active_label')}: <b>{active_users}</b>\n"
            f"{tm.get_text(lang, 'admin_users_expired_label')}: <b>{expired_users}</b>\n"
            f"{tm.get_text(lang, 'admin_users_new_today_label')}: <b>{new_today}</b>"
        )
        await callback.message.edit_text(
            text,
            reply_markup=_admin_users_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminUsersNew)
    async def admin_users_new_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        today_local = datetime.now(_timezone()).date()
        start_utc, end_utc = _local_day_bounds_utc(today_local)
        rows = await customer_repo.list_new_in_period(start_utc, end_utc, limit=30)
        lines = [tm.get_text(lang, "admin_users_new_today_title")]
        if rows:
            for item in rows:
                username = f"@{item.username}" if item.username else "-"
                created = item.created_at.strftime("%H:%M")
                lines.append(f"• <code>{item.telegram_id}</code> {html.escape(username)} ({created} UTC)")
        else:
            lines.append(tm.get_text(lang, "admin_users_new_empty"))
        await callback.message.edit_text(
            "\n".join(lines),
            reply_markup=_admin_users_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminUsersFind)
    async def admin_users_find_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        panel_state[callback.from_user.id] = {"step": "await_user_lookup"}
        await callback.message.edit_text(
            tm.get_text(lang, "admin_users_find_prompt"),
            reply_markup=_admin_users_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminUsersDelete)
    async def admin_users_delete_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        panel_state[callback.from_user.id] = {"step": "await_user_delete"}
        await callback.message.edit_text(
            tm.get_text(lang, "admin_users_delete_prompt"),
            reply_markup=_admin_users_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminSubs)
    async def admin_subs_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        await callback.message.edit_text(
            tm.get_text(lang, "admin_subscriptions_title"),
            reply_markup=_admin_subs_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminSubsExtend)
    async def admin_subs_extend_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        panel_state[callback.from_user.id] = {"step": "await_sub_extend"}
        await callback.message.edit_text(
            tm.get_text(lang, "admin_sub_extend_prompt"),
            reply_markup=_admin_subs_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminSubsForever)
    async def admin_subs_forever_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        panel_state[callback.from_user.id] = {"step": "await_sub_forever"}
        await callback.message.edit_text(
            tm.get_text(lang, "admin_sub_forever_prompt"),
            reply_markup=_admin_subs_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminSubsDisable)
    async def admin_subs_disable_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        panel_state[callback.from_user.id] = {"step": "await_sub_disable"}
        await callback.message.edit_text(
            tm.get_text(lang, "admin_sub_disable_prompt"),
            reply_markup=_admin_subs_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminBroadcast)
    async def admin_broadcast_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        state = _ensure_broadcast_state(callback.from_user.id)
        state["step"] = "broadcast_idle"
        await callback.message.edit_text(
            _broadcast_panel_text(lang, state),
            reply_markup=_admin_broadcast_keyboard(lang, state),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminBroadcastStart)
    async def admin_broadcast_start_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        state = _ensure_broadcast_state(callback.from_user.id)
        state["step"] = "await_broadcast_source"
        await callback.message.edit_text(
            tm.get_text(lang, "admin_broadcast_prompt"),
            reply_markup=_admin_broadcast_keyboard(lang, state),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data.startswith(f"{CallbackAdminBroadcastAudience}?"))
    async def admin_broadcast_audience_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        state = _ensure_broadcast_state(callback.from_user.id)
        current = state.get("broadcast_audience", "broadcast_enabled")
        variants = _broadcast_audience_keys()
        try:
            idx = variants.index(current)
        except ValueError:
            idx = 0
        state["broadcast_audience"] = variants[(idx + 1) % len(variants)]
        await callback.message.edit_text(
            _broadcast_panel_text(lang, state),
            reply_markup=_admin_broadcast_keyboard(lang, state),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminBroadcastButton)
    async def admin_broadcast_button_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        state = _ensure_broadcast_state(callback.from_user.id)
        state["step"] = "broadcast_idle"
        await callback.message.edit_text(
            _broadcast_button_settings_text(lang, state),
            reply_markup=_admin_broadcast_button_keyboard(lang, state),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminBroadcastButtonToggle)
    async def admin_broadcast_button_toggle_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        state = _ensure_broadcast_state(callback.from_user.id)
        state["button_enabled"] = not bool(state.get("button_enabled"))
        state["step"] = "broadcast_idle"
        await callback.message.edit_text(
            _broadcast_button_settings_text(lang, state),
            reply_markup=_admin_broadcast_button_keyboard(lang, state),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminBroadcastButtonStyle)
    async def admin_broadcast_button_style_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        state = _ensure_broadcast_state(callback.from_user.id)
        current = state.get("button_style", "primary")
        styles = _broadcast_button_style_keys()
        try:
            idx = styles.index(current)
        except ValueError:
            idx = 0
        state["button_style"] = styles[(idx + 1) % len(styles)]
        state["step"] = "broadcast_idle"
        await callback.message.edit_text(
            _broadcast_button_settings_text(lang, state),
            reply_markup=_admin_broadcast_button_keyboard(lang, state),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminBroadcastButtonText)
    async def admin_broadcast_button_text_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        state = _ensure_broadcast_state(callback.from_user.id)
        state["step"] = "await_broadcast_button_text"
        await callback.message.edit_text(
            tm.get_text(lang, "admin_broadcast_button_text_prompt"),
            reply_markup=_admin_broadcast_button_keyboard(lang, state),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminBroadcastButtonUrl)
    async def admin_broadcast_button_url_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        state = _ensure_broadcast_state(callback.from_user.id)
        state["step"] = "await_broadcast_button_url"
        await callback.message.edit_text(
            tm.get_text(lang, "admin_broadcast_button_url_prompt"),
            reply_markup=_admin_broadcast_button_keyboard(lang, state),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminBroadcastButtonEmoji)
    async def admin_broadcast_button_emoji_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        state = _ensure_broadcast_state(callback.from_user.id)
        state["step"] = "await_broadcast_button_emoji"
        await callback.message.edit_text(
            tm.get_text(lang, "admin_broadcast_button_emoji_prompt"),
            reply_markup=_admin_broadcast_button_keyboard(lang, state),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminBroadcastButtonEmojiClear)
    async def admin_broadcast_button_emoji_clear_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        state = _ensure_broadcast_state(callback.from_user.id)
        state["button_emoji_id"] = ""
        state["step"] = "broadcast_idle"
        await callback.message.edit_text(
            _broadcast_button_settings_text(lang, state),
            reply_markup=_admin_broadcast_button_keyboard(lang, state),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminBroadcastTest)
    async def admin_broadcast_test_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        state = _ensure_broadcast_state(callback.from_user.id)
        if not _broadcast_source_ready(state):
            await callback.answer(tm.get_text(lang, "admin_broadcast_nothing"), show_alert=True)
            return
        if not _broadcast_button_ready(state):
            await callback.answer(tm.get_text(lang, "admin_broadcast_button_incomplete"), show_alert=True)
            return
        try:
            await _copy_broadcast_message(callback.from_user.id, state)
        except Exception as err:  # noqa: BLE001
            logger.warning("broadcast admin test failed: %s", err)
            await callback.answer(tm.get_text(lang, "admin_broadcast_test_failed"), show_alert=True)
            return
        await callback.answer(tm.get_text(lang, "admin_broadcast_test_done"), show_alert=True)

    @router.callback_query(F.data == CallbackAdminBroadcastSend)
    async def admin_broadcast_send_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        state = _ensure_broadcast_state(callback.from_user.id)
        if not _broadcast_source_ready(state):
            await callback.answer(tm.get_text(lang, "admin_broadcast_nothing"), show_alert=True)
            return
        if not _broadcast_button_ready(state):
            await callback.answer(tm.get_text(lang, "admin_broadcast_button_incomplete"), show_alert=True)
            return
        audience = state.get("broadcast_audience", "broadcast_enabled")
        user_ids = await _resolve_broadcast_target_ids(audience)
        success = 0
        failed = 0
        for user_id in user_ids:
            if user_id in config.blocked_telegram_ids:
                continue
            try:
                await _copy_broadcast_message(user_id, state)
                success += 1
            except Exception as err:  # noqa: BLE001
                logger.debug("broadcast send failed user=%s: %s", user_id, err)
                failed += 1
            await asyncio.sleep(0.04)
        state["step"] = "broadcast_idle"
        result_text = (
            tm.get_text(lang, "admin_broadcast_done") % (success, failed)
            + "\n"
            + f"{tm.get_text(lang, 'admin_broadcast_audience_line')}: <b>{_broadcast_audience_title(lang, audience)}</b>"
        )
        await callback.message.edit_text(
            result_text,
            reply_markup=_admin_broadcast_keyboard(lang, state),
            parse_mode="HTML",
        )
        await _send_log_message(
            "📢 <b>Рассылка завершена</b>\n"
            f"Админ: <code>{callback.from_user.id}</code>\n"
            f"Аудитория: <b>{html.escape(audience)}</b>\n"
            f"Успешно: <b>{success}</b>\n"
            f"Ошибок: <b>{failed}</b>"
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminBroadcastCancel)
    async def admin_broadcast_cancel_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        panel_state.pop(callback.from_user.id, None)
        await callback.message.edit_text(tm.get_text(lang, "admin_broadcast_cancelled"), reply_markup=_admin_main_keyboard(lang), parse_mode="HTML")
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminPrices)
    async def admin_prices_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        text = await _admin_prices_text(lang)
        await callback.message.edit_text(
            f"{tm.get_text(lang, 'admin_prices_title')}\n\n{text}",
            reply_markup=_admin_prices_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data.startswith(f"{CallbackAdminPriceEdit}?"))
    async def admin_price_edit_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        params = parse_callback_data(callback.data)
        key = params.get("key", "")
        if key not in PRICE_FIELD_ORDER:
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        panel_state[callback.from_user.id] = {"step": "await_price_value", "key": key}
        label = PRICE_FIELD_LABELS.get(key, key)
        current_value = getattr(config, key, 0)
        await callback.message.edit_text(
            tm.get_text(lang, "admin_price_edit_prompt") % (label, current_value),
            reply_markup=_admin_prices_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminReportTraffic)
    async def admin_report_traffic_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        day = _stats_day_in_ekb()
        text = await stats_service.build_traffic_users_report_for_local_day(day)
        await callback.message.edit_text(
            text,
            reply_markup=_admin_main_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackAdminReportFinance)
    async def admin_report_finance_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        day = _stats_day_in_ekb()
        text = await stats_service.build_financial_report_for_local_day(day)
        await callback.message.edit_text(
            text,
            reply_markup=_admin_main_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.message(Command("agift"))
    async def admin_gift_command(message: Message) -> None:
        if not _is_admin(message.from_user.id):
            return
        lang = message.from_user.language_code or config.default_language
        gift_state.pop(message.from_user.id, None)
        await message.answer(
            tm.get_text(lang, "agift_title"),
            reply_markup=_agift_duration_keyboard(lang),
            parse_mode="HTML",
        )

    @router.callback_query(F.data == CallbackAdminGift)
    async def admin_gift_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        gift_state.pop(callback.from_user.id, None)
        await callback.message.edit_text(
            tm.get_text(lang, "agift_title"),
            reply_markup=_agift_duration_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data.startswith(f"{CallbackAdminGiftDuration}?"))
    async def admin_gift_duration_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        params = parse_callback_data(callback.data)
        try:
            month = int(params.get("month", "0"))
        except ValueError:
            month = 0
        try:
            days = int(params.get("days", "0"))
        except ValueError:
            days = 0
        if month <= 0 and days <= 0:
            await callback.answer()
            return
        total_days = days if days > 0 else month * config.days_in_month
        gift_state[callback.from_user.id] = {
            "mode": "admin",
            "days": total_days,
            "months": month,
        }
        await callback.message.edit_text(
            tm.get_text(lang, "agift_choose_tag"),
            reply_markup=_agift_tag_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data.startswith(f"{CallbackAdminGiftTag}?"))
    async def admin_gift_tag_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        state = gift_state.get(callback.from_user.id)
        if not state or state.get("mode") != "admin":
            await callback.answer(tm.get_text(lang, "agift_state_expired"), show_alert=True)
            return
        params = parse_callback_data(callback.data)
        selected_tag = (params.get("tag") or "").strip().lower()
        if selected_tag not in {"sub", "gift"}:
            await callback.answer()
            return
        selected_tag_api = selected_tag.upper()
        request_id = random.randint(1_000_000, 9_999_999)
        state["tag"] = selected_tag_api
        state["request_id"] = request_id
        gift_state[callback.from_user.id] = state
        await callback.message.answer(
            tm.get_text(lang, "agift_tag_selected_prompt") % selected_tag_api,
            reply_markup=_gift_user_picker_keyboard(lang, request_id),
            parse_mode="HTML",
        )
        await callback.answer()

    async def _connect_markup(customer: Customer, lang: str, tm: TranslationManager, used: int, limit: int, has_usage: bool) -> InlineKeyboardMarkup:
        buttons = []
        if config.mini_app_url:
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=tm.get_text(lang, "connect_button"),
                        web_app={"url": config.mini_app_url},
                        style="primary",
                        icon_custom_emoji_id=_button_emoji_id(lang, "connect_button"),
                    )
                ]
            )
        elif config.is_web_app_link and customer.subscription_link and customer.expire_at and customer.expire_at > datetime.utcnow():
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=tm.get_text(lang, "connect_button"),
                        web_app={"url": customer.subscription_link},
                        style="primary",
                        icon_custom_emoji_id=_button_emoji_id(lang, "connect_button"),
                    )
                ]
            )
        buttons.append(
            [
                InlineKeyboardButton(
                    text=tm.get_text(lang, "connect_instructions_button"),
                    callback_data=CallbackConnectInstructions,
                )
            ]
        )
        if config.telegram_stars_enabled:
            topup_row: List[InlineKeyboardButton] = []
            topup_row.append(
                InlineKeyboardButton(
                    text=_topup_text(lang, 10, config.topup_10_price_stars),
                    callback_data=f"payment?plan=topup10&invoiceType=telegram&amount={config.topup_10_price_stars}&month=0",
                )
            )
            topup_row.append(
                InlineKeyboardButton(
                    text=_topup_text(lang, 20, config.topup_20_price_stars),
                    callback_data=f"payment?plan=topup20&invoiceType=telegram&amount={config.topup_20_price_stars}&month=0",
                )
            )
            buttons.append(topup_row)
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=_topup_text(lang, 50, config.topup_50_price_stars),
                        callback_data=f"payment?plan=topup50&invoiceType=telegram&amount={config.topup_50_price_stars}&month=0",
                    )
                ]
            )
        buttons.append([InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackStart, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))])
        return InlineKeyboardMarkup(inline_keyboard=buttons)

    def _connect_instructions_markup(lang: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackConnect, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))]
            ]
        )

    @router.message(Command("sync"))
    async def sync_command(message: Message) -> None:
        if not _is_admin(message.from_user.id):
            await message.answer("Access denied")
            return
        await message.answer("Sync started...")
        stats = await sync_service.sync()
        await message.answer(
            "Sync completed\n"
            f"fetched: {stats['fetched']}\n"
            f"with telegram_id: {stats['with_telegram_id']}\n"
            f"created: {stats['created']}\n"
            f"updated: {stats['updated']}\n"
            f"skipped (no telegram_id): {stats['skipped_without_telegram_id']}\n"
            f"skipped (duplicates): {stats['skipped_duplicates']}"
        )

    @router.callback_query(F.data == CallbackBuy)
    async def buy_callback(callback: CallbackQuery) -> None:
        lang = callback.from_user.language_code or config.default_language
        keyboard = price_keyboard(lang, tm)
        await callback.message.edit_text(
            _pricing_text(lang),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data.startswith(CallbackSell))
    async def sell_callback(callback: CallbackQuery) -> None:
        params = parse_callback_data(callback.data)
        lang = callback.from_user.language_code or config.default_language
        month = int(params.get("month", 1))
        plan = params.get("plan", "standard")
        amount = int(params.get("amount", 0))
        if plan == "duo":
            amount = config.price_duo
        elif plan == "family":
            amount = config.price_family
        if plan == "duo":
            await _start_duo_member_pick_flow(callback, lang, month, amount)
            await callback.answer()
            return

        allow_stars = await _is_stars_allowed_for_customer(callback.message.chat.id)

        keyboard = payment_methods_keyboard(
            lang=lang,
            tm=tm,
            month=month,
            amount=amount,
            plan=plan,
            allow_crypto=config.crypto_pay_enabled,
            allow_card=config.yookasa_enabled,
            allow_stars=allow_stars,
            allow_platega=config.platega_enabled and config.platega_merchant_id and config.platega_secret,
            tribute_url=config.tribute_payment_url if config.tribute_webhook_url else None,
        )
        await callback.message.edit_text(
            _selected_plan_text(lang, month, plan, amount),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data.startswith(f"{CallbackDuoMembers}?"))
    async def duo_members_callback(callback: CallbackQuery) -> None:
        params = parse_callback_data(callback.data)
        lang = callback.from_user.language_code or config.default_language
        try:
            month = int(params.get("month", "1"))
        except ValueError:
            month = 1
        amount = config.price_duo
        await _start_duo_member_pick_flow(callback, lang, month, amount)
        await callback.answer()

    @router.callback_query(F.data.startswith(CallbackPayment))
    async def payment_callback(callback: CallbackQuery) -> None:
        params = parse_callback_data(callback.data)
        try:
            month = int(params.get("month") or params.get("m") or "1")
        except ValueError:
            month = 1
        plan = params.get("plan", "standard")
        invoice_type = params.get("invoiceType")
        if not invoice_type:
            short_type = (params.get("i") or "").lower()
            short_map = {"c": "crypto", "y": "yookasa", "p": "platega", "t": "telegram"}
            invoice_type = short_map.get(short_type, "crypto")
        is_gift = (params.get("gift") or params.get("g") or "0") in {"1", "true", "yes"}
        recipient_id_raw = params.get("to") or params.get("u")
        recipient_id: Optional[int] = None
        if recipient_id_raw:
            try:
                recipient_id = int(recipient_id_raw)
            except ValueError:
                recipient_id = None
        if is_gift:
            plan = "standard"
        duo_member_ids: List[int] = []
        for key in ("d1", "d2"):
            raw_value = params.get(key)
            if not raw_value:
                continue
            try:
                parsed = int(raw_value)
            except ValueError:
                continue
            if parsed > 0 and parsed not in duo_member_ids:
                duo_member_ids.append(parsed)
        lang = callback.from_user.language_code or config.default_language
        if plan == "duo":
            price = config.price_duo
        elif plan == "family":
            price = config.price_family
        elif plan.startswith("topup"):
            if plan == "topup10":
                price = config.topup_10_price_stars
            elif plan == "topup20":
                price = config.topup_20_price_stars
            else:
                price = config.topup_50_price_stars
        elif invoice_type == "telegram":
            price = getattr(config, f"stars_price_{month}", config.stars_price_1)
        else:
            price = getattr(config, f"price_{month}", config.price_1)
        if invoice_type == "platega":
            price = math.ceil(price * 1.1)
        payer_customer = await customer_repo.find_by_telegram_id(callback.message.chat.id)
        if not payer_customer:
            await callback.answer()
            return
        duo_member_ids = [member_id for member_id in duo_member_ids if member_id != payer_customer.telegram_id][:1]
        customer = payer_customer
        if is_gift:
            if not recipient_id:
                await callback.answer(tm.get_text(lang, "gift_pick_user_failed"), show_alert=True)
                return
            recipient_customer = await customer_repo.find_by_telegram_id(recipient_id)
            if not recipient_customer:
                recipient_customer = await customer_repo.find_or_create(recipient_id, config.default_language)
            customer = recipient_customer
        try:
            url, purchase_id, _ = await payment_service.create_purchase(
                amount=float(price),
                months=month,
                customer=customer,
                invoice_type=invoice_type,
                username=callback.from_user.username,
                plan=plan,
            )
        except Exception as err:  # noqa: BLE001
            logger.exception("failed to create purchase: %s", err)
            await callback.answer("Error creating payment", show_alert=True)
            return
        if is_gift and recipient_id:
            await purchase_repo.update_fields(
                purchase_id,
                {
                    "gift_sender_telegram_id": callback.from_user.id,
                    "gift_recipient_telegram_id": recipient_id,
                },
            )
        if plan == "duo" and duo_member_ids:
            await duo_member_repo.replace_members(purchase_id, duo_member_ids)

        back_callback = f"{CallbackSell}?month={month}&amount={price}"
        if is_gift:
            back_callback = CallbackGiftMenu
        elif plan == "duo":
            back_callback = f"{CallbackDuoMembers}?month={month}"
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text=tm.get_text(lang, "pay_button"), url=url, style="success"),
                    InlineKeyboardButton(
                        text=tm.get_text(lang, "back_button"),
                        callback_data=back_callback,
                        style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"),
                    ),
                ]
            ]
        )
        msg = await callback.message.edit_reply_markup(reply_markup=markup)
        await payment_service.cache.set(purchase_id, msg.message_id)
        await callback.answer()

    @router.pre_checkout_query()
    async def pre_checkout_handler(pre_checkout: PreCheckoutQuery) -> None:
        try:
            await bot.answer_pre_checkout_query(pre_checkout.id, ok=True)
        except Exception as err:  # noqa: BLE001
            logger.warning("pre_checkout answer failed: %s", err)

    @router.message(F.successful_payment)
    async def successful_payment_handler(message: Message) -> None:
        payload = (message.successful_payment.invoice_payload or "").split("&")
        if not payload:
            return
        try:
            purchase_id = int(payload[0])
        except ValueError:
            return
        username = payload[1] if len(payload) > 1 else None
        await payment_service.process_purchase_by_id(purchase_id, username=username)

    @router.message(F.users_shared | F.user_shared)
    async def gift_users_shared_handler(message: Message) -> None:
        state = gift_state.get(message.from_user.id)
        if not state:
            return
        lang = message.from_user.language_code or config.default_language
        selected_user_id, display_name = _extract_shared_user(message)
        if not selected_user_id:
            await message.answer(
                tm.get_text(lang, "gift_pick_user_failed"),
                reply_markup=ReplyKeyboardRemove(remove_keyboard=True),
                parse_mode="HTML",
            )
            return
        request_id = int(state.get("request_id") or 0)
        if message.users_shared and request_id and int(message.users_shared.request_id) != request_id:
            return
        if message.from_user.id == selected_user_id:
            await message.answer(
                tm.get_text(lang, "duo_self_not_allowed") if state.get("mode") == "duo" else tm.get_text(lang, "gift_self_not_allowed"),
                reply_markup=ReplyKeyboardRemove(remove_keyboard=True),
                parse_mode="HTML",
            )
            if state.get("mode") != "duo":
                gift_state.pop(message.from_user.id, None)
            return

        if state.get("mode") == "paid":
            month = int(state.get("month") or 1)
            allow_stars = await _is_stars_allowed_for_customer(message.from_user.id)
            payment_markup = await _gift_payment_keyboard(
                lang=lang,
                month=month,
                recipient_id=selected_user_id,
                allow_stars=allow_stars,
            )
            gift_state.pop(message.from_user.id, None)
            await message.answer(
                tm.get_text(lang, "gift_user_selected") % (display_name or selected_user_id),
                reply_markup=ReplyKeyboardRemove(remove_keyboard=True),
                parse_mode="HTML",
            )
            await message.answer(
                tm.get_text(lang, "gift_payment_prompt") % month,
                reply_markup=payment_markup,
                parse_mode="HTML",
            )
            return

        if state.get("mode") == "duo":
            member_ids = [int(x) for x in state.get("member_ids", []) if str(x).isdigit()]
            member_names = [str(x) for x in state.get("member_names", [])]
            if selected_user_id in member_ids:
                await message.answer(
                    tm.get_text(lang, "duo_duplicate_not_allowed"),
                    parse_mode="HTML",
                )
                return

            member_ids.append(selected_user_id)
            member_names.append(str(display_name or selected_user_id))
            state["member_ids"] = member_ids
            state["member_names"] = member_names
            gift_state.pop(message.from_user.id, None)
            allow_stars = await _is_stars_allowed_for_customer(message.from_user.id)
            month = int(state.get("month") or 1)
            amount = int(state.get("amount") or config.price_duo)
            payment_keyboard = payment_methods_keyboard(
                lang=lang,
                tm=tm,
                month=month,
                amount=amount,
                plan="duo",
                allow_crypto=config.crypto_pay_enabled,
                allow_card=config.yookasa_enabled,
                allow_stars=allow_stars,
                allow_platega=config.platega_enabled and config.platega_merchant_id and config.platega_secret,
                tribute_url=config.tribute_payment_url if config.tribute_webhook_url else None,
                back_callback=f"{CallbackDuoMembers}?month={month}",
                duo_member_ids=member_ids[:1],
            )
            await message.answer(
                tm.get_text(lang, "duo_user_selected") % member_names[0],
                reply_markup=ReplyKeyboardRemove(remove_keyboard=True),
                parse_mode="HTML",
            )
            await message.answer(
                _selected_plan_text(lang, month, "duo", amount),
                reply_markup=InlineKeyboardMarkup(inline_keyboard=payment_keyboard),
                parse_mode="HTML",
            )
            return

        if state.get("mode") == "admin":
            total_days = int(state.get("days") or 0)
            months = int(state.get("months") or 0)
            selected_tag = str(state.get("tag") or "GIFT")
            recipient_customer = await customer_repo.find_by_telegram_id(selected_user_id)
            if not recipient_customer:
                recipient_customer = await customer_repo.find_or_create(selected_user_id, config.default_language)
            try:
                existing_user = await payment_service.remnawave_client.fetch_user_by_telegram(selected_user_id)
                traffic_limit_bytes = config.traffic_limit_bytes
                if existing_user and existing_user.traffic_limit_bytes:
                    traffic_limit_bytes = int(existing_user.traffic_limit_bytes)
                updated_user = await payment_service.remnawave_client.create_or_update_user(
                    customer_id=recipient_customer.id,
                    telegram_id=selected_user_id,
                    traffic_limit_bytes=traffic_limit_bytes,
                    days=total_days,
                    is_trial_user=False,
                    username=recipient_customer.username,
                    tag=selected_tag,
                )
            except Exception as err:  # noqa: BLE001
                logger.exception("failed to apply admin gift recipient=%s tag=%s: %s", selected_user_id, selected_tag, err)
                await message.answer(
                    tm.get_text(lang, "agift_apply_failed") % html.escape(str(err)[:300]),
                    parse_mode="HTML",
                )
                return
            await customer_repo.update_fields(
                recipient_customer.id,
                {"subscription_link": updated_user.subscription_url, "expire_at": updated_user.expire_at.isoformat()},
            )
            recipient_message = (
                tm.get_text(recipient_customer.language, "gift_subscription_received") % months
                if months > 0
                else tm.get_text(recipient_customer.language, "gift_subscription_received_days") % total_days
            )
            try:
                await bot.send_message(selected_user_id, recipient_message, parse_mode="HTML")
            except Exception as err:  # noqa: BLE001
                logger.warning("failed to notify admin-gift recipient=%s: %s", selected_user_id, err)
                await gift_notification_repo.create(
                    recipient_telegram_id=selected_user_id,
                    sender_telegram_id=message.from_user.id,
                    months=months,
                    days=total_days,
                    message=recipient_message,
                    purchase_id=None,
                )
            gift_state.pop(message.from_user.id, None)
            await message.answer(
                tm.get_text(lang, "agift_done") % selected_user_id,
                reply_markup=ReplyKeyboardRemove(remove_keyboard=True),
                parse_mode="HTML",
            )
            await _send_log_message(
                "🎁 <b>Админ выдал подарок</b>\n"
                f"Админ: <code>{message.from_user.id}</code>\n"
                f"Получатель: <code>{selected_user_id}</code>\n"
                f"Срок: <b>{total_days} дн.</b>\n"
                f"Тег: <code>{html.escape(selected_tag)}</code>"
            )
            return

    @router.callback_query(F.data == CallbackTrial)
    async def trial_callback(callback: CallbackQuery) -> None:
        if config.trial_days == 0:
            await callback.answer()
            return
        customer = await customer_repo.find_by_telegram_id(callback.from_user.id)
        if not customer or customer.subscription_link:
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        await callback.message.edit_text(
            tm.get_text(lang, "trial_text"),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text=tm.get_text(lang, "activate_trial_button"), callback_data=CallbackActivateTrial, icon_custom_emoji_id=_button_emoji_id(lang, "activate_trial_button"))],
                    [InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackStart, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))],
                ]
            ),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackActivateTrial)
    async def activate_trial_callback(callback: CallbackQuery) -> None:
        if config.trial_days == 0:
            await callback.answer()
            return
        customer = await customer_repo.find_by_telegram_id(callback.from_user.id)
        if not customer or customer.subscription_link:
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        await payment_service.activate_trial(callback.from_user.id, callback.from_user.username)
        updated_customer = await customer_repo.find_by_telegram_id(callback.from_user.id)
        if updated_customer:
            referral = await referral_repo.find_by_referee(updated_customer.telegram_id)
            referrer_customer = None
            if referral:
                referrer_customer = await customer_repo.find_by_telegram_id(referral.referrer_id)
            await _notify_owners_about_trial(
                customer=updated_customer,
                username=callback.from_user.username,
                referrer_customer=referrer_customer,
            )
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=tm.get_text(lang, "connect_button"), callback_data=CallbackConnect, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "connect_button"))],
                [InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackStart, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))],
            ]
        )
        await callback.message.edit_text(
            tm.get_text(lang, "trial_activated"),
            reply_markup=markup,
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackReferral)
    async def referral_callback(callback: CallbackQuery) -> None:
        customer = await customer_repo.find_by_telegram_id(callback.from_user.id)
        if not customer:
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        details = await referral_repo.find_details_by_referrer(customer.telegram_id)
        signup_days = max(0, config.referral_signup_days)
        purchase_days = max(0, config.referral_purchase_days)
        purchase_bonus_count = sum(1 for d in details if d.referral.bonus_granted)
        total_days = len(details) * signup_days + purchase_bonus_count * purchase_days
        lines = [tm.get_text(lang, "referral_header") % len(details)]
        lines.append(tm.get_text(lang, "referral_reward_info") % (signup_days, purchase_days))
        lines.append(tm.get_text(lang, "referral_total") % total_days)
        ref_url = f"https://t.me/{bot_username}?start=ref_{customer.telegram_id}"
        lines.append("")
        lines.append(tm.get_text(lang, "referral_link_text") % ref_url)
        share_text = tm.get_text(lang, "referral_share_text")
        ref_link = f"https://t.me/share/url?url={quote_plus(ref_url)}&text={quote_plus(share_text)}"
        text = "\n".join(f"<i>{line}</i>" if line else "" for line in lines)
        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text=tm.get_text(lang, "share_referral_button"),
                            url=ref_link,
                            icon_custom_emoji_id=_button_emoji_id(lang, "share_referral_button"),
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            text=tm.get_text(lang, "referral_list_button"),
                            callback_data=CallbackReferralList,
                            icon_custom_emoji_id=_button_emoji_id(lang, "referral_list_button"),
                        )
                    ],
                    [InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackStart, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))],
                ]
            ),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackGiftMenu)
    async def gift_menu_callback(callback: CallbackQuery) -> None:
        lang = callback.from_user.language_code or config.default_language
        gift_state.pop(callback.from_user.id, None)
        await callback.message.edit_text(
            tm.get_text(lang, "gift_menu_title"),
            reply_markup=_gift_duration_keyboard(lang),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data.startswith(f"{CallbackGiftSelect}?"))
    async def gift_select_callback(callback: CallbackQuery) -> None:
        params = parse_callback_data(callback.data)
        lang = callback.from_user.language_code or config.default_language
        try:
            month = int(params.get("month", "1"))
        except ValueError:
            month = 1
        if month not in {1, 3, 6, 12}:
            month = 1
        if int(getattr(config, f"price_{month}", 0)) <= 0:
            await callback.answer(tm.get_text(lang, "gift_duration_unavailable"), show_alert=True)
            return
        request_id = random.randint(1_000_000, 9_999_999)
        gift_state[callback.from_user.id] = {
            "mode": "paid",
            "month": month,
            "request_id": request_id,
        }
        await callback.message.answer(
            tm.get_text(lang, "gift_pick_user_prompt"),
            reply_markup=_gift_user_picker_keyboard(lang, request_id),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackReferralList)
    async def referral_list_callback(callback: CallbackQuery) -> None:
        customer = await customer_repo.find_by_telegram_id(callback.from_user.id)
        if not customer:
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        details = await referral_repo.find_details_by_referrer(customer.telegram_id)
        signup_days = max(0, config.referral_signup_days)
        purchase_days = max(0, config.referral_purchase_days)
        lines = [tm.get_text(lang, "referral_list_title")]
        if details:
            for idx, item in enumerate(details[:30], start=1):
                name = f"@{item.referee_username}" if item.referee_username else f"id {item.referral.referee_id}"
                status = (
                    tm.get_text(lang, "referral_status_bonus") % (signup_days, purchase_days)
                    if item.referral.bonus_granted
                    else tm.get_text(lang, "referral_status_waiting") % (signup_days, purchase_days)
                )
                lines.append(f"{idx}. {name}")
                lines.append(status)
            if len(details) > 30:
                lines.append(tm.get_text(lang, "referral_list_more") % (len(details) - 30))
        else:
            lines.append(tm.get_text(lang, "referral_empty"))
        text = "\n".join(f"<i>{line}</i>" if line else "" for line in lines)
        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackReferral, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))]]
            ),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackPromo)
    async def promo_callback(callback: CallbackQuery) -> None:
        pending_promo.add(callback.from_user.id)
        lang = callback.from_user.language_code or config.default_language
        await callback.message.answer(
            tm.get_text(lang, "promo_enter_prompt"),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackBuy, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))]]
            ),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackStats)
    async def stats_callback(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        day = _stats_day_in_ekb()
        text = await stats_service.build_report_for_local_day(day)
        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text=tm.get_text(lang, "stats_refresh_button"), callback_data=CallbackStats)],
                    [InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackStart, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))],
                ]
            ),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackPromoAdminList)
    async def promo_admin_list(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        promos = await promo_repo.list_all(30)
        if promos:
            lines = [
                "• {code} — +{days}д, +{gb}ГБ, {used}/{max_uses}".format(
                    code=p.code, days=p.days, gb=getattr(p, "traffic_gb", 0), used=p.used, max_uses=p.max_uses
                )
                for p in promos
            ]
            text = tm.get_text(lang, "promo_admin_list") + "\n" + "\n".join(lines)
        else:
            text = tm.get_text(lang, "promo_admin_empty")
        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackPromoAdmin, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))],
                ]
            ),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackPromoAdminCreate)
    async def promo_admin_create(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        panel_state.pop(callback.from_user.id, None)
        pending_promo.discard(callback.from_user.id)
        promo_admin_state[callback.from_user.id] = {"step": "choose_type"}
        await callback.message.edit_text(
            tm.get_text(lang, "promo_create_choose_type"),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text=tm.get_text(lang, "promo_type_days"), callback_data=CallbackPromoTypeDays),
                        InlineKeyboardButton(text=tm.get_text(lang, "promo_type_gb"), callback_data=CallbackPromoTypeGb),
                    ],
                    [InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackPromoAdmin, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))],
                ]
            ),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CallbackPromoTypeDays)
    async def promo_type_days(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        panel_state.pop(callback.from_user.id, None)
        promo_admin_state[callback.from_user.id] = {"step": "await_days", "type": "days"}
        await callback.message.edit_text(tm.get_text(lang, "promo_enter_days"), parse_mode="HTML")
        await callback.answer()

    @router.callback_query(F.data == CallbackPromoTypeGb)
    async def promo_type_gb(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        lang = callback.from_user.language_code or config.default_language
        panel_state.pop(callback.from_user.id, None)
        promo_admin_state[callback.from_user.id] = {"step": "await_gb", "type": "gb"}
        await callback.message.edit_text(tm.get_text(lang, "promo_enter_gb"), parse_mode="HTML")
        await callback.answer()

    @router.callback_query(F.data == CallbackPromoAdmin)
    async def promo_admin_back(callback: CallbackQuery) -> None:
        if not _is_admin(callback.from_user.id):
            await callback.answer()
            return
        panel_state.pop(callback.from_user.id, None)
        promo_admin_state.pop(callback.from_user.id, None)
        lang = callback.from_user.language_code or config.default_language
        await callback.message.edit_text(
            tm.get_text(lang, "promo_admin_prompt"),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text=tm.get_text(lang, "promo_admin_create"), callback_data=CallbackPromoAdminCreate),
                        InlineKeyboardButton(text=tm.get_text(lang, "promo_admin_list"), callback_data=CallbackPromoAdminList),
                    ],
                    [InlineKeyboardButton(text=tm.get_text(lang, "back_button"), callback_data=CallbackAdminPanel, style="primary", icon_custom_emoji_id=_button_emoji_id(lang, "back_button"))],
                ]
            ),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.message(~F.text)
    async def admin_broadcast_media_handler(message: Message) -> None:
        if not _is_admin(message.from_user.id):
            return
        state = panel_state.get(message.from_user.id)
        if not state or state.get("step") != "await_broadcast_source":
            return
        lang = message.from_user.language_code or config.default_language
        await _save_broadcast_source_message(message, lang)

    @router.message(F.text)
    async def promo_text_handler(message: Message) -> None:
        lang = message.from_user.language_code or config.default_language
        text_val = message.text.strip()

        # Admin panel actions via text input after inline selection.
        if (
            _is_admin(message.from_user.id)
            and message.from_user.id in panel_state
            and message.from_user.id not in promo_admin_state
        ):
            state = panel_state[message.from_user.id]
            step = state.get("step")

            if step == "await_user_lookup":
                try:
                    telegram_id = int(text_val)
                except ValueError:
                    await message.answer(tm.get_text(lang, "admin_invalid_id"), parse_mode="HTML")
                    return
                customer = await customer_repo.find_by_telegram_id(telegram_id)
                if not customer:
                    await message.answer(tm.get_text(lang, "admin_user_not_found"), parse_mode="HTML")
                    panel_state.pop(message.from_user.id, None)
                    return
                customer = await payment_service.refresh_customer_subscription(customer)
                used, limit, ok = await sync_service.get_traffic_usage(customer.telegram_id)
                if ok and limit > 0:
                    used_gb = round(used / 1_073_741_824, 2)
                    limit_gb = round(limit / 1_073_741_824, 2)
                    traffic_text = f"{used_gb}/{limit_gb} GB"
                else:
                    traffic_text = "-"
                expire = customer.expire_at.strftime("%d.%m.%Y %H:%M") if customer.expire_at else "-"
                active = "yes" if customer.expire_at and customer.expire_at > datetime.utcnow() else "no"
                username = f"@{customer.username}" if customer.username else "-"
                await message.answer(
                    tm.get_text(lang, "admin_user_info_template") % (
                        customer.telegram_id,
                        username,
                        customer.created_at.strftime("%d.%m.%Y %H:%M"),
                        expire,
                        active,
                        traffic_text,
                    ),
                    parse_mode="HTML",
                )
                panel_state.pop(message.from_user.id, None)
                return

            if step == "await_user_delete":
                try:
                    telegram_id = int(text_val)
                except ValueError:
                    await message.answer(tm.get_text(lang, "admin_invalid_id"), parse_mode="HTML")
                    return
                customer = await customer_repo.find_by_telegram_id(telegram_id)
                if not customer:
                    await message.answer(tm.get_text(lang, "admin_user_not_found"), parse_mode="HTML")
                    panel_state.pop(message.from_user.id, None)
                    return
                try:
                    removed_from_remnawave = await payment_service.remnawave_client.delete_user_by_telegram(telegram_id)
                except Exception as err:  # noqa: BLE001
                    logger.warning("failed to delete user from remnawave telegram_id=%s: %s", telegram_id, err)
                    await message.answer(tm.get_text(lang, "admin_user_delete_remnawave_failed"), parse_mode="HTML")
                    return
                if not removed_from_remnawave:
                    await message.answer(tm.get_text(lang, "admin_user_delete_remnawave_failed"), parse_mode="HTML")
                    return
                deleted_from_db = await customer_repo.delete_by_telegram_id(telegram_id)
                if not deleted_from_db:
                    await message.answer(tm.get_text(lang, "admin_user_delete_db_failed"), parse_mode="HTML")
                    return
                panel_state.pop(message.from_user.id, None)
                await message.answer(tm.get_text(lang, "admin_user_deleted_done") % telegram_id, parse_mode="HTML")
                await _send_log_message(
                    "🗑 <b>Админ удалил пользователя</b>\n"
                    f"Админ: <code>{message.from_user.id}</code>\n"
                    f"Пользователь: <code>{telegram_id}</code>"
                )
                return

            if step == "await_sub_extend":
                parts = text_val.split()
                if len(parts) != 2:
                    await message.answer(tm.get_text(lang, "admin_sub_extend_format"), parse_mode="HTML")
                    return
                try:
                    telegram_id = int(parts[0])
                    days = int(parts[1])
                except ValueError:
                    await message.answer(tm.get_text(lang, "admin_invalid_number"), parse_mode="HTML")
                    return
                customer = await customer_repo.find_by_telegram_id(telegram_id)
                if not customer:
                    await message.answer(tm.get_text(lang, "admin_user_not_found"), parse_mode="HTML")
                    panel_state.pop(message.from_user.id, None)
                    return
                current_user = await payment_service.remnawave_client.fetch_user_by_telegram(telegram_id)
                traffic_limit = (
                    int(current_user.traffic_limit_bytes)
                    if current_user and current_user.traffic_limit_bytes
                    else config.traffic_limit_bytes
                )
                updated_user = await payment_service.remnawave_client.create_or_update_user(
                    customer_id=customer.id,
                    telegram_id=telegram_id,
                    traffic_limit_bytes=traffic_limit,
                    days=days,
                    is_trial_user=False,
                    username=customer.username,
                )
                await customer_repo.update_fields(
                    customer.id,
                    {"expire_at": updated_user.expire_at.isoformat(), "subscription_link": updated_user.subscription_url},
                )
                panel_state.pop(message.from_user.id, None)
                await message.answer(
                    tm.get_text(lang, "admin_sub_updated") % (telegram_id, updated_user.expire_at.strftime("%d.%m.%Y %H:%M")),
                    parse_mode="HTML",
                )
                await _send_log_message(
                    "🛠 <b>Админ продлил подписку</b>\n"
                    f"Админ: <code>{message.from_user.id}</code>\n"
                    f"Пользователь: <code>{telegram_id}</code>\n"
                    f"Дней: <b>{days}</b>\n"
                    f"До: <b>{updated_user.expire_at.strftime('%d.%m.%Y %H:%M')}</b>"
                )
                return

            if step == "await_sub_forever":
                try:
                    telegram_id = int(text_val)
                except ValueError:
                    await message.answer(tm.get_text(lang, "admin_invalid_id"), parse_mode="HTML")
                    return
                customer = await customer_repo.find_by_telegram_id(telegram_id)
                if not customer:
                    await message.answer(tm.get_text(lang, "admin_user_not_found"), parse_mode="HTML")
                    panel_state.pop(message.from_user.id, None)
                    return
                forever_dt = datetime(2099, 12, 31, 23, 59, 59)
                updated_user = await payment_service.remnawave_client.set_user_expire_at(
                    telegram_id=telegram_id,
                    expire_at=forever_dt,
                )
                if updated_user:
                    await customer_repo.update_fields(
                        customer.id,
                        {"expire_at": updated_user.expire_at.isoformat(), "subscription_link": updated_user.subscription_url},
                    )
                panel_state.pop(message.from_user.id, None)
                await message.answer(
                    tm.get_text(lang, "admin_sub_forever_done") % telegram_id,
                    parse_mode="HTML",
                )
                await _send_log_message(
                    "♾ <b>Админ выдал бессрочную подписку</b>\n"
                    f"Админ: <code>{message.from_user.id}</code>\n"
                    f"Пользователь: <code>{telegram_id}</code>"
                )
                return

            if step == "await_sub_disable":
                try:
                    telegram_id = int(text_val)
                except ValueError:
                    await message.answer(tm.get_text(lang, "admin_invalid_id"), parse_mode="HTML")
                    return
                customer = await customer_repo.find_by_telegram_id(telegram_id)
                if not customer:
                    await message.answer(tm.get_text(lang, "admin_user_not_found"), parse_mode="HTML")
                    panel_state.pop(message.from_user.id, None)
                    return
                disabled_dt = datetime.utcnow() - timedelta(minutes=1)
                updated_user = await payment_service.remnawave_client.set_user_expire_at(
                    telegram_id=telegram_id,
                    expire_at=disabled_dt,
                )
                if updated_user:
                    await customer_repo.update_fields(
                        customer.id,
                        {"expire_at": updated_user.expire_at.isoformat(), "subscription_link": updated_user.subscription_url},
                    )
                else:
                    await customer_repo.update_fields(customer.id, {"expire_at": disabled_dt.isoformat()})
                panel_state.pop(message.from_user.id, None)
                await message.answer(
                    tm.get_text(lang, "admin_sub_disabled_done") % telegram_id,
                    parse_mode="HTML",
                )
                await _send_log_message(
                    "⛔ <b>Админ отключил подписку</b>\n"
                    f"Админ: <code>{message.from_user.id}</code>\n"
                    f"Пользователь: <code>{telegram_id}</code>"
                )
                return

            if step == "await_broadcast_source":
                await _save_broadcast_source_message(message, lang)
                return

            if step == "await_broadcast_button_text":
                state["button_text"] = text_val
                state["step"] = "broadcast_idle"
                await message.answer(
                    _broadcast_button_settings_text(lang, state),
                    reply_markup=_admin_broadcast_button_keyboard(lang, state),
                    parse_mode="HTML",
                )
                return

            if step == "await_broadcast_button_url":
                if not _is_valid_broadcast_button_url(text_val):
                    await message.answer(tm.get_text(lang, "admin_broadcast_button_url_invalid"), parse_mode="HTML")
                    return
                state["button_url"] = text_val
                state["step"] = "broadcast_idle"
                await message.answer(
                    _broadcast_button_settings_text(lang, state),
                    reply_markup=_admin_broadcast_button_keyboard(lang, state),
                    parse_mode="HTML",
                )
                return

            if step == "await_broadcast_button_emoji":
                state["button_emoji_id"] = text_val
                state["step"] = "broadcast_idle"
                await message.answer(
                    _broadcast_button_settings_text(lang, state),
                    reply_markup=_admin_broadcast_button_keyboard(lang, state),
                    parse_mode="HTML",
                )
                return

            if step == "await_price_value":
                key = state.get("key")
                if key not in PRICE_FIELD_ORDER:
                    panel_state.pop(message.from_user.id, None)
                    return
                try:
                    value = int(text_val)
                except ValueError:
                    await message.answer(tm.get_text(lang, "admin_invalid_number"), parse_mode="HTML")
                    return
                if value < 0:
                    await message.answer(tm.get_text(lang, "admin_invalid_number"), parse_mode="HTML")
                    return
                await price_repo.set_value(key, value, message.from_user.id)
                setattr(config, key, value)
                panel_state.pop(message.from_user.id, None)
                label = PRICE_FIELD_LABELS.get(key, key)
                await message.answer(
                    tm.get_text(lang, "admin_price_updated") % (label, value),
                    parse_mode="HTML",
                )
                await _send_log_message(
                    "💸 <b>Админ изменил цену</b>\n"
                    f"Админ: <code>{message.from_user.id}</code>\n"
                    f"Поле: <b>{html.escape(label)}</b>\n"
                    f"Новое значение: <code>{value}</code>"
                )
                return

        # Promo admin creation flow.
        if _is_admin(message.from_user.id) and message.from_user.id in promo_admin_state:
            state = promo_admin_state[message.from_user.id]
            if state.get("step") == "choose_type":
                await message.answer(tm.get_text(lang, "promo_create_choose_type"), parse_mode="HTML")
                return
            if state.get("step") in {"await_days", "await_gb", "await_uses"}:
                try:
                    value = int(text_val)
                except ValueError:
                    await message.answer(tm.get_text(lang, "promo_invalid_number"), parse_mode="HTML")
                    return

            if state.get("step") == "await_days":
                if value < 0:
                    await message.answer(tm.get_text(lang, "promo_invalid_number"), parse_mode="HTML")
                    return
                promo_admin_state[message.from_user.id] = {"step": "await_uses", "days": value, "gb": 0}
                await message.answer(tm.get_text(lang, "promo_enter_uses"), parse_mode="HTML")
                return
            if state.get("step") == "await_gb":
                if value < 0:
                    await message.answer(tm.get_text(lang, "promo_invalid_number"), parse_mode="HTML")
                    return
                promo_admin_state[message.from_user.id] = {"step": "await_uses", "days": 0, "gb": value}
                await message.answer(tm.get_text(lang, "promo_enter_uses"), parse_mode="HTML")
                return
            if state.get("step") == "await_uses":
                days = state.get("days", 0)
                gb = state.get("gb", 0)
                uses = max(1, value)
                code = ""
                while not code:
                    candidate = "PROMO-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
                    existing = await promo_repo.find_by_code(candidate)
                    if not existing:
                        code = candidate
                promo = await promo_repo.create(code.upper(), days, gb, uses, message.from_user.id)
                promo_admin_state.pop(message.from_user.id, None)
                await message.answer(
                    tm.get_text(lang, "promo_admin_created") % (promo.code, promo.days, promo.traffic_gb, promo.max_uses),
                    parse_mode="HTML",
                )
                return

        # User promo redemption flow.
        if message.from_user.id not in pending_promo:
            return
        pending_promo.discard(message.from_user.id)
        customer = await customer_repo.find_by_telegram_id(message.from_user.id)
        if not customer:
            return
        markup = InlineKeyboardMarkup(inline_keyboard=start_keyboard(customer, lang, tm))
        status = await payment_service.apply_promo_code(customer, message.text, message.from_user.username)
        if status == "ok":
            await message.answer(tm.get_text(lang, "promo_ok"), reply_markup=markup, parse_mode="HTML")
        elif status == "already_used":
            await message.answer(tm.get_text(lang, "promo_already_used"), reply_markup=markup, parse_mode="HTML")
        elif status == "exhausted":
            await message.answer(tm.get_text(lang, "promo_exhausted"), reply_markup=markup, parse_mode="HTML")
        else:
            await message.answer(tm.get_text(lang, "promo_invalid"), reply_markup=markup, parse_mode="HTML")

    return router
