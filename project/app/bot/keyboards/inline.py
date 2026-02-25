from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from aiogram.types import InlineKeyboardButton

from ...config import config
from ...db.queries import Customer
from ...services.translation import TranslationManager


def _button_emoji_id(lang: str, tm: TranslationManager, key: str) -> Optional[str]:
    value = tm.get_text(lang, f"{key}_emoji_id")
    if not value or value == f"{key}_emoji_id":
        return None
    return value


def _month_text(lang: str, tm: TranslationManager, month: int, price_rub: int) -> str:
    base = tm.get_text(lang, f"month_{month}")
    return f"{base} · {price_rub}₽"


def _duo_text(lang: str, tm: TranslationManager) -> str:
    tpl = tm.get_text(lang, "plan_duo_dynamic")
    if "%s" in tpl:
        return tpl % config.price_duo
    return f"Duo · {config.price_duo}₽"


def _family_text(lang: str, tm: TranslationManager) -> str:
    tpl = tm.get_text(lang, "plan_family_dynamic")
    if "%s" in tpl:
        return tpl % config.price_family
    return f"Family · {config.price_family}₽"


def start_keyboard(customer: Customer, lang: str, tm: TranslationManager) -> List[List[InlineKeyboardButton]]:
    keyboard: List[List[InlineKeyboardButton]] = []

    # Trial always first and single
    if customer.subscription_link is None and config.trial_days > 0:
        keyboard.append(
            [InlineKeyboardButton(text=tm.get_text(lang, "trial_button"), callback_data="trial")]
        )

    # Main actions row: Buy / Connect (connect only if active)
    row_main: List[InlineKeyboardButton] = [
        InlineKeyboardButton(
            text=tm.get_text(lang, "buy_button"),
            callback_data="buy",
            style="success",
            icon_custom_emoji_id=_button_emoji_id(lang, tm, "buy_button"),
        )
    ]
    if customer.subscription_link and customer.expire_at and customer.expire_at > datetime.utcnow():
        row_main.append(_connect_buttons(lang, tm)[0])
    keyboard.append(row_main)

    # Referral / Support
    row_ref_support: List[InlineKeyboardButton] = []
    if config.referral_signup_days > 0 or config.referral_purchase_days > 0:
        row_ref_support.append(
            InlineKeyboardButton(
                text=tm.get_text(lang, "referral_button"),
                callback_data="referral",
                icon_custom_emoji_id=_button_emoji_id(lang, tm, "referral_button"),
            )
        )
    if config.support_url:
        row_ref_support.append(
            InlineKeyboardButton(
                text=tm.get_text(lang, "support_button"),
                url=config.support_url,
                icon_custom_emoji_id=_button_emoji_id(lang, tm, "support_button"),
            )
        )
    if row_ref_support:
        keyboard.append(row_ref_support)

    keyboard.append(
        [
            InlineKeyboardButton(
                text=tm.get_text(lang, "settings_button"),
                callback_data="settings",
                icon_custom_emoji_id=_button_emoji_id(lang, tm, "settings_button"),
            )
        ]
    )

    # Channel / Feedback
    row_info: List[InlineKeyboardButton] = []
    if config.channel_url:
        row_info.append(
            InlineKeyboardButton(
                text=tm.get_text(lang, "channel_button"),
                url=config.channel_url,
                icon_custom_emoji_id=_button_emoji_id(lang, tm, "channel_button"),
            )
        )
    if config.feedback_url:
        row_info.append(
            InlineKeyboardButton(
                text=tm.get_text(lang, "feedback_button"),
                url=config.feedback_url,
                icon_custom_emoji_id=_button_emoji_id(lang, tm, "feedback_button"),
            )
        )
    if row_info:
        keyboard.append(row_info)

    # Status / ToS
    row_misc: List[InlineKeyboardButton] = []
    if config.server_status_url:
        row_misc.append(InlineKeyboardButton(text=tm.get_text(lang, "server_status_button"), url=config.server_status_url))
    if config.tos_url:
        row_misc.append(
            InlineKeyboardButton(
                text=tm.get_text(lang, "tos_button"),
                url=config.tos_url,
            )
        )
    if row_misc:
        keyboard.append(row_misc)

    if customer.telegram_id in config.notify_telegram_ids or (
        config.admin_telegram_id > 0 and customer.telegram_id == config.admin_telegram_id
    ):
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=tm.get_text(lang, "stats_button"),
                    callback_data="stats_panel",
                ),
                InlineKeyboardButton(
                    text=tm.get_text(lang, "admin_panel_button"),
                    callback_data="admin_panel",
                    icon_custom_emoji_id=_button_emoji_id(lang, tm, "admin_panel_button"),
                ),
            ]
        )

    # Privacy link separate to keep layout clean
    keyboard.append(
        [
            InlineKeyboardButton(
                text=tm.get_text(lang, "privacy_policy_button") if tm.get_text(lang, "privacy_policy_button") else "Privacy policy",
                url="https://telegra.ph/Politika-konfedecialnosti-01-10",
            )
        ]
    )
    return keyboard


def _connect_buttons(lang: str, tm: TranslationManager) -> List[InlineKeyboardButton]:
    if config.mini_app_url:
        return [
            InlineKeyboardButton(
                text=tm.get_text(lang, "connect_button"),
                web_app={"url": config.mini_app_url},
                style="primary",
                icon_custom_emoji_id=_button_emoji_id(lang, tm, "connect_button"),
            )
        ]
    return [
            InlineKeyboardButton(
                text=tm.get_text(lang, "connect_button"),
                callback_data="connect",
                style="primary",
                icon_custom_emoji_id=_button_emoji_id(lang, tm, "connect_button"),
            )
    ]


def price_keyboard(lang: str, tm: TranslationManager) -> List[List[InlineKeyboardButton]]:
    keyboard: List[List[InlineKeyboardButton]] = []

    row_1: List[InlineKeyboardButton] = []
    if config.price_1 > 0:
        row_1.append(
            InlineKeyboardButton(
                text=_month_text(lang, tm, 1, config.price_1),
                callback_data=f"sell?month=1&amount={config.price_1}",
            )
        )
    if config.price_3 > 0:
        row_1.append(
            InlineKeyboardButton(
                text=_month_text(lang, tm, 3, config.price_3),
                callback_data=f"sell?month=3&amount={config.price_3}",
            )
        )
    if row_1:
        keyboard.append(row_1)

    row_2: List[InlineKeyboardButton] = []
    if config.price_6 > 0:
        row_2.append(
            InlineKeyboardButton(
                text=_month_text(lang, tm, 6, config.price_6),
                callback_data=f"sell?month=6&amount={config.price_6}",
            )
        )
    if config.price_12 > 0:
        row_2.append(
            InlineKeyboardButton(
                text=_month_text(lang, tm, 12, config.price_12),
                callback_data=f"sell?month=12&amount={config.price_12}",
            )
        )
    if row_2:
        keyboard.append(row_2)

    if config.price_duo > 0:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=_duo_text(lang, tm),
                    callback_data=f"sell?plan=duo&amount={config.price_duo}&month=1",
                    icon_custom_emoji_id=_button_emoji_id(lang, tm, "duo_button"),
                )
            ]
        )
    if config.price_family > 0:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=_family_text(lang, tm),
                    callback_data=f"sell?plan=family&amount={config.price_family}&month=1",
                )
            ]
        )
    keyboard.append(
        [
            InlineKeyboardButton(
                text=tm.get_text(lang, "gift_button"),
                callback_data="gift_menu",
                style="success",
                icon_custom_emoji_id=_button_emoji_id(lang, tm, "gift_button"),
            )
        ]
    )
    keyboard.append(
        [
            InlineKeyboardButton(
                text=tm.get_text(lang, "promo_button") or "Promo",
                callback_data="promo",
                style="danger",
                icon_custom_emoji_id=_button_emoji_id(lang, tm, "promo_button"),
            )
        ]
    )
    keyboard.append(
        [
            InlineKeyboardButton(
                text=tm.get_text(lang, "back_button"),
                callback_data="start",
                style="primary",
                icon_custom_emoji_id=_button_emoji_id(lang, tm, "back_button"),
            )
        ]
    )
    return keyboard


def payment_methods_keyboard(
    lang: str,
    tm: TranslationManager,
    month: int,
    amount: int,
    plan: str,
    allow_crypto: bool,
    allow_card: bool,
    allow_stars: bool,
    allow_platega: bool,
    tribute_url: Optional[str],
    back_callback: str = "buy",
    duo_member_ids: Optional[List[int]] = None,
) -> List[List[InlineKeyboardButton]]:
    keyboard: List[List[InlineKeyboardButton]] = []
    duo_suffix = ""
    if duo_member_ids:
        sanitized: List[int] = []
        for raw in duo_member_ids:
            try:
                value = int(raw)
            except Exception:
                continue
            if value <= 0 or value in sanitized:
                continue
            sanitized.append(value)
        if sanitized:
            duo_suffix = f"&d1={sanitized[0]}"
            if len(sanitized) > 1:
                duo_suffix += f"&d2={sanitized[1]}"
    if allow_card:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=tm.get_text(lang, "card_button"),
                    callback_data=f"payment?month={month}&invoiceType=yookasa&amount={amount}&plan={plan}{duo_suffix}",
                )
            ]
        )
    if allow_platega:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=tm.get_text(lang, "sbp_button") or "SBP",
                    callback_data=f"payment?month={month}&invoiceType=platega&amount={amount}&plan={plan}{duo_suffix}",
                    icon_custom_emoji_id=_button_emoji_id(lang, tm, "sbp_button"),
                )
            ]
        )
    elif config.support_url:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=tm.get_text(lang, "sbp_button") or "SBP",
                    url=config.support_url,
                    icon_custom_emoji_id=_button_emoji_id(lang, tm, "sbp_button"),
                )
            ]
        )
    if allow_stars:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=tm.get_text(lang, "stars_button"),
                    callback_data=f"payment?month={month}&invoiceType=telegram&amount={amount}&plan={plan}{duo_suffix}",
                    icon_custom_emoji_id=_button_emoji_id(lang, tm, "stars_button"),
                )
            ]
        )
    if allow_crypto:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=tm.get_text(lang, "crypto_button"),
                    callback_data=f"payment?month={month}&invoiceType=crypto&amount={amount}&plan={plan}{duo_suffix}",
                    icon_custom_emoji_id=_button_emoji_id(lang, tm, "crypto_button"),
                )
            ]
        )
    if tribute_url:
        keyboard.append(
            [InlineKeyboardButton(text=tm.get_text(lang, "tribute_button"), url=tribute_url)]
        )
    keyboard.append(
        [
            InlineKeyboardButton(
                text=tm.get_text(lang, "back_button"),
                callback_data=back_callback,
                style="primary",
                icon_custom_emoji_id=_button_emoji_id(lang, tm, "back_button"),
            )
        ]
    )
    return keyboard

