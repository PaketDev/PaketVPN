from __future__ import annotations

import asyncio
import logging
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice

from ..config import config
from ..db.queries import (
    Customer,
    CustomerRepository,
    DuoPurchaseMemberRepository,
    GiftNotificationRepository,
    PromoCode,
    PromoRepository,
    Purchase,
    PurchaseRepository,
    ReferralRepository,
    SalesRepository,
)
from .cache import TTLCache
from .cryptopay import CryptoPayClient
from .moynalog import MoynalogClient
from .platega import PlategaClient
from .remnawave import RemnawaveClient
from .translation import TranslationManager
from .yookassa import YookassaClient

logger = logging.getLogger(__name__)


class PaymentService:
    def __init__(
        self,
        translation: TranslationManager,
        purchase_repo: PurchaseRepository,
        remnawave_client: RemnawaveClient,
        customer_repo: CustomerRepository,
        bot: Bot,
        crypto_client: Optional[CryptoPayClient],
        yookassa_client: Optional[YookassaClient],
        platega_client: Optional[PlategaClient],
        referral_repo: ReferralRepository,
        promo_repo: PromoRepository,
        sales_repo: SalesRepository,
        gift_notification_repo: GiftNotificationRepository,
        duo_member_repo: DuoPurchaseMemberRepository,
        cache: TTLCache,
        moynalog_client: Optional[MoynalogClient],
    ) -> None:
        self.translation = translation
        self.purchase_repo = purchase_repo
        self.remnawave_client = remnawave_client
        self.customer_repo = customer_repo
        self.bot = bot
        self.crypto_client = crypto_client
        self.yookassa_client = yookassa_client
        self.platega_client = platega_client
        self.referral_repo = referral_repo
        self.promo_repo = promo_repo
        self.sales_repo = sales_repo
        self.gift_notification_repo = gift_notification_repo
        self.duo_member_repo = duo_member_repo
        self.cache = cache
        self.moynalog_client = moynalog_client

    def _button_emoji_id(self, lang: str, key: str) -> Optional[str]:
        value = self.translation.get_text(lang, f"{key}_emoji_id")
        if not value or value == f"{key}_emoji_id":
            return None
        return value

    async def process_purchase_by_id(self, purchase_id: int, username: Optional[str]) -> None:
        purchase = await self.purchase_repo.find_by_id(purchase_id)
        if not purchase:
            raise RuntimeError(f"purchase {purchase_id} not found")
        if purchase.status == "paid":
            logger.info("purchase already processed id=%s", purchase_id)
            return

        customer = await self.customer_repo.find_by_id(purchase.customer_id)
        if not customer:
            raise RuntimeError(f"customer {purchase.customer_id} not found")
        previous_expire_at = customer.expire_at
        is_renewal = bool(previous_expire_at and previous_expire_at > datetime.utcnow())
        prior_paid_count = await self.purchase_repo.count_paid_by_customer(customer.id)

        message_id = await self.cache.get(purchase.id)
        if message_id:
            try:
                await self.bot.delete_message(customer.telegram_id, message_id)
            except Exception as err:  # noqa: BLE001
                logger.warning("failed to delete payment message: %s", err)

        plan = purchase.plan or "standard"
        gift_sender_id = purchase.gift_sender_telegram_id
        gift_recipient_id = purchase.gift_recipient_telegram_id
        is_gift = bool(gift_sender_id and gift_recipient_id)
        user = await self.remnawave_client.fetch_user_by_telegram(customer.telegram_id)

        days = purchase.month * config.days_in_month
        traffic_limit_bytes = config.traffic_limit_bytes
        if plan == "duo":
            traffic_limit_bytes = config.duo_traffic_limit_bytes
        elif plan == "family":
            traffic_limit_bytes = config.family_traffic_limit_bytes
        elif plan in {"topup10", "topup20", "topup50"}:
            extra_map = {"topup10": 10, "topup20": 20, "topup50": 50}
            extra_gb = extra_map.get(plan, 0)
            extra_bytes = extra_gb * 1_073_741_824
            base_limit = traffic_limit_bytes
            if user and user.traffic_limit_bytes:
                base_limit = user.traffic_limit_bytes
            traffic_limit_bytes = base_limit + extra_bytes
            days = 0 if user else config.days_in_month

        user = await self.remnawave_client.create_or_update_user(
            customer_id=customer.id,
            telegram_id=customer.telegram_id,
            traffic_limit_bytes=traffic_limit_bytes,
            days=days,
            is_trial_user=False,
            username=username,
        )

        if is_renewal and plan not in {"topup10", "topup20", "topup50"}:
            try:
                reset_ok = await self.remnawave_client.reset_user_traffic_by_telegram(customer.telegram_id)
                if not reset_ok:
                    logger.warning("traffic reset was not applied for renewal customer=%s", customer.telegram_id)
            except Exception as err:  # noqa: BLE001
                logger.warning("failed to reset traffic for renewal customer=%s: %s", customer.telegram_id, err)

        await self.purchase_repo.mark_as_paid(purchase.id)
        refreshed_purchase = await self.purchase_repo.find_by_id(purchase.id)
        if refreshed_purchase:
            purchase = refreshed_purchase
        await self.sales_repo.record_sale(
            purchase=purchase,
            customer=customer,
            is_new_customer=prior_paid_count == 0,
        )
        await self.customer_repo.update_fields(
            customer.id,
            {"subscription_link": user.subscription_url, "expire_at": user.expire_at.isoformat()},
        )

        recipient_notified = False
        if plan in {"topup10", "topup20", "topup50"}:
            limit_gb = round(traffic_limit_bytes / 1_073_741_824, 2)
            text = self.translation.get_text(customer.language, "topup_applied") % limit_gb
        elif is_gift:
            text = self.translation.get_text(customer.language, "gift_subscription_received") % purchase.month
        else:
            text = self.translation.get_text(customer.language, "subscription_activated")
        try:
            await self.bot.send_message(
                customer.telegram_id,
                text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=self._connect_keyboard(customer.language)),
            )
            recipient_notified = True
        except Exception as err:  # noqa: BLE001
            logger.warning("failed to notify customer=%s about purchase=%s: %s", customer.telegram_id, purchase.id, err)
            if is_gift:
                await self.gift_notification_repo.create(
                    recipient_telegram_id=customer.telegram_id,
                    sender_telegram_id=gift_sender_id,
                    months=max(0, int(purchase.month)),
                    days=max(0, int(purchase.month * config.days_in_month)),
                    message=text,
                    purchase_id=purchase.id,
                )

        if purchase.invoice_type == "yookasa" and self.moynalog_client:
            asyncio.create_task(self._send_receipt_to_moynalog(purchase))

        if is_gift and gift_sender_id:
            sender_customer = await self.customer_repo.find_by_telegram_id(gift_sender_id)
            sender_lang = sender_customer.language if sender_customer else config.default_language
            if recipient_notified:
                sender_text = self.translation.get_text(sender_lang, "gift_sender_success") % customer.telegram_id
            else:
                sender_text = self.translation.get_text(sender_lang, "gift_sender_pending") % customer.telegram_id
            try:
                await self.bot.send_message(gift_sender_id, sender_text, parse_mode="HTML")
            except Exception as err:  # noqa: BLE001
                logger.warning("failed to notify gift sender=%s: %s", gift_sender_id, err)

        if plan == "duo":
            await self._notify_duo_members(purchase, customer, user.subscription_url or "")

        if plan not in {"topup10", "topup20", "topup50"}:
            await self._maybe_grant_referral_bonus(customer)
        await self._notify_owner_about_purchase(customer, purchase, plan, previous_expire_at, user.expire_at)
        logger.info("purchase processed id=%s type=%s", purchase.id, purchase.invoice_type)

    async def _notify_duo_members(self, purchase: Purchase, buyer: Customer, subscription_url: str) -> None:
        member_ids = await self.duo_member_repo.list_member_ids(purchase.id)
        if not member_ids:
            return
        member_ids = member_ids[:1]
        for member_id in member_ids:
            if member_id == buyer.telegram_id:
                continue
            member_customer = await self.customer_repo.find_by_telegram_id(member_id)
            if not member_customer:
                try:
                    await self.bot.send_message(
                        buyer.telegram_id,
                        self.translation.get_text(buyer.language, "duo_member_notify_missing") % member_id,
                        parse_mode="HTML",
                    )
                except Exception as err:  # noqa: BLE001
                    logger.warning("failed to notify duo buyer=%s about missing member=%s: %s", buyer.telegram_id, member_id, err)
                continue

            member_lang = member_customer.language or config.default_language
            notify_text = "\n\n".join(
                [
                    self.translation.get_text(member_lang, "duo_member_notification_title") % buyer.telegram_id,
                    self.translation.get_text(member_lang, "connect_instructions"),
                    self.translation.get_text(member_lang, "subscription_link") % subscription_url,
                ]
            )
            try:
                await self.bot.send_message(member_id, notify_text, parse_mode="HTML")
            except Exception as err:  # noqa: BLE001
                logger.warning("failed to notify duo member=%s for purchase=%s: %s", member_id, purchase.id, err)
                await self.gift_notification_repo.create(
                    recipient_telegram_id=member_id,
                    sender_telegram_id=buyer.telegram_id,
                    months=0,
                    days=0,
                    message=notify_text,
                    purchase_id=purchase.id,
                )
                try:
                    await self.bot.send_message(
                        buyer.telegram_id,
                        self.translation.get_text(buyer.language, "duo_member_notify_failed") % member_id,
                        parse_mode="HTML",
                    )
                except Exception as notify_err:  # noqa: BLE001
                    logger.warning(
                        "failed to notify duo buyer=%s about failed member notification=%s: %s",
                        buyer.telegram_id,
                        member_id,
                        notify_err,
                    )

    async def apply_promo_code(self, customer: Customer, promo_code: str, username: Optional[str]) -> str:
        code = promo_code.strip()
        if not code:
            return "empty"
        promo = await self.promo_repo.find_by_code(code.upper())
        if not promo:
            return "not_found"

        redeem_status = await self.promo_repo.redeem(promo, customer.id)
        if redeem_status != "ok":
            return redeem_status

        extra_days = promo.days
        extra_bytes = getattr(promo, "traffic_gb", 0) * 1_073_741_824 if promo else 0
        base_limit = config.traffic_limit_bytes
        try:
            existing = await self.remnawave_client.fetch_user_by_telegram(customer.telegram_id)
            if existing and existing.traffic_limit_bytes:
                base_limit = existing.traffic_limit_bytes
        except Exception as err:  # noqa: BLE001
            logger.debug("fetch user for promo failed: %s", err)

        user = await self.remnawave_client.create_or_update_user(
            customer_id=customer.id,
            telegram_id=customer.telegram_id,
            traffic_limit_bytes=base_limit + extra_bytes,
            days=extra_days,
            is_trial_user=False,
            username=username,
        )
        await self.customer_repo.update_fields(
            customer.id,
            {"subscription_link": user.subscription_url, "expire_at": user.expire_at.isoformat()},
        )
        try:
            await self.bot.send_message(
                customer.telegram_id,
                self.translation.get_text(customer.language, "promo_applied") % (promo.days, getattr(promo, "traffic_gb", 0)),
                reply_markup=InlineKeyboardMarkup(inline_keyboard=self._connect_keyboard(customer.language)),
            )
        except Exception as err:  # noqa: BLE001
            logger.warning("failed to notify promo applied: %s", err)
        logger.info("promo applied code=%s customer=%s", code, customer.telegram_id)
        return "ok"

    async def _maybe_grant_referral_bonus(self, customer: Customer) -> None:
        referral = await self.referral_repo.find_by_referee(customer.telegram_id)
        if not referral or referral.bonus_granted:
            return
        referrer_customer = await self.customer_repo.find_by_telegram_id(referral.referrer_id)
        if not referrer_customer:
            return
        purchase_days = max(0, config.referral_purchase_days)
        if purchase_days == 0:
            await self.referral_repo.mark_bonus_granted(referral.id)
            return
        existing_referrer_user = await self.remnawave_client.fetch_user_by_telegram(referrer_customer.telegram_id)
        traffic_limit_bytes = config.traffic_limit_bytes
        if existing_referrer_user and existing_referrer_user.traffic_limit_bytes:
            traffic_limit_bytes = existing_referrer_user.traffic_limit_bytes
        user = await self.remnawave_client.create_or_update_user(
            customer_id=referrer_customer.id,
            telegram_id=referrer_customer.telegram_id,
            traffic_limit_bytes=traffic_limit_bytes,
            days=purchase_days,
            is_trial_user=False,
            username=referrer_customer.username,
        )
        await self.customer_repo.update_fields(
            referrer_customer.id,
            {"subscription_link": user.subscription_url, "expire_at": user.expire_at.isoformat()},
        )
        await self.referral_repo.mark_bonus_granted(referral.id)
        try:
            await self.bot.send_message(
                referrer_customer.telegram_id,
                self.translation.get_text(referrer_customer.language, "referral_bonus_granted") % purchase_days,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=self._connect_keyboard(referrer_customer.language)),
            )
        except Exception as err:  # noqa: BLE001
            logger.warning("failed to send referral bonus notification: %s", err)
        logger.info("referral bonus granted referrer=%s referee=%s", referrer_customer.telegram_id, customer.telegram_id)

    async def grant_referral_signup_bonus(self, referrer_telegram_id: int) -> None:
        signup_days = max(0, config.referral_signup_days)
        if signup_days == 0:
            return
        referrer_customer = await self.customer_repo.find_by_telegram_id(referrer_telegram_id)
        if not referrer_customer:
            return
        existing_referrer_user = await self.remnawave_client.fetch_user_by_telegram(referrer_customer.telegram_id)
        traffic_limit_bytes = config.traffic_limit_bytes
        if existing_referrer_user and existing_referrer_user.traffic_limit_bytes:
            traffic_limit_bytes = existing_referrer_user.traffic_limit_bytes
        user = await self.remnawave_client.create_or_update_user(
            customer_id=referrer_customer.id,
            telegram_id=referrer_customer.telegram_id,
            traffic_limit_bytes=traffic_limit_bytes,
            days=signup_days,
            is_trial_user=False,
            username=referrer_customer.username,
        )
        await self.customer_repo.update_fields(
            referrer_customer.id,
            {"subscription_link": user.subscription_url, "expire_at": user.expire_at.isoformat()},
        )

    async def _notify_owner_about_purchase(
        self,
        customer: Customer,
        purchase: Purchase,
        plan: str,
        previous_expire_at: Optional[datetime],
        current_expire_at: Optional[datetime],
    ) -> None:
        chat_ids = list(config.log_chat_ids)
        if not chat_ids:
            return

        now = datetime.utcnow()
        is_gift = bool(purchase.gift_sender_telegram_id and purchase.gift_recipient_telegram_id)
        if plan in {"topup10", "topup20", "topup50"}:
            event_title = "Докупка трафика"
        elif is_gift:
            event_title = "Подарочная подписка"
        elif previous_expire_at and previous_expire_at > now:
            event_title = "Продление подписки"
        else:
            event_title = "Новая покупка подписки"

        username = f"@{customer.username}" if customer.username else "-"
        currency = (purchase.currency or "").upper()
        if currency in {"STARS", "XTR"}:
            amount_text = f"{purchase.amount} ⭐ (~{float(purchase.amount):.2f} ₽)"
        else:
            amount_text = f"{purchase.amount} {purchase.currency or 'RUB'}"
        expire_value = current_expire_at.strftime("%d.%m.%Y %H:%M") if current_expire_at else "-"
        paid_at_value = purchase.paid_at.strftime("%d.%m.%Y %H:%M") if purchase.paid_at else "-"
        text = (
            f"🔔 <b>{event_title}</b>\n"
            f"Purchase ID: <code>{purchase.id}</code>\n"
            f"ID: <code>{customer.telegram_id}</code>\n"
            f"Username: {username}\n"
            f"План: <b>{plan}</b>\n"
            f"Сумма: <b>{amount_text}</b>\n"
            f"Платеж: <b>{purchase.invoice_type or '-'}</b>\n"
            f"Оплачен: <b>{paid_at_value}</b>\n"
            f"Действует до: <b>{expire_value}</b>"
        )
        if is_gift:
            text += f"\nДаритель: <code>{purchase.gift_sender_telegram_id}</code>"
        for chat_id in chat_ids:
            try:
                await self.bot.send_message(chat_id, text, parse_mode="HTML")
            except Exception as err:  # noqa: BLE001
                logger.warning("failed to notify chat=%s about purchase=%s: %s", chat_id, purchase.id, err)

    def _connect_keyboard(self, lang: str) -> List[List[InlineKeyboardButton]]:
        buttons: List[List[InlineKeyboardButton]] = []
        if config.mini_app_url:
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=self.translation.get_text(lang, "connect_button"),
                        web_app={"url": config.mini_app_url},
                        style="primary",
                        icon_custom_emoji_id=self._button_emoji_id(lang, "connect_button"),
                    )
                ]
            )
        else:
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=self.translation.get_text(lang, "connect_button"),
                        callback_data="connect",
                        style="primary",
                        icon_custom_emoji_id=self._button_emoji_id(lang, "connect_button"),
                    )
                ]
            )
        buttons.append(
            [InlineKeyboardButton(text=self.translation.get_text(lang, "back_button"), callback_data="start", style="primary", icon_custom_emoji_id=self._button_emoji_id(lang, "back_button"))]
        )
        return buttons

    async def create_purchase(
        self,
        amount: float,
        months: int,
        customer: Customer,
        invoice_type: str,
        username: Optional[str],
        plan: str = "standard",
    ) -> Tuple[str, int, Optional[Dict[str, Any]]]:
        if invoice_type == "crypto":
            return await self._create_crypto_invoice(amount, months, customer, username, plan)
        if invoice_type == "yookasa":
            return await self._create_yookasa_invoice(amount, months, customer, username, plan)
        if invoice_type == "platega":
            return await self._create_platega_invoice(amount, months, customer, username, plan)
        if invoice_type == "telegram":
            return await self._create_telegram_invoice(amount, months, customer, username, plan)
        if invoice_type == "tribute":
            return await self._create_tribute_invoice(amount, months, customer)
        raise ValueError(f"unknown invoice type {invoice_type}")

    async def cancel_tribute_purchase(self, telegram_id: int) -> None:
        customer = await self.customer_repo.find_by_telegram_id(telegram_id)
        if not customer:
            raise RuntimeError("customer not found")
        tribute_purchase = await self.purchase_repo.find_by_customer_id_and_invoice_type_last(
            customer.id, "tribute"
        )
        if not tribute_purchase:
            raise RuntimeError("tribute purchase not found")
        new_expire = await self.remnawave_client.decrease_subscription(
            telegram_id, config.traffic_limit_bytes, -tribute_purchase.month * config.days_in_month
        )
        if new_expire:
            await self.customer_repo.update_fields(customer.id, {"expire_at": new_expire.isoformat()})
        await self.purchase_repo.update_fields(tribute_purchase.id, {"status": "cancel"})
        try:
            await self.bot.send_message(
                telegram_id,
                self.translation.get_text(customer.language, "tribute_cancelled"),
                parse_mode="HTML",
            )
        except Exception as err:  # noqa: BLE001
            logger.warning("failed to send tribute cancel message: %s", err)

    async def _create_crypto_invoice(
        self, amount: float, months: int, customer: Customer, username: Optional[str], plan: str
    ) -> Tuple[str, int, None]:
        if not self.crypto_client:
            raise RuntimeError("CryptoPay disabled")
        purchase = Purchase(
            id=0,
            amount=amount,
            customer_id=customer.id,
            created_at=datetime.utcnow(),
            month=months,
            paid_at=None,
            currency="RUB",
            expire_at=None,
            status="new",
            invoice_type="crypto",
            plan=plan,
            crypto_invoice_id=None,
            crypto_invoice_url=None,
            yookasa_url=None,
            yookasa_id=None,
        )
        purchase_id = await self.purchase_repo.create(purchase)
        invoice = await self.crypto_client.create_invoice(
            {
                "currency_type": "fiat",
                "fiat": "RUB",
                "amount": int(amount),
                "accepted_assets": "USDT",
                "payload": f"purchaseId={purchase_id}&username={username or ''}",
                "description": f"Subscription on {months} month",
                "paid_btn_name": "callback",
                "paid_btn_url": config.bot_url or "",
            }
        )
        await self.purchase_repo.update_fields(
            purchase_id,
            {
                "crypto_invoice_url": invoice.get("bot_invoice_url") or invoice.get("botInvoiceUrl"),
                "crypto_invoice_id": invoice.get("invoice_id") or invoice.get("invoiceId"),
                "status": "pending",
            },
        )
        url = invoice.get("bot_invoice_url") or invoice.get("botInvoiceUrl") or ""
        return url, purchase_id, None

    async def _create_platega_invoice(
        self, amount: float, months: int, customer: Customer, username: Optional[str], plan: str
    ) -> Tuple[str, int, Optional[Dict[str, Any]]]:
        if not self.platega_client:
            raise RuntimeError("Platega disabled")
        purchase = Purchase(
            id=0,
            amount=amount,
            customer_id=customer.id,
            created_at=datetime.utcnow(),
            month=months,
            paid_at=None,
            currency="RUB",
            expire_at=None,
            status="new",
            invoice_type="platega",
            plan=plan,
            crypto_invoice_id=None,
            crypto_invoice_url=None,
            yookasa_url=None,
            yookasa_id=None,
        )
        purchase_id = await self.purchase_repo.create(purchase)
        description = self.translation.get_text(customer.language, "invoice_description") or "VPN subscription"
        plan_label = self._format_plan_label(plan, months)
        description = f"{description} - {plan_label}"
        payload = f"purchaseId={purchase_id}&username={username or ''}"
        return_url = config.platega_return_url or config.bot_url or config.support_url or config.channel_url or ""
        failed_url = config.platega_failed_url or config.support_url or return_url
        try:
            payment_method = int(config.platega_payment_method or 2)
        except Exception:  # noqa: BLE001
            payment_method = 2
        invoice = await self.platega_client.create_transaction(
            amount=int(amount),
            description=description,
            return_url=return_url,
            failed_url=failed_url or return_url,
            payload=payload,
            payment_method=payment_method,
        )
        transaction_id = invoice.get("transactionId") or invoice.get("id")
        redirect_url = (
            invoice.get("redirect")
            or invoice.get("redirectUrl")
            or invoice.get("qr")
            or invoice.get("url")
            or invoice.get("link")
        )
        await self.purchase_repo.update_fields(
            purchase_id,
            {
                "platega_transaction_id": transaction_id,
                "platega_redirect_url": redirect_url,
                "status": "pending",
            },
        )
        return redirect_url or "", purchase_id, invoice

    async def _create_yookasa_invoice(
        self, amount: float, months: int, customer: Customer, username: Optional[str], plan: str
    ) -> Tuple[str, int, Optional[Dict[str, Any]]]:
        if not self.yookassa_client:
            raise RuntimeError("Yookassa disabled")
        purchase = Purchase(
            id=0,
            amount=amount,
            customer_id=customer.id,
            created_at=datetime.utcnow(),
            month=months,
            paid_at=None,
            currency="RUB",
            expire_at=None,
            status="new",
            invoice_type="yookasa",
            plan=plan,
            crypto_invoice_id=None,
            crypto_invoice_url=None,
            yookasa_url=None,
            yookasa_id=None,
        )
        purchase_id = await self.purchase_repo.create(purchase)
        invoice = await self.yookassa_client.create_invoice(
            int(amount), months, customer.id, purchase_id, username
        )
        confirmation = invoice.get("confirmation", {})
        await self.purchase_repo.update_fields(
            purchase_id,
            {"yookasa_url": confirmation.get("confirmation_url"), "yookasa_id": invoice.get("id"), "status": "pending"},
        )
        return confirmation.get("confirmation_url", ""), purchase_id, invoice

    async def _create_telegram_invoice(
        self, amount: float, months: int, customer: Customer, username: Optional[str], plan: str
    ) -> Tuple[str, int, None]:
        purchase = Purchase(
            id=0,
            amount=amount,
            customer_id=customer.id,
            created_at=datetime.utcnow(),
            month=months,
            paid_at=None,
            currency="STARS",
            expire_at=None,
            status="new",
            invoice_type="telegram",
            plan=plan,
            crypto_invoice_id=None,
            crypto_invoice_url=None,
            yookasa_url=None,
            yookasa_id=None,
        )
        purchase_id = await self.purchase_repo.create(purchase)
        invoice_url = await self.bot.create_invoice_link(
            title=self.translation.get_text(customer.language, "invoice_title"),
            currency="XTR",
            prices=[LabeledPrice(label=self.translation.get_text(customer.language, "invoice_label"), amount=int(amount))],
            description=self.translation.get_text(customer.language, "invoice_description"),
            payload=f"{purchase_id}&{username or ''}",
        )
        await self.purchase_repo.update_fields(purchase_id, {"status": "pending"})
        return invoice_url, purchase_id, None

    async def _create_tribute_invoice(
        self, amount: float, months: int, customer: Customer
    ) -> Tuple[str, int, None]:
        purchase = Purchase(
            id=0,
            amount=amount,
            customer_id=customer.id,
            created_at=datetime.utcnow(),
            month=months,
            paid_at=None,
            currency="RUB",
            expire_at=None,
            status="pending",
            invoice_type="tribute",
            plan="tribute",
            crypto_invoice_id=None,
            crypto_invoice_url=None,
            yookasa_url=None,
            yookasa_id=None,
        )
        purchase_id = await self.purchase_repo.create(purchase)
        return "", purchase_id, None

    async def refresh_customer_subscription(self, customer: Customer) -> Customer:
        user = await self.remnawave_client.fetch_user_by_telegram(customer.telegram_id)
        if not user:
            return customer
        updates: Dict[str, Any] = {}
        if user.subscription_url:
            updates["subscription_link"] = user.subscription_url
        if user.expire_at:
            updates["expire_at"] = user.expire_at.isoformat()
        if not updates:
            return customer
        await self.customer_repo.update_fields(customer.id, updates)
        refreshed = await self.customer_repo.find_by_id(customer.id)
        return refreshed or customer

    async def activate_trial(self, telegram_id: int, username: Optional[str]) -> str:
        if config.trial_days == 0:
            return ""
        customer = await self.customer_repo.find_by_telegram_id(telegram_id)
        if not customer:
            raise RuntimeError(f"customer {telegram_id} not found")
        user = await self.remnawave_client.create_or_update_user(
            customer_id=customer.id,
            telegram_id=telegram_id,
            traffic_limit_bytes=config.trial_traffic_limit_bytes,
            days=config.trial_days,
            is_trial_user=True,
            username=username,
        )
        await self.customer_repo.update_fields(
            customer.id,
            {"subscription_link": user.subscription_url, "expire_at": user.expire_at.isoformat()},
        )
        return user.subscription_url or ""

    async def cancel_yookassa_payment(self, purchase_id: int) -> None:
        await self.purchase_repo.update_fields(purchase_id, {"status": "cancel"})

    async def cancel_platega_payment(self, purchase_id: int) -> None:
        await self.purchase_repo.update_fields(purchase_id, {"status": "cancel"})

    def _format_plan_label(self, plan: str, months: int) -> str:
        if plan == "duo":
            return f"Duo {months}m"
        if plan == "family":
            return f"Family {months}m"
        if plan == "topup10":
            return "Topup +10GB"
        if plan == "topup20":
            return "Topup +20GB"
        if plan == "topup50":
            return "Topup +50GB"
        if months:
            return f"Standard {months}m"
        return plan

    async def _send_receipt_to_moynalog(self, purchase: Purchase) -> None:
        if not self.moynalog_client:
            return
        try:
            month = purchase.month
            month_string = "month" if month == 1 else "months"
            comment = f"Subscription payment: {month} {month_string}"
            await self.moynalog_client.create_income(purchase.amount, comment)
            logger.info("moynalog receipt sent purchase_id=%s", purchase.id)
        except Exception as err:  # noqa: BLE001
            logger.exception("failed to send Moynalog receipt: %s", err)
            for chat_id in config.log_chat_ids:
                try:
                    await self.bot.send_message(
                        chat_id,
                        f"⚠️ Moynalog error for purchase {purchase.id}: {err}",
                    )
                except Exception as send_err:  # noqa: BLE001
                    logger.warning("failed to notify chat=%s about Moynalog issue: %s", chat_id, send_err)


class SubscriptionService:
    def __init__(
        self,
        customer_repo: CustomerRepository,
        purchase_repo: PurchaseRepository,
        payment_service: PaymentService,
        bot: Bot,
        translation: TranslationManager,
    ) -> None:
        self.customer_repo = customer_repo
        self.purchase_repo = purchase_repo
        self.payment_service = payment_service
        self.bot = bot
        self.translation = translation

    async def process_subscription_expiration(self) -> None:
        now = datetime.utcnow()
        customers = await self.customer_repo.find_by_expiration_range(now, now + timedelta(days=3))
        if not customers:
            return
        customer_ids = [c.id for c in customers]
        tributes = await self.purchase_repo.find_latest_active_tributes_by_customer_ids(customer_ids)
        tribute_map = {p.customer_id: p for p in tributes}

        for customer in customers:
            if not customer.expire_at:
                continue
            days_until_expire = self._days_until_expiration(now, customer.expire_at)
            if customer.id in tribute_map and days_until_expire == 1:
                tribute = tribute_map[customer.id]
                try:
                    _, purchase_id, _ = await self.payment_service.create_purchase(
                        tribute.amount, tribute.month, customer, "tribute", username=None
                    )
                    await self.payment_service.process_purchase_by_id(purchase_id, username=None)
                    logger.info("processed tribute renewal for customer=%s", customer.id)
                except Exception as err:  # noqa: BLE001
                    logger.exception("failed to process tribute renewal: %s", err)
                continue

            await self._send_notification(customer)

    def _days_until_expiration(self, now: datetime, expire_at: datetime) -> int:
        now_date = datetime(now.year, now.month, now.day)
        expire_date = datetime(expire_at.year, expire_at.month, expire_at.day)
        duration = expire_date - now_date
        return int(duration.total_seconds() // 86400)

    async def _send_notification(self, customer: Customer) -> None:
        if not customer.expire_at or not customer.notifications_enabled:
            return
        expire_date = customer.expire_at.strftime("%d.%m.%Y")
        message_text = self.translation.get_text(customer.language, "subscription_expiring") % expire_date
        try:
            await self.bot.send_message(
                customer.telegram_id,
                message_text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text=self.translation.get_text(customer.language, "renew_subscription_button"),
                                callback_data="buy",
                            )
                        ]
                    ]
                ),
            )
            logger.info("sent expiration notification to %s", customer.telegram_id)
        except Exception as err:  # noqa: BLE001
            logger.warning("failed to send expiration notification: %s", err)


class SyncService:
    def __init__(self, remnawave_client: RemnawaveClient, customer_repo: CustomerRepository) -> None:
        self.remnawave_client = remnawave_client
        self.customer_repo = customer_repo

    async def sync(
        self,
        progress_cb: Optional[Callable[[str], Awaitable[None]]] = None,
        prune_missing: bool = False,
    ) -> Dict[str, int]:
        async def _report(text: str) -> None:
            if progress_cb:
                await progress_cb(text)

        await _report("Sync: fetching users from API...")
        users = await self.remnawave_client.get_users()
        if not users:
            logger.warning("no users fetched from remnawave")
            await _report("Sync: API returned 0 users")
            return {
                "fetched": 0,
                "with_telegram_id": 0,
                "skipped_without_telegram_id": 0,
                "skipped_duplicates": 0,
                "created": 0,
                "updated": 0,
                "deleted": 0,
            }
        await _report(f"Sync: fetched {len(users)} users, preparing data...")
        telegram_ids: List[int] = []
        mapped_users: List[Customer] = []
        seen: set[int] = set()
        skipped_without_telegram_id = 0
        skipped_duplicates = 0
        for user in users:
            if user.telegram_id is None:
                skipped_without_telegram_id += 1
                continue
            if user.telegram_id in seen:
                skipped_duplicates += 1
                continue
            seen.add(user.telegram_id)
            telegram_ids.append(user.telegram_id)
            mapped_users.append(
                Customer(
                    id=0,
                    telegram_id=user.telegram_id,
                    expire_at=user.expire_at,
                    created_at=datetime.utcnow(),
                    subscription_link=user.subscription_url,
                    language=config.default_language,
                    username=None,
                )
            )

        existing = await self.customer_repo.find_by_telegram_ids(telegram_ids)
        existing_map = {c.telegram_id: c for c in existing}
        to_create: List[Customer] = []
        to_update: List[Customer] = []
        for cust in mapped_users:
            if cust.telegram_id in existing_map:
                existing_customer = existing_map[cust.telegram_id]
                cust.id = existing_customer.id
                cust.created_at = existing_customer.created_at
                cust.language = existing_customer.language or config.default_language
                to_update.append(cust)
            else:
                to_create.append(cust)

        deleted_count = 0
        if prune_missing:
            await self.customer_repo.delete_by_not_in_telegram_ids(telegram_ids)
            deleted_count = -1
            await _report("Sync: prune enabled, removed users missing in API")
        created_count = 0
        updated_count = 0
        await _report(
            f"Sync: applying changes (create={len(to_create)}, update={len(to_update)})..."
        )
        if to_create:
            await self.customer_repo.create_batch(to_create)
            logger.info("sync created customers count=%s", len(to_create))
            created_count = len(to_create)
        if to_update:
            await self.customer_repo.update_batch(to_update)
            logger.info("sync updated customers count=%s", len(to_update))
            updated_count = len(to_update)
        logger.info("synchronization completed")
        await _report("Sync: completed")
        return {
            "fetched": len(users),
            "with_telegram_id": len(telegram_ids),
            "skipped_without_telegram_id": skipped_without_telegram_id,
            "skipped_duplicates": skipped_duplicates,
            "created": created_count,
            "updated": updated_count,
            "deleted": deleted_count,
        }

    async def get_traffic_usage(self, telegram_id: int) -> Tuple[int, int, bool]:
        user = await self.remnawave_client.fetch_user_by_telegram(telegram_id)
        if user and user.traffic_limit_bytes is not None:
            used_val = user.traffic_used_bytes or 0
            return used_val, user.traffic_limit_bytes, True

        users = await self.remnawave_client.get_users()
        if not users:
            return 0, 0, False
        for item in users:
            if item.telegram_id != telegram_id:
                continue
            if item.traffic_limit_bytes is not None:
                return item.traffic_used_bytes or 0, item.traffic_limit_bytes, True
        return 0, 0, False


class StatsService:
    def __init__(
        self,
        remnawave_client: RemnawaveClient,
        purchase_repo: PurchaseRepository,
        sales_repo: SalesRepository,
        customer_repo: CustomerRepository,
        bot: Bot,
    ) -> None:
        self.remnawave_client = remnawave_client
        self.purchase_repo = purchase_repo
        self.sales_repo = sales_repo
        self.customer_repo = customer_repo
        self.bot = bot

    def _timezone(self) -> ZoneInfo:
        try:
            return ZoneInfo(config.stats_timezone)
        except Exception:  # noqa: BLE001
            return ZoneInfo("Asia/Yekaterinburg")

    def _bounds_utc_for_local_day(self, target_date: date) -> Tuple[datetime, datetime]:
        tz = self._timezone()
        start_local = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=tz)
        end_local = start_local + timedelta(days=1)
        start_utc = start_local.astimezone(timezone.utc).replace(tzinfo=None)
        end_utc = end_local.astimezone(timezone.utc).replace(tzinfo=None)
        return start_utc, end_utc

    async def build_traffic_users_report_for_local_day(self, target_date: date) -> str:
        start_utc, end_utc = self._bounds_utc_for_local_day(target_date)
        new_users = await self.customer_repo.count_new_in_period(start_utc, end_utc)
        paid_count = await self.sales_repo.count_paid_in_period(start_utc, end_utc)
        new_paid_customers = await self.sales_repo.count_new_paid_customers_in_period(start_utc, end_utc)
        renewals = max(0, paid_count - new_paid_customers)

        users = await self.remnawave_client.get_users()
        now = datetime.utcnow()
        active = 0
        expired = 0
        expiring_soon = 0
        traffic_used_sum = 0
        traffic_limit_sum = 0
        active_used_sum = 0
        active_limit_sum = 0
        for user in users:
            if user.expire_at and user.expire_at > now:
                active += 1
                active_used_sum += int(user.traffic_used_bytes or 0)
                active_limit_sum += int(user.traffic_limit_bytes or 0)
                if user.expire_at <= now + timedelta(days=3):
                    expiring_soon += 1
            else:
                expired += 1
            traffic_used_sum += int(user.traffic_used_bytes or 0)
            traffic_limit_sum += int(user.traffic_limit_bytes or 0)

        used_gb = round(traffic_used_sum / 1_073_741_824, 2)
        limit_gb = round(traffic_limit_sum / 1_073_741_824, 2)
        utilization = round((traffic_used_sum / traffic_limit_sum) * 100, 2) if traffic_limit_sum > 0 else 0.0
        active_used_gb = round(active_used_sum / 1_073_741_824, 2)
        active_limit_gb = round(active_limit_sum / 1_073_741_824, 2)
        active_utilization = round((active_used_sum / active_limit_sum) * 100, 2) if active_limit_sum > 0 else 0.0
        avg_per_active_gb = round((active_used_sum / max(active, 1)) / 1_073_741_824, 2) if active > 0 else 0.0
        local_day = target_date.strftime("%d.%m.%Y")
        return (
            f"📈 <b>Трафик и пользователи за {local_day} (Екб)</b>\n\n"
            f"Новые пользователи: <b>{new_users}</b>\n"
            f"Оплаты: <b>{paid_count}</b>\n"
            f"Новые оплаты: <b>{new_paid_customers}</b>\n"
            f"Продления: <b>{renewals}</b>\n"
            f"Пользователи Remnawave (snapshot):\n"
            f"Всего: <b>{len(users)}</b>\n"
            f"Активные: <b>{active}</b>\n"
            f"Истекшие: <b>{expired}</b>\n"
            f"Истекают за 3 дня: <b>{expiring_soon}</b>\n\n"
            f"Трафик (snapshot): <b>{used_gb}</b> / <b>{limit_gb}</b> ГБ ({utilization}%)\n"
            f"Нагрузка активных: <b>{active_used_gb}</b> / <b>{active_limit_gb}</b> ГБ ({active_utilization}%)\n"
            f"Средний расход на активного: <b>{avg_per_active_gb}</b> ГБ"
        )

    async def build_financial_report_for_local_day(self, target_date: date) -> str:
        start_utc, end_utc = self._bounds_utc_for_local_day(target_date)
        totals = await self.sales_repo.finance_totals_in_period(start_utc, end_utc)
        sales_count = int(totals["sales_count"])
        revenue_rub = float(totals["revenue_rub"])
        stars_amount = float(totals["stars_amount"])
        stars_revenue_rub = float(totals["stars_revenue_rub"])
        fiat_revenue_rub = float(totals["fiat_revenue_rub"])
        new_paid_customers = await self.sales_repo.count_new_paid_customers_in_period(start_utc, end_utc)
        renewals = max(0, sales_count - new_paid_customers)
        avg_check = round(revenue_rub / sales_count, 2) if sales_count > 0 else 0.0
        local_day = target_date.strftime("%d.%m.%Y")
        return (
            f"💰 <b>Финансовая статистика за {local_day} (Екб)</b>\n\n"
            f"Продаж: <b>{sales_count}</b>\n"
            f"Новые платящие: <b>{new_paid_customers}</b>\n"
            f"Продления: <b>{renewals}</b>\n"
            f"Доход за день: <b>{revenue_rub:.2f} ₽</b>\n"
            f"Из Stars: <b>{stars_amount:.2f} ⭐</b> (учтено как <b>{stars_revenue_rub:.2f} ₽</b>, курс 1⭐=1₽)\n"
            f"Из фиата: <b>{fiat_revenue_rub:.2f} ₽</b>\n"
            f"Средний чек: <b>{avg_check:.2f} ₽</b>"
        )

    async def build_report_for_local_day(self, target_date: date) -> str:
        traffic_report = await self.build_traffic_users_report_for_local_day(target_date)
        finance_report = await self.build_financial_report_for_local_day(target_date)
        return f"{traffic_report}\n\n{finance_report}"

    async def _send_to_report_chats(self, text: str) -> None:
        chat_ids = list(config.report_chat_ids)
        if not chat_ids:
            return
        for chat_id in chat_ids:
            try:
                await self.bot.send_message(chat_id, text, parse_mode="HTML")
            except Exception as err:  # noqa: BLE001
                logger.warning("failed to send stats report to chat=%s: %s", chat_id, err)

    async def send_traffic_users_report_for_local_day(self, target_date: date) -> None:
        report = await self.build_traffic_users_report_for_local_day(target_date)
        await self._send_to_report_chats(report)

    async def send_financial_report_for_local_day(self, target_date: date) -> None:
        report = await self.build_financial_report_for_local_day(target_date)
        await self._send_to_report_chats(report)

    async def send_report_for_local_day(self, target_date: date) -> None:
        report = await self.build_report_for_local_day(target_date)
        await self._send_to_report_chats(report)
