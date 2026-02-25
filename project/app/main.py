import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.client.bot import DefaultBotProperties
from aiohttp import web

from .bot.routers.main import setup_router
from .config import config
from .db.connection import close_db, init_db
from .db.migrations import run_migrations
from .db.queries import (
    CustomerRepository,
    DuoPurchaseMemberRepository,
    GiftNotificationRepository,
    PriceSettingRepository,
    PromoRepository,
    PurchaseRepository,
    ReferralRepository,
    SalesRepository,
)
from .logging_setup import setup_logging
from .services.business import PaymentService, StatsService, SubscriptionService, SyncService
from .services.cache import TTLCache
from .services.cryptopay import CryptoPayClient
from .services.moynalog import MoynalogClient
from .services.platega import PlategaClient, build_platega_handler
from .services.remnawave import RemnawaveClient
from .services.translation import TranslationManager
from .services.tribute import build_tribute_handler
from .services.yookassa import YookassaClient

logger = logging.getLogger(__name__)

PRICE_SETTING_KEYS = (
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
)


def _price_defaults_from_config() -> dict[str, int]:
    return {key: int(getattr(config, key)) for key in PRICE_SETTING_KEYS}


async def _apply_prices_from_db(price_repo: PriceSettingRepository) -> None:
    await price_repo.ensure_defaults(_price_defaults_from_config())
    db_prices = await price_repo.get_all_map()
    for key, value in db_prices.items():
        if hasattr(config, key):
            setattr(config, key, int(value))


async def start_health_server(
    remnawave_client: RemnawaveClient, db, tribute_handler: Optional[web.View], platega_handler: Optional[web.View]
) -> web.AppRunner:
    app = web.Application()

    async def healthcheck(request: web.Request) -> web.Response:
        status = {"status": "ok", "db": "ok", "rw": "ok", "time": datetime.utcnow().isoformat()}
        try:
            async with db.execute("SELECT 1") as cursor:
                await cursor.fetchone()
        except Exception as err:  # noqa: BLE001
            status["status"] = "fail"
            status["db"] = f"error: {err}"
        try:
            await remnawave_client.ping()
        except Exception as err:  # noqa: BLE001
            status["status"] = "fail"
            status["rw"] = f"error: {err}"
        return web.json_response(status, status=200 if status["status"] == "ok" else 503)

    app.router.add_get("/healthcheck", healthcheck)
    if tribute_handler and config.tribute_webhook_url:
        app.router.add_post(config.tribute_webhook_url, tribute_handler)
    if platega_handler and config.platega_webhook_path:
        path = config.platega_webhook_path
        if not path.startswith("/"):
            path = f"/{path}"
        app.router.add_post(path, platega_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", config.health_check_port)
    await site.start()
    logger.info("Health server started on port %s", config.health_check_port)
    return runner


async def crypto_checker(
    purchase_repo: PurchaseRepository,
    crypto_client: CryptoPayClient,
    payment_service: PaymentService,
):
    while True:
        try:
            pending = await purchase_repo.find_by_invoice_type_and_status("crypto", "pending")
            invoice_ids = [str(p.crypto_invoice_id) for p in pending if p.crypto_invoice_id]
            if invoice_ids:
                invoices = await crypto_client.get_invoices(invoice_ids=",".join(invoice_ids))
                for invoice in invoices:
                    status = invoice.get("status")
                    if status and status.lower() == "paid":
                        payload = invoice.get("payload", "")
                        parts = payload.split("&")
                        purchase_id = int(parts[0].split("=")[1])
                        username = parts[1].split("=")[1] if len(parts) > 1 and "=" in parts[1] else None
                        await payment_service.process_purchase_by_id(purchase_id, username=username)
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001
            logger.exception("crypto checker error: %s", err)
        await asyncio.sleep(5)


async def yookassa_checker(
    purchase_repo: PurchaseRepository,
    yookassa_client: YookassaClient,
    payment_service: PaymentService,
):
    while True:
        try:
            pending = await purchase_repo.find_by_invoice_type_and_status("yookasa", "pending")
            for purchase in pending:
                if not purchase.yookasa_id:
                    continue
                invoice = await yookassa_client.get_payment(purchase.yookasa_id)
                if invoice.get("status") == "canceled":
                    await payment_service.cancel_yookassa_payment(purchase.id)
                    continue
                if invoice.get("paid"):
                    metadata = invoice.get("metadata", {})
                    purchase_id = int(metadata.get("purchaseId") or purchase.id)
                    username = metadata.get("username")
                    await payment_service.process_purchase_by_id(purchase_id, username=username)
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001
            logger.exception("yookassa checker error: %s", err)
        await asyncio.sleep(5)


async def platega_checker(
    purchase_repo: PurchaseRepository,
    platega_client: PlategaClient,
    payment_service: PaymentService,
):
    while True:
        try:
            pending = await purchase_repo.find_by_invoice_type_and_status("platega", "pending")
            for purchase in pending:
                if not purchase.platega_transaction_id:
                    continue
                invoice = await platega_client.get_transaction(purchase.platega_transaction_id)
                status = (invoice.get("status") or "").upper()
                if status == "CANCELED":
                    await payment_service.cancel_platega_payment(purchase.id)
                    continue
                if status == "CONFIRMED":
                    await payment_service.process_purchase_by_id(purchase.id, username=None)
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001
            logger.exception("platega checker error: %s", err)
        await asyncio.sleep(10)


async def subscription_checker(subscription_service: SubscriptionService):
    while True:
        now = datetime.now()
        target = now.replace(hour=16, minute=0, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        try:
            await subscription_service.process_subscription_expiration()
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001
            logger.exception("subscription checker error: %s", err)


async def daily_report_checker(stats_service: StatsService, report_type: str, hour_local: int) -> None:
    hour = max(0, min(23, int(hour_local)))
    while True:
        try:
            try:
                tz = ZoneInfo(config.stats_timezone)
            except Exception:  # noqa: BLE001
                tz = ZoneInfo("Asia/Yekaterinburg")

            now = datetime.now(tz)
            target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            await asyncio.sleep((target - now).total_seconds())

            report_day = (datetime.now(tz) - timedelta(days=1)).date()
            if report_type == "traffic":
                await stats_service.send_traffic_users_report_for_local_day(report_day)
            elif report_type == "finance":
                await stats_service.send_financial_report_for_local_day(report_day)
            else:
                await stats_service.send_report_for_local_day(report_day)
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001
            logger.exception("%s daily report checker error: %s", report_type, err)
            await asyncio.sleep(60)


async def main() -> None:
    setup_logging()
    translations_path = Path(__file__).resolve().parent.parent / "translations"
    tm = TranslationManager(default_language=config.default_language)
    tm.load(translations_path)

    db = await init_db(config.db_path)
    await run_migrations(db)

    session = aiohttp.ClientSession()

    bot = Bot(token=config.bot_token, default=DefaultBotProperties(parse_mode="HTML"))
    me = await bot.get_me()
    config.bot_url = f"https://t.me/{me.username}"

    customer_repo = CustomerRepository(db)
    purchase_repo = PurchaseRepository(db)
    referral_repo = ReferralRepository(db)
    promo_repo = PromoRepository(db)
    sales_repo = SalesRepository(db)
    price_repo = PriceSettingRepository(db)
    gift_notification_repo = GiftNotificationRepository(db)
    duo_member_repo = DuoPurchaseMemberRepository(db)
    await _apply_prices_from_db(price_repo)
    cache = TTLCache(1800)

    crypto_client = CryptoPayClient(config.crypto_pay_url, config.crypto_pay_token, session) if config.crypto_pay_enabled else None
    yookassa_client = (
        YookassaClient(config.yookasa_url, config.yookasa_shop_id, config.yookasa_secret_key, session)
        if config.yookasa_enabled
        else None
    )
    platega_client = (
        PlategaClient(config.platega_base_url, config.platega_merchant_id, config.platega_secret, session)
        if config.platega_enabled and config.platega_merchant_id and config.platega_secret
        else None
    )
    moynalog_client = (
        MoynalogClient(config.moynalog_url, config.moynalog_username, config.moynalog_password, session)
        if config.moynalog_enabled
        else None
    )

    remnawave_client = RemnawaveClient(config.remnawave_url, config.remnawave_token, config.remnawave_mode, session)

    payment_service = PaymentService(
        translation=tm,
        purchase_repo=purchase_repo,
        remnawave_client=remnawave_client,
        customer_repo=customer_repo,
        bot=bot,
        crypto_client=crypto_client,
        yookassa_client=yookassa_client,
        platega_client=platega_client,
        referral_repo=referral_repo,
        promo_repo=promo_repo,
        sales_repo=sales_repo,
        gift_notification_repo=gift_notification_repo,
        duo_member_repo=duo_member_repo,
        cache=cache,
        moynalog_client=moynalog_client,
    )

    sync_service = SyncService(remnawave_client, customer_repo)
    subscription_service = SubscriptionService(customer_repo, purchase_repo, payment_service, bot, tm)
    stats_service = StatsService(remnawave_client, purchase_repo, sales_repo, customer_repo, bot)

    router = setup_router(
        bot=bot,
        tm=tm,
        payment_service=payment_service,
        sync_service=sync_service,
        customer_repo=customer_repo,
        purchase_repo=purchase_repo,
        referral_repo=referral_repo,
        promo_repo=promo_repo,
        price_repo=price_repo,
        gift_notification_repo=gift_notification_repo,
        duo_member_repo=duo_member_repo,
        stats_service=stats_service,
        bot_username=me.username,
    )

    dp = Dispatcher()
    dp.include_router(router)

    tribute_handler = None
    if config.tribute_webhook_url:
        tribute_handler = build_tribute_handler(payment_service, customer_repo)
    platega_handler = None
    if config.platega_webhook_path and platega_client:
        platega_handler = build_platega_handler(payment_service, purchase_repo)

    health_runner = await start_health_server(remnawave_client, db, tribute_handler, platega_handler)

    tasks = []
    if crypto_client:
        tasks.append(asyncio.create_task(crypto_checker(purchase_repo, crypto_client, payment_service)))
    if yookassa_client:
        tasks.append(asyncio.create_task(yookassa_checker(purchase_repo, yookassa_client, payment_service)))
    if platega_client:
        tasks.append(asyncio.create_task(platega_checker(purchase_repo, platega_client, payment_service)))
    tasks.append(asyncio.create_task(subscription_checker(subscription_service)))
    tasks.append(
        asyncio.create_task(
            daily_report_checker(stats_service, "traffic", config.daily_traffic_report_hour)
        )
    )
    tasks.append(
        asyncio.create_task(
            daily_report_checker(stats_service, "finance", config.daily_finance_report_hour)
        )
    )

    try:
        await dp.start_polling(bot)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await health_runner.cleanup()
        await session.close()
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
