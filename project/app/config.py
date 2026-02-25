import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

from dotenv import load_dotenv


def _load_env_from_candidates() -> None:
    """Load .env from cwd or package root unless explicitly disabled."""
    if os.getenv("DISABLE_ENV_FILE") == "true":
        return

    candidates = [
        Path(".env"),
        Path(__file__).resolve().parents[1] / ".env",
    ]
    for path in candidates:
        if path.exists():
            load_dotenv(path)
            return



def _as_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _as_int(value: Optional[str], default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _parse_int_list(raw: str) -> Set[int]:
    result: Set[int] = set()
    if not raw:
        return result
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            result.add(int(part))
        except ValueError:
            continue
    return result


def _parse_uuid_map(raw: str) -> Dict[uuid.UUID, uuid.UUID]:
    values: Dict[uuid.UUID, uuid.UUID] = {}
    if not raw:
        return values
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            parsed = uuid.UUID(part)
        except ValueError:
            continue
        values[parsed] = parsed
    return values


def _parse_headers(raw: str) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    if not raw:
        return headers
    for pair in raw.split(";"):
        if not pair.strip():
            continue
        key, _, value = pair.partition(":")
        key = key.strip()
        value = value.strip()
        if key and value:
            headers[key] = value
    return headers


@dataclass
class Config:
    bot_token: str
    db_path: Path

    price_1: int
    price_3: int
    price_6: int
    price_12: int
    price_duo: int
    price_family: int

    stars_price_1: int
    stars_price_3: int
    stars_price_6: int
    stars_price_12: int

    default_language: str

    remnawave_url: str
    remnawave_token: str
    remnawave_mode: str = "remote"
    remnawave_tag: str = ""
    trial_remnawave_tag: str = ""
    remnawave_headers: Dict[str, str] = field(default_factory=dict)

    crypto_pay_enabled: bool = False
    crypto_pay_url: str = ""
    crypto_pay_token: str = ""

    platega_enabled: bool = False
    platega_base_url: str = "https://app.platega.io"
    platega_merchant_id: str = ""
    platega_secret: str = ""
    platega_return_url: str = ""
    platega_failed_url: str = ""
    platega_webhook_path: str = ""
    platega_payment_method: int = 2

    yookasa_enabled: bool = False
    yookasa_url: str = ""
    yookasa_shop_id: str = ""
    yookasa_secret_key: str = ""
    yookasa_email: str = ""
    bot_url: str = ""

    traffic_limit_gb: int = 0
    referral_days: int = 0
    referral_signup_days: int = 1
    referral_purchase_days: int = 10

    telegram_stars_enabled: bool = False
    require_paid_purchase_for_stars: bool = False
    topup_10_price_stars: int = 1
    topup_20_price_stars: int = 2
    topup_50_price_stars: int = 5

    traffic_limit_reset_strategy: str = "MONTH"
    trial_traffic_limit_reset_strategy: str = "MONTH"
    duo_traffic_limit_gb: int = 0
    family_traffic_limit_gb: int = 0

    mini_app_url: str = ""
    is_web_app_link: bool = False

    feedback_url: str = ""
    channel_url: str = ""
    server_status_url: str = ""
    support_url: str = ""
    tos_url: str = ""

    admin_telegram_id: int = 0
    notify_telegram_ids: Set[int] = field(default_factory=set)
    log_group_id: int = -1003299002180
    log_chat_ids: Set[int] = field(default_factory=set)
    report_chat_ids: Set[int] = field(default_factory=set)
    stats_timezone: str = "Asia/Yekaterinburg"
    daily_stats_hour: int = 9
    daily_traffic_report_hour: int = 7
    daily_finance_report_hour: int = 10

    trial_days: int = 0
    trial_traffic_limit_gb: int = 0
    trial_internal_squads: Dict[uuid.UUID, uuid.UUID] = field(default_factory=dict)
    trial_external_squad_uuid: Optional[uuid.UUID] = None

    days_in_month: int = 30
    squad_uuids: Dict[uuid.UUID, uuid.UUID] = field(default_factory=dict)
    external_squad_uuid: Optional[uuid.UUID] = None

    blocked_telegram_ids: Set[int] = field(default_factory=set)
    whitelisted_telegram_ids: Set[int] = field(default_factory=set)

    enable_auto_payment: bool = False
    health_check_port: int = 8080

    tribute_webhook_url: str = ""
    tribute_api_key: str = ""
    tribute_payment_url: str = ""

    moynalog_enabled: bool = False
    moynalog_url: str = "https://moynalog.ru/api/v1"
    moynalog_username: str = ""
    moynalog_password: str = ""

    @property
    def traffic_limit_bytes(self) -> int:
        return self.traffic_limit_gb * 1_073_741_824

    @property
    def trial_traffic_limit_bytes(self) -> int:
        return self.trial_traffic_limit_gb * 1_073_741_824

    @property
    def duo_traffic_limit_bytes(self) -> int:
        return self.duo_traffic_limit_gb * 1_073_741_824

    @property
    def family_traffic_limit_bytes(self) -> int:
        return self.family_traffic_limit_gb * 1_073_741_824

    @classmethod
    def load(cls) -> "Config":
        _load_env_from_candidates()

        bot_token = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
        if not bot_token:
            raise RuntimeError("BOT_TOKEN is required")

        db_path = Path(os.getenv("DB_PATH", "bot.db"))

        price_1 = _as_int(os.getenv("PRICE_1"), 0)
        price_3 = _as_int(os.getenv("PRICE_3"), 0)
        price_6 = _as_int(os.getenv("PRICE_6"), 0)
        price_12 = _as_int(os.getenv("PRICE_12"), 0)
        price_duo = _as_int(os.getenv("PRICE_DUO"), 150)
        price_family = _as_int(os.getenv("PRICE_FAMILY"), 200)
        topup_10_price_stars = _as_int(os.getenv("TOPUP_10_GB_PRICE_STARS"), 1)
        topup_20_price_stars = _as_int(os.getenv("TOPUP_20_GB_PRICE_STARS"), 2)
        topup_50_price_stars = _as_int(os.getenv("TOPUP_50_GB_PRICE_STARS"), 5)

        stars_price_1 = _as_int(os.getenv("STARS_PRICE_1"), price_1)
        stars_price_3 = _as_int(os.getenv("STARS_PRICE_3"), price_3)
        stars_price_6 = _as_int(os.getenv("STARS_PRICE_6"), price_6)
        stars_price_12 = _as_int(os.getenv("STARS_PRICE_12"), price_12)

        remnawave_mode = os.getenv("REMNAWAVE_MODE", "remote")
        if remnawave_mode not in {"remote", "local"}:
            remnawave_mode = "remote"

        external_squad_uuid = os.getenv("EXTERNAL_SQUAD_UUID")
        trial_external_squad_uuid = os.getenv("TRIAL_EXTERNAL_SQUAD_UUID")

        admin_telegram_id = _as_int(os.getenv("ADMIN_TELEGRAM_ID"), 0)
        notify_telegram_ids = _parse_int_list(os.getenv("NOTIFY_TELEGRAM_IDS", ""))
        # Owner notifications requested by project owner.
        notify_telegram_ids.add(8229399404)
        if admin_telegram_id > 0:
            notify_telegram_ids.add(admin_telegram_id)
        log_group_id = _as_int(os.getenv("LOG_GROUP_ID"), -1003299002180)
        log_chat_ids = _parse_int_list(os.getenv("LOG_TELEGRAM_IDS", ""))
        report_chat_ids = _parse_int_list(os.getenv("REPORT_TELEGRAM_IDS", ""))
        if log_group_id:
            log_chat_ids.add(log_group_id)
        if not log_chat_ids:
            log_chat_ids.update(notify_telegram_ids)
        else:
            log_chat_ids.update(notify_telegram_ids)
        if not report_chat_ids:
            report_chat_ids.update(log_chat_ids)
        report_chat_ids.update(notify_telegram_ids)

        return cls(
            bot_token=bot_token,
            db_path=db_path,
            price_1=price_1,
            price_3=price_3,
            price_6=price_6,
            price_12=price_12,
            price_duo=price_duo,
            price_family=price_family,
            stars_price_1=stars_price_1,
            stars_price_3=stars_price_3,
            stars_price_6=stars_price_6,
            stars_price_12=stars_price_12,
            default_language=os.getenv("DEFAULT_LANGUAGE", "ru"),
            remnawave_url=os.getenv("REMNAWAVE_URL", ""),
            remnawave_token=os.getenv("REMNAWAVE_TOKEN", ""),
            remnawave_mode=remnawave_mode,
            remnawave_tag=os.getenv("REMNAWAVE_TAG", ""),
            trial_remnawave_tag=os.getenv("TRIAL_REMNAWAVE_TAG", ""),
            remnawave_headers=_parse_headers(os.getenv("REMNAWAVE_HEADERS", "")),
            crypto_pay_enabled=_as_bool(os.getenv("CRYPTO_PAY_ENABLED")),
            crypto_pay_url=os.getenv("CRYPTO_PAY_URL", ""),
            crypto_pay_token=os.getenv("CRYPTO_PAY_TOKEN", ""),
            platega_enabled=_as_bool(os.getenv("PLATEGA_ENABLED")),
            platega_base_url=os.getenv("PLATEGA_BASE_URL", "https://app.platega.io"),
            platega_merchant_id=os.getenv("PLATEGA_MERCHANT_ID", ""),
            platega_secret=os.getenv("PLATEGA_SECRET", ""),
            platega_return_url=os.getenv("PLATEGA_RETURN_URL", ""),
            platega_failed_url=os.getenv("PLATEGA_FAILED_URL", ""),
            platega_webhook_path=os.getenv("PLATEGA_WEBHOOK_PATH", ""),
            platega_payment_method=_as_int(os.getenv("PLATEGA_PAYMENT_METHOD"), 2),
            yookasa_enabled=_as_bool(os.getenv("YOOKASA_ENABLED")),
            yookasa_url=os.getenv("YOOKASA_URL", ""),
            yookasa_shop_id=os.getenv("YOOKASA_SHOP_ID", ""),
            yookasa_secret_key=os.getenv("YOOKASA_SECRET_KEY", ""),
            yookasa_email=os.getenv("YOOKASA_EMAIL", ""),
            traffic_limit_gb=_as_int(os.getenv("TRAFFIC_LIMIT"), 0),
            referral_days=_as_int(os.getenv("REFERRAL_DAYS"), 0),
            referral_signup_days=_as_int(os.getenv("REFERRAL_SIGNUP_DAYS"), 1),
            referral_purchase_days=_as_int(os.getenv("REFERRAL_PURCHASE_DAYS"), 10),
            telegram_stars_enabled=_as_bool(os.getenv("TELEGRAM_STARS_ENABLED")),
            require_paid_purchase_for_stars=_as_bool(os.getenv("REQUIRE_PAID_PURCHASE_FOR_STARS")),
            topup_10_price_stars=topup_10_price_stars,
            topup_20_price_stars=topup_20_price_stars,
            topup_50_price_stars=topup_50_price_stars,
            traffic_limit_reset_strategy=os.getenv("TRAFFIC_LIMIT_RESET_STRATEGY", "MONTH").upper(),
            trial_traffic_limit_reset_strategy=os.getenv("TRIAL_TRAFFIC_LIMIT_RESET_STRATEGY", "MONTH").upper(),
            duo_traffic_limit_gb=_as_int(os.getenv("DUO_TRAFFIC_LIMIT_GB"), 200),
            family_traffic_limit_gb=_as_int(os.getenv("FAMILY_TRAFFIC_LIMIT_GB"), 300),
            mini_app_url=os.getenv("MINI_APP_URL", ""),
            is_web_app_link=_as_bool(os.getenv("IS_WEB_APP_LINK")),
            feedback_url=os.getenv("FEEDBACK_URL", ""),
            channel_url=os.getenv("CHANNEL_URL", ""),
            server_status_url=os.getenv("SERVER_STATUS_URL", ""),
            support_url=os.getenv("SUPPORT_URL", ""),
            tos_url=os.getenv("TOS_URL", ""),
            admin_telegram_id=admin_telegram_id,
            notify_telegram_ids=notify_telegram_ids,
            log_group_id=log_group_id,
            log_chat_ids=log_chat_ids,
            report_chat_ids=report_chat_ids,
            stats_timezone=os.getenv("STATS_TIMEZONE", "Asia/Yekaterinburg"),
            daily_stats_hour=_as_int(os.getenv("DAILY_STATS_HOUR"), 9),
            daily_traffic_report_hour=_as_int(os.getenv("DAILY_TRAFFIC_REPORT_HOUR"), 7),
            daily_finance_report_hour=_as_int(os.getenv("DAILY_FINANCE_REPORT_HOUR"), 10),
            trial_days=_as_int(os.getenv("TRIAL_DAYS"), 0),
            trial_traffic_limit_gb=_as_int(os.getenv("TRIAL_TRAFFIC_LIMIT"), 0),
            trial_internal_squads=_parse_uuid_map(os.getenv("TRIAL_INTERNAL_SQUADS", "")),
            trial_external_squad_uuid=uuid.UUID(trial_external_squad_uuid) if trial_external_squad_uuid else None,
            days_in_month=_as_int(os.getenv("DAYS_IN_MONTH"), 30),
            squad_uuids=_parse_uuid_map(os.getenv("SQUAD_UUIDS", "")),
            external_squad_uuid=uuid.UUID(external_squad_uuid) if external_squad_uuid else None,
            blocked_telegram_ids=_parse_int_list(os.getenv("BLOCKED_TELEGRAM_IDS", "")),
            whitelisted_telegram_ids=_parse_int_list(os.getenv("WHITELISTED_TELEGRAM_IDS", "")),
            enable_auto_payment=_as_bool(os.getenv("ENABLE_AUTO_PAYMENT")),
            health_check_port=_as_int(os.getenv("HEALTH_CHECK_PORT"), 8080),
            tribute_webhook_url=os.getenv("TRIBUTE_WEBHOOK_URL", ""),
            tribute_api_key=os.getenv("TRIBUTE_API_KEY", ""),
            tribute_payment_url=os.getenv("TRIBUTE_PAYMENT_URL", ""),
            moynalog_enabled=_as_bool(os.getenv("MOYNALOG_ENABLED")),
            moynalog_url=os.getenv("MOYNALOG_URL", "https://moynalog.ru/api/v1"),
            moynalog_username=os.getenv("MOYNALOG_USERNAME", ""),
            moynalog_password=os.getenv("MOYNALOG_PASSWORD", ""),
        )


config = Config.load()
