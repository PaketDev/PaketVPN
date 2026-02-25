from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import aiosqlite
import sqlite3

from ..config import config


def _to_iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.isoformat()


def _from_iso(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    try:
        dt_val = datetime.fromisoformat(value)
        if dt_val.tzinfo:
            dt_val = dt_val.astimezone(timezone.utc).replace(tzinfo=None)
        return dt_val
    except ValueError:
        return None


@dataclass
class Customer:
    id: int
    telegram_id: int
    expire_at: Optional[datetime]
    created_at: datetime
    subscription_link: Optional[str]
    language: str
    username: Optional[str]
    language_selected: bool = False
    notifications_enabled: bool = True
    broadcast_enabled: bool = True


@dataclass
class Purchase:
    id: int
    amount: float
    customer_id: int
    created_at: datetime
    month: int
    paid_at: Optional[datetime]
    currency: Optional[str]
    expire_at: Optional[datetime]
    status: Optional[str]
    invoice_type: Optional[str]
    plan: Optional[str]
    crypto_invoice_id: Optional[int]
    crypto_invoice_url: Optional[str]
    yookasa_url: Optional[str]
    yookasa_id: Optional[str]
    gift_sender_telegram_id: Optional[int] = None
    gift_recipient_telegram_id: Optional[int] = None
    platega_transaction_id: Optional[str] = None
    platega_redirect_url: Optional[str] = None


@dataclass
class Referral:
    id: int
    referrer_id: int
    referee_id: int
    used_at: datetime
    bonus_granted: bool


@dataclass
class ReferralDetails:
    referral: Referral
    referee_username: Optional[str]


@dataclass
class PromoCode:
    id: int
    code: str
    days: int
    traffic_gb: int
    max_uses: int
    used: int
    created_at: datetime
    created_by: Optional[int]


@dataclass
class PromoRedemption:
    id: int
    promo_id: int
    customer_id: int
    used_at: datetime


@dataclass
class SaleLog:
    id: int
    purchase_id: int
    customer_id: int
    telegram_id: int
    amount: float
    currency: Optional[str]
    amount_rub: float
    invoice_type: Optional[str]
    plan: Optional[str]
    is_new_customer: bool
    paid_at: datetime
    created_at: datetime


@dataclass
class PriceSetting:
    key: str
    value: int
    updated_at: datetime
    updated_by: Optional[int]


@dataclass
class GiftNotification:
    id: int
    recipient_telegram_id: int
    sender_telegram_id: Optional[int]
    months: int
    days: int
    message: str
    purchase_id: Optional[int]
    delivered: bool
    created_at: datetime
    delivered_at: Optional[datetime]

def _row_to_customer(row: aiosqlite.Row) -> Customer:
    return Customer(
        id=row["id"],
        telegram_id=row["telegram_id"],
        expire_at=_from_iso(row["expire_at"]),
        created_at=_from_iso(row["created_at"]) or datetime.utcnow(),
        subscription_link=row["subscription_link"],
        language=row["language"] or config.default_language,
        username=row["username"] if "username" in row.keys() else None,
        language_selected=bool(row["language_selected"]) if "language_selected" in row.keys() else False,
        notifications_enabled=bool(row["notifications_enabled"]) if "notifications_enabled" in row.keys() else True,
        broadcast_enabled=bool(row["broadcast_enabled"]) if "broadcast_enabled" in row.keys() else True,
    )


def _row_to_purchase(row: aiosqlite.Row) -> Purchase:
    return Purchase(
        id=row["id"],
        amount=row["amount"],
        customer_id=row["customer_id"],
        created_at=_from_iso(row["created_at"]) or datetime.utcnow(),
        month=row["month"],
        paid_at=_from_iso(row["paid_at"]),
        currency=row["currency"],
        expire_at=_from_iso(row["expire_at"]),
        status=row["status"],
        invoice_type=row["invoice_type"],
        plan=row["plan"] if "plan" in row.keys() else None,
        crypto_invoice_id=row["crypto_invoice_id"],
        crypto_invoice_url=row["crypto_invoice_url"],
        yookasa_url=row["yookasa_url"],
        yookasa_id=row["yookasa_id"],
        gift_sender_telegram_id=row["gift_sender_telegram_id"] if "gift_sender_telegram_id" in row.keys() else None,
        gift_recipient_telegram_id=(
            row["gift_recipient_telegram_id"] if "gift_recipient_telegram_id" in row.keys() else None
        ),
        platega_transaction_id=row["platega_transaction_id"] if "platega_transaction_id" in row.keys() else None,
        platega_redirect_url=row["platega_redirect_url"] if "platega_redirect_url" in row.keys() else None,
    )


def _row_to_referral(row: aiosqlite.Row) -> Referral:
    return Referral(
        id=row["id"],
        referrer_id=row["referrer_id"],
        referee_id=row["referee_id"],
        used_at=_from_iso(row["used_at"]) or datetime.utcnow(),
        bonus_granted=bool(row["bonus_granted"]),
    )


def _row_to_promocode(row: aiosqlite.Row) -> PromoCode:
    return PromoCode(
        id=row["id"],
        code=row["code"],
        days=row["days"],
        traffic_gb=row["traffic_gb"] if "traffic_gb" in row.keys() else 0,
        max_uses=row["max_uses"],
        used=row["used"],
        created_at=_from_iso(row["created_at"]) or datetime.utcnow(),
        created_by=row["created_by"],
    )


def _row_to_promo_redemption(row: aiosqlite.Row) -> PromoRedemption:
    return PromoRedemption(
        id=row["id"],
        promo_id=row["promo_id"],
        customer_id=row["customer_id"],
        used_at=_from_iso(row["used_at"]) or datetime.utcnow(),
    )


def _row_to_sale_log(row: aiosqlite.Row) -> SaleLog:
    return SaleLog(
        id=row["id"],
        purchase_id=row["purchase_id"],
        customer_id=row["customer_id"],
        telegram_id=row["telegram_id"],
        amount=float(row["amount"]),
        currency=row["currency"],
        amount_rub=float(row["amount_rub"]),
        invoice_type=row["invoice_type"],
        plan=row["plan"],
        is_new_customer=bool(row["is_new_customer"]),
        paid_at=_from_iso(row["paid_at"]) or datetime.utcnow(),
        created_at=_from_iso(row["created_at"]) or datetime.utcnow(),
    )


def _row_to_price_setting(row: aiosqlite.Row) -> PriceSetting:
    return PriceSetting(
        key=row["key"],
        value=int(row["value"]),
        updated_at=_from_iso(row["updated_at"]) or datetime.utcnow(),
        updated_by=row["updated_by"],
    )


def _row_to_gift_notification(row: aiosqlite.Row) -> GiftNotification:
    return GiftNotification(
        id=row["id"],
        recipient_telegram_id=int(row["recipient_telegram_id"]),
        sender_telegram_id=row["sender_telegram_id"],
        months=int(row["months"] or 0),
        days=int(row["days"] or 0),
        message=row["message"] or "",
        purchase_id=row["purchase_id"],
        delivered=bool(row["delivered"]),
        created_at=_from_iso(row["created_at"]) or datetime.utcnow(),
        delivered_at=_from_iso(row["delivered_at"]),
    )


class CustomerRepository:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self.db = db
        self._lock = asyncio.Lock()

    async def find_by_expiration_range(self, start_date: datetime, end_date: datetime) -> List[Customer]:
        query = """
            SELECT id, telegram_id, expire_at, created_at, subscription_link, language, username,
                   language_selected, notifications_enabled, broadcast_enabled
            FROM customer
            WHERE expire_at IS NOT NULL
              AND expire_at >= ?
              AND expire_at <= ?
        """
        async with self.db.execute(query, (_to_iso(start_date), _to_iso(end_date))) as cursor:
            rows = await cursor.fetchall()
        return [_row_to_customer(row) for row in rows]

    async def find_by_id(self, customer_id: int) -> Optional[Customer]:
        query = """
            SELECT id, telegram_id, expire_at, created_at, subscription_link, language, username,
                   language_selected, notifications_enabled, broadcast_enabled
            FROM customer
            WHERE id = ?
        """
        async with self.db.execute(query, (customer_id,)) as cursor:
            row = await cursor.fetchone()
        return _row_to_customer(row) if row else None

    async def find_by_telegram_id(self, telegram_id: int) -> Optional[Customer]:
        query = """
            SELECT id, telegram_id, expire_at, created_at, subscription_link, language, username,
                   language_selected, notifications_enabled, broadcast_enabled
            FROM customer
            WHERE telegram_id = ?
        """
        async with self.db.execute(query, (telegram_id,)) as cursor:
            row = await cursor.fetchone()
        return _row_to_customer(row) if row else None

    async def list_all_telegram_ids(self) -> List[int]:
        query = "SELECT telegram_id FROM customer ORDER BY id ASC"
        async with self.db.execute(query) as cursor:
            rows = await cursor.fetchall()
        return [int(row["telegram_id"]) for row in rows]

    async def list_broadcast_enabled_telegram_ids(self) -> List[int]:
        query = """
            SELECT telegram_id
            FROM customer
            WHERE broadcast_enabled = 1
            ORDER BY id ASC
        """
        async with self.db.execute(query) as cursor:
            rows = await cursor.fetchall()
        return [int(row["telegram_id"]) for row in rows]

    async def list_active_telegram_ids(self, now_utc: datetime) -> List[int]:
        query = """
            SELECT telegram_id
            FROM customer
            WHERE expire_at IS NOT NULL
              AND expire_at > ?
            ORDER BY id ASC
        """
        async with self.db.execute(query, (_to_iso(now_utc),)) as cursor:
            rows = await cursor.fetchall()
        return [int(row["telegram_id"]) for row in rows]

    async def list_inactive_telegram_ids(self, now_utc: datetime) -> List[int]:
        query = """
            SELECT telegram_id
            FROM customer
            WHERE expire_at IS NULL
               OR expire_at <= ?
            ORDER BY id ASC
        """
        async with self.db.execute(query, (_to_iso(now_utc),)) as cursor:
            rows = await cursor.fetchall()
        return [int(row["telegram_id"]) for row in rows]

    async def count_all(self) -> int:
        async with self.db.execute("SELECT COUNT(*) FROM customer") as cursor:
            row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def count_active(self, now_utc: datetime) -> int:
        query = """
            SELECT COUNT(*)
            FROM customer
            WHERE expire_at IS NOT NULL
              AND expire_at > ?
        """
        async with self.db.execute(query, (_to_iso(now_utc),)) as cursor:
            row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def count_new_in_period(self, start_utc: datetime, end_utc: datetime) -> int:
        query = """
            SELECT COUNT(*)
            FROM customer
            WHERE created_at >= ?
              AND created_at < ?
        """
        async with self.db.execute(query, (_to_iso(start_utc), _to_iso(end_utc))) as cursor:
            row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def list_new_in_period(self, start_utc: datetime, end_utc: datetime, limit: int = 30) -> List[Customer]:
        query = """
            SELECT id, telegram_id, expire_at, created_at, subscription_link, language, username,
                   language_selected, notifications_enabled, broadcast_enabled
            FROM customer
            WHERE created_at >= ?
              AND created_at < ?
            ORDER BY created_at DESC
            LIMIT ?
        """
        async with self.db.execute(query, (_to_iso(start_utc), _to_iso(end_utc), limit)) as cursor:
            rows = await cursor.fetchall()
        return [_row_to_customer(row) for row in rows]

    async def find_or_create(self, telegram_id: int, language: str) -> Customer:
        query = """
            INSERT INTO customer (telegram_id, language, username, language_selected, notifications_enabled, broadcast_enabled)
            VALUES (?, ?, ?, 0, 1, 1)
            ON CONFLICT(telegram_id) DO UPDATE SET username=excluded.username
            RETURNING id, telegram_id, expire_at, created_at, subscription_link, language, username,
                      language_selected, notifications_enabled, broadcast_enabled;
        """
        async with self.db.execute(query, (telegram_id, language, None)) as cursor:
            row = await cursor.fetchone()
        await self.db.commit()
        return _row_to_customer(row)

    async def update_fields(self, customer_id: int, updates: Dict[str, Any]) -> None:
        if not updates:
            return
        fields = ", ".join(f"{key} = ?" for key in updates.keys())
        params = []
        for value in updates.values():
            if isinstance(value, datetime):
                params.append(_to_iso(value))
            else:
                params.append(value)
        params.append(customer_id)
        query = f"UPDATE customer SET {fields} WHERE id = ?"
        async with self._lock:
            await self.db.execute(query, tuple(params))
            await self.db.commit()

    async def find_by_telegram_ids(self, telegram_ids: Sequence[int]) -> List[Customer]:
        if not telegram_ids:
            return []
        placeholders = ",".join("?" for _ in telegram_ids)
        query = f"""
            SELECT id, telegram_id, expire_at, created_at, subscription_link, language, username,
                   language_selected, notifications_enabled, broadcast_enabled
            FROM customer
            WHERE telegram_id IN ({placeholders})
        """
        async with self.db.execute(query, tuple(telegram_ids)) as cursor:
            rows = await cursor.fetchall()
        return [_row_to_customer(row) for row in rows]

    async def create_batch(self, customers: Iterable[Customer]) -> None:
        data = [
            (
                c.telegram_id,
                _to_iso(c.expire_at),
                c.language,
                c.subscription_link,
                c.username,
                int(bool(c.language_selected)),
                int(bool(c.notifications_enabled)),
                int(bool(c.broadcast_enabled)),
            )
            for c in customers
        ]
        if not data:
            return
        query = """
            INSERT INTO customer (
                telegram_id, expire_at, language, subscription_link, username,
                language_selected, notifications_enabled, broadcast_enabled
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(telegram_id) DO NOTHING
        """
        async with self._lock:
            await self.db.executemany(query, data)
            await self.db.commit()

    async def update_batch(self, customers: Iterable[Customer]) -> None:
        async with self._lock:
            for c in customers:
                await self.update_fields(
                    c.id,
                    {
                        "expire_at": _to_iso(c.expire_at),
                        "subscription_link": c.subscription_link,
                        "language": c.language,
                    },
                )

    async def delete_by_not_in_telegram_ids(self, telegram_ids: Sequence[int]) -> None:
        if telegram_ids:
            placeholders = ",".join("?" for _ in telegram_ids)
            query = f"DELETE FROM customer WHERE telegram_id NOT IN ({placeholders})"
            params: Tuple[Any, ...] = tuple(telegram_ids)
        else:
            query = "DELETE FROM customer"
            params = ()
        async with self._lock:
            await self.db.execute(query, params)
            await self.db.commit()

    async def delete_by_telegram_id(self, telegram_id: int) -> bool:
        query = "DELETE FROM customer WHERE telegram_id = ?"
        async with self._lock:
            cursor = await self.db.execute(query, (telegram_id,))
            await self.db.commit()
        return bool(cursor.rowcount and cursor.rowcount > 0)


class PurchaseRepository:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self.db = db
        self._lock = asyncio.Lock()

    async def create(self, purchase: Purchase) -> int:
        query = """
            INSERT INTO purchase (
                amount, customer_id, month, currency, expire_at, status, invoice_type,
                plan, crypto_invoice_id, crypto_invoice_url, yookasa_url, yookasa_id,
                gift_sender_telegram_id, gift_recipient_telegram_id, platega_transaction_id, platega_redirect_url
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            purchase.amount,
            purchase.customer_id,
            purchase.month,
            purchase.currency,
            _to_iso(purchase.expire_at),
            purchase.status,
            purchase.invoice_type,
            purchase.plan,
            purchase.crypto_invoice_id,
            purchase.crypto_invoice_url,
            purchase.yookasa_url,
            purchase.yookasa_id,
            purchase.gift_sender_telegram_id,
            purchase.gift_recipient_telegram_id,
            purchase.platega_transaction_id,
            purchase.platega_redirect_url,
        )
        async with self._lock:
            cursor = await self.db.execute(query, params)
            await self.db.commit()
        return cursor.lastrowid

    async def find_by_invoice_type_and_status(self, invoice_type: str, status: str) -> List[Purchase]:
        query = """
            SELECT *
            FROM purchase
            WHERE invoice_type = ? AND status = ?
        """
        async with self.db.execute(query, (invoice_type, status)) as cursor:
            rows = await cursor.fetchall()
        return [_row_to_purchase(row) for row in rows]

    async def find_by_id(self, purchase_id: int) -> Optional[Purchase]:
        query = "SELECT * FROM purchase WHERE id = ?"
        async with self.db.execute(query, (purchase_id,)) as cursor:
            row = await cursor.fetchone()
        return _row_to_purchase(row) if row else None

    async def find_by_platega_transaction_id(self, transaction_id: str) -> Optional[Purchase]:
        query = """
            SELECT *
            FROM purchase
            WHERE platega_transaction_id = ?
            ORDER BY created_at DESC
            LIMIT 1
        """
        async with self.db.execute(query, (transaction_id,)) as cursor:
            row = await cursor.fetchone()
        return _row_to_purchase(row) if row else None

    async def update_fields(self, purchase_id: int, updates: Dict[str, Any]) -> None:
        if not updates:
            return
        fields = ", ".join(f"{key} = ?" for key in updates.keys())
        params: List[Any] = []
        for value in updates.values():
            if isinstance(value, datetime):
                params.append(_to_iso(value))
            else:
                params.append(value)
        params.append(purchase_id)
        query = f"UPDATE purchase SET {fields} WHERE id = ?"
        async with self._lock:
            await self.db.execute(query, tuple(params))
            await self.db.commit()

    async def mark_as_paid(self, purchase_id: int) -> None:
        await self.update_fields(
            purchase_id,
            {"status": "paid", "paid_at": _to_iso(datetime.utcnow())},
        )

    async def find_latest_active_tributes_by_customer_ids(self, customer_ids: Sequence[int]) -> List[Purchase]:
        if not customer_ids:
            return []
        placeholders = ",".join("?" for _ in customer_ids)
        query = f"""
            SELECT p.*
            FROM purchase p
            JOIN (
                SELECT customer_id, MAX(created_at) AS max_created
                FROM purchase
                WHERE invoice_type = 'tribute'
                GROUP BY customer_id
            ) latest ON latest.customer_id = p.customer_id AND latest.max_created = p.created_at
            WHERE p.invoice_type = 'tribute'
              AND p.status != 'cancel'
              AND p.customer_id IN ({placeholders})
        """
        async with self.db.execute(query, tuple(customer_ids)) as cursor:
            rows = await cursor.fetchall()
        return [_row_to_purchase(row) for row in rows]

    async def find_by_customer_id_and_invoice_type_last(self, customer_id: int, invoice_type: str) -> Optional[Purchase]:
        query = """
            SELECT *
            FROM purchase
            WHERE customer_id = ? AND invoice_type = ?
            ORDER BY created_at DESC
            LIMIT 1
        """
        async with self.db.execute(query, (customer_id, invoice_type)) as cursor:
            row = await cursor.fetchone()
        return _row_to_purchase(row) if row else None

    async def find_successful_paid_purchase_by_customer(self, customer_id: int) -> Optional[Purchase]:
        query = """
            SELECT *
            FROM purchase
            WHERE customer_id = ?
              AND status = 'paid'
              AND invoice_type IN ('crypto', 'yookasa', 'platega')
            ORDER BY paid_at DESC
            LIMIT 1
        """
        async with self.db.execute(query, (customer_id,)) as cursor:
            row = await cursor.fetchone()
        return _row_to_purchase(row) if row else None

    async def count_paid_by_customer(self, customer_id: int) -> int:
        query = """
            SELECT COUNT(*)
            FROM purchase
            WHERE customer_id = ?
              AND status = 'paid'
              AND paid_at IS NOT NULL
        """
        async with self.db.execute(query, (customer_id,)) as cursor:
            row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def count_paid_in_period(self, start_utc: datetime, end_utc: datetime) -> int:
        query = """
            SELECT COUNT(*)
            FROM purchase
            WHERE status = 'paid'
              AND paid_at IS NOT NULL
              AND paid_at >= ?
              AND paid_at < ?
        """
        async with self.db.execute(query, (_to_iso(start_utc), _to_iso(end_utc))) as cursor:
            row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def revenue_paid_in_period(self, start_utc: datetime, end_utc: datetime) -> float:
        query = """
            SELECT COALESCE(SUM(amount), 0)
            FROM purchase
            WHERE status = 'paid'
              AND paid_at IS NOT NULL
              AND paid_at >= ?
              AND paid_at < ?
        """
        async with self.db.execute(query, (_to_iso(start_utc), _to_iso(end_utc))) as cursor:
            row = await cursor.fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0

    async def count_new_paid_customers_in_period(self, start_utc: datetime, end_utc: datetime) -> int:
        query = """
            WITH first_paid AS (
                SELECT customer_id, MIN(paid_at) AS first_paid_at
                FROM purchase
                WHERE status = 'paid' AND paid_at IS NOT NULL
                GROUP BY customer_id
            )
            SELECT COUNT(*)
            FROM first_paid
            WHERE first_paid_at >= ? AND first_paid_at < ?
        """
        async with self.db.execute(query, (_to_iso(start_utc), _to_iso(end_utc))) as cursor:
            row = await cursor.fetchone()
        return int(row[0]) if row else 0


class ReferralRepository:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self.db = db
        self._lock = asyncio.Lock()

    async def create(self, referrer_id: int, referee_id: int) -> Referral:
        existing_referee = await self.find_by_referee(referee_id)
        if existing_referee:
            return existing_referee
        query = """
            INSERT INTO referral (referrer_id, referee_id, used_at, bonus_granted)
            VALUES (?, ?, CURRENT_TIMESTAMP, 0)
        """
        async with self._lock:
            try:
                cursor = await self.db.execute(query, (referrer_id, referee_id))
                await self.db.commit()
                new_id = cursor.lastrowid
            except sqlite3.IntegrityError:
                return await self.find_by_pair(referrer_id, referee_id)
        return Referral(
            id=new_id,
            referrer_id=referrer_id,
            referee_id=referee_id,
            used_at=datetime.utcnow(),
            bonus_granted=False,
        )

    async def find_by_pair(self, referrer_id: int, referee_id: int) -> Optional[Referral]:
        query = """
            SELECT id, referrer_id, referee_id, used_at, bonus_granted
            FROM referral
            WHERE referrer_id = ? AND referee_id = ?
            LIMIT 1
        """
        async with self.db.execute(query, (referrer_id, referee_id)) as cursor:
            row = await cursor.fetchone()
        return _row_to_referral(row) if row else None

    async def find_by_referrer(self, referrer_id: int) -> List[Referral]:
        query = """
            SELECT id, referrer_id, referee_id, used_at, bonus_granted
            FROM referral
            WHERE referrer_id = ?
            ORDER BY used_at DESC
        """
        async with self.db.execute(query, (referrer_id,)) as cursor:
            rows = await cursor.fetchall()
        return [_row_to_referral(row) for row in rows]

    async def count_by_referrer(self, referrer_id: int) -> int:
        query = "SELECT COUNT(*) FROM referral WHERE referrer_id = ?"
        async with self.db.execute(query, (referrer_id,)) as cursor:
            row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def find_details_by_referrer(self, referrer_id: int) -> List[ReferralDetails]:
        query = """
            SELECT r.id, r.referrer_id, r.referee_id, r.used_at, r.bonus_granted, c.username AS referee_username
            FROM referral r
            LEFT JOIN customer c ON c.telegram_id = r.referee_id
            WHERE r.referrer_id = ?
            ORDER BY r.used_at DESC
        """
        async with self.db.execute(query, (referrer_id,)) as cursor:
            rows = await cursor.fetchall()
        details: List[ReferralDetails] = []
        for row in rows:
            ref = _row_to_referral(row)
            username = row["referee_username"] if "referee_username" in row.keys() else None
            details.append(ReferralDetails(referral=ref, referee_username=username))
        return details

    async def find_by_referee(self, referee_id: int) -> Optional[Referral]:
        query = """
            SELECT id, referrer_id, referee_id, used_at, bonus_granted
            FROM referral
            WHERE referee_id = ?
            LIMIT 1
        """
        async with self.db.execute(query, (referee_id,)) as cursor:
            row = await cursor.fetchone()
        return _row_to_referral(row) if row else None

    async def mark_bonus_granted(self, referral_id: int) -> None:
        query = "UPDATE referral SET bonus_granted = 1 WHERE id = ?"
        async with self._lock:
            await self.db.execute(query, (referral_id,))
            await self.db.commit()


class PromoRepository:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self.db = db
        self._lock = asyncio.Lock()

    async def create(self, code: str, days: int, traffic_gb: int, max_uses: int, created_by: Optional[int]) -> PromoCode:
        query = """
            INSERT INTO promo_code (code, days, traffic_gb, max_uses, used, created_by)
            VALUES (?, ?, ?, ?, 0, ?)
        """
        async with self._lock:
            cursor = await self.db.execute(query, (code, days, traffic_gb, max_uses, created_by))
            await self.db.commit()
            promo_id = cursor.lastrowid
        return PromoCode(
            id=promo_id,
            code=code,
            days=days,
            traffic_gb=traffic_gb,
            max_uses=max_uses,
            used=0,
            created_at=datetime.utcnow(),
            created_by=created_by,
        )

    async def list_all(self, limit: int = 30) -> List[PromoCode]:
        query = """
            SELECT id, code, days, traffic_gb, max_uses, used, created_at, created_by
            FROM promo_code
            ORDER BY created_at DESC
            LIMIT ?
        """
        async with self.db.execute(query, (limit,)) as cursor:
            rows = await cursor.fetchall()
        return [_row_to_promocode(row) for row in rows]

    async def find_by_code(self, code: str) -> Optional[PromoCode]:
        query = """
            SELECT id, code, days, traffic_gb, max_uses, used, created_at, created_by
            FROM promo_code
            WHERE code = ?
        """
        async with self.db.execute(query, (code,)) as cursor:
            row = await cursor.fetchone()
        return _row_to_promocode(row) if row else None

    async def _find_redemption(self, promo_id: int, customer_id: int) -> Optional[PromoRedemption]:
        query = """
            SELECT id, promo_id, customer_id, used_at
            FROM promo_redemption
            WHERE promo_id = ? AND customer_id = ?
            LIMIT 1
        """
        async with self.db.execute(query, (promo_id, customer_id)) as cursor:
            row = await cursor.fetchone()
        return _row_to_promo_redemption(row) if row else None

    async def redeem(self, promo: PromoCode, customer_id: int) -> str:
        """
        Try to redeem promo for customer.
        Returns status: 'ok', 'exhausted', 'already_used'
        """
        async with self._lock:
            existing = await self._find_redemption(promo.id, customer_id)
            if existing:
                return "already_used"
            async with self.db.execute(
                "SELECT used, max_uses FROM promo_code WHERE id = ?", (promo.id,)
            ) as cursor:
                row = await cursor.fetchone()
            if not row:
                return "exhausted"
            used, max_uses = row
            if used >= max_uses:
                return "exhausted"
            await self.db.execute("UPDATE promo_code SET used = used + 1 WHERE id = ?", (promo.id,))
            await self.db.execute(
                "INSERT INTO promo_redemption (promo_id, customer_id) VALUES (?, ?)",
                (promo.id, customer_id),
            )
            await self.db.commit()
            return "ok"


class SalesRepository:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self.db = db
        self._lock = asyncio.Lock()

    async def record_sale(self, purchase: Purchase, customer: Customer, is_new_customer: bool) -> None:
        paid_at = purchase.paid_at or datetime.utcnow()
        currency = (purchase.currency or "").upper()
        amount_rub = float(purchase.amount)
        if currency in {"STARS", "XTR"}:
            amount_rub = float(purchase.amount)
        query = """
            INSERT INTO sales_log (
                purchase_id, customer_id, telegram_id, amount, currency, amount_rub,
                invoice_type, plan, is_new_customer, paid_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(purchase_id) DO UPDATE SET
                customer_id = excluded.customer_id,
                telegram_id = excluded.telegram_id,
                amount = excluded.amount,
                currency = excluded.currency,
                amount_rub = excluded.amount_rub,
                invoice_type = excluded.invoice_type,
                plan = excluded.plan,
                is_new_customer = excluded.is_new_customer,
                paid_at = excluded.paid_at
        """
        params = (
            purchase.id,
            customer.id,
            customer.telegram_id,
            float(purchase.amount),
            purchase.currency,
            amount_rub,
            purchase.invoice_type,
            purchase.plan,
            1 if is_new_customer else 0,
            _to_iso(paid_at),
        )
        async with self._lock:
            await self.db.execute(query, params)
            await self.db.commit()

    async def count_paid_in_period(self, start_utc: datetime, end_utc: datetime) -> int:
        query = """
            SELECT COUNT(*)
            FROM sales_log
            WHERE paid_at >= ?
              AND paid_at < ?
        """
        async with self.db.execute(query, (_to_iso(start_utc), _to_iso(end_utc))) as cursor:
            row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def count_new_paid_customers_in_period(self, start_utc: datetime, end_utc: datetime) -> int:
        query = """
            SELECT COUNT(*)
            FROM sales_log
            WHERE paid_at >= ?
              AND paid_at < ?
              AND is_new_customer = 1
        """
        async with self.db.execute(query, (_to_iso(start_utc), _to_iso(end_utc))) as cursor:
            row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def finance_totals_in_period(self, start_utc: datetime, end_utc: datetime) -> Dict[str, float]:
        query = """
            SELECT
                COUNT(*) AS sales_count,
                COALESCE(SUM(amount_rub), 0) AS revenue_rub,
                COALESCE(SUM(CASE WHEN UPPER(COALESCE(currency, '')) IN ('STARS', 'XTR') THEN amount ELSE 0 END), 0) AS stars_amount,
                COALESCE(SUM(CASE WHEN UPPER(COALESCE(currency, '')) IN ('STARS', 'XTR') THEN amount_rub ELSE 0 END), 0) AS stars_revenue_rub,
                COALESCE(SUM(CASE WHEN UPPER(COALESCE(currency, '')) NOT IN ('STARS', 'XTR') THEN amount_rub ELSE 0 END), 0) AS fiat_revenue_rub
            FROM sales_log
            WHERE paid_at >= ?
              AND paid_at < ?
        """
        async with self.db.execute(query, (_to_iso(start_utc), _to_iso(end_utc))) as cursor:
            row = await cursor.fetchone()
        if not row:
            return {
                "sales_count": 0.0,
                "revenue_rub": 0.0,
                "stars_amount": 0.0,
                "stars_revenue_rub": 0.0,
                "fiat_revenue_rub": 0.0,
            }
        return {
            "sales_count": float(row["sales_count"] or 0),
            "revenue_rub": float(row["revenue_rub"] or 0),
            "stars_amount": float(row["stars_amount"] or 0),
            "stars_revenue_rub": float(row["stars_revenue_rub"] or 0),
            "fiat_revenue_rub": float(row["fiat_revenue_rub"] or 0),
        }

    async def list_recent(self, limit: int = 30) -> List[SaleLog]:
        query = """
            SELECT *
            FROM sales_log
            ORDER BY paid_at DESC
            LIMIT ?
        """
        async with self.db.execute(query, (limit,)) as cursor:
            rows = await cursor.fetchall()
        return [_row_to_sale_log(row) for row in rows]


class PriceSettingRepository:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self.db = db
        self._lock = asyncio.Lock()

    async def ensure_defaults(self, defaults: Dict[str, int]) -> None:
        if not defaults:
            return
        async with self._lock:
            for key, value in defaults.items():
                await self.db.execute(
                    """
                    INSERT OR IGNORE INTO price_setting (key, value, updated_at, updated_by)
                    VALUES (?, ?, CURRENT_TIMESTAMP, NULL)
                    """,
                    (key, int(value)),
                )
            await self.db.commit()

    async def list_all(self) -> List[PriceSetting]:
        query = """
            SELECT key, value, updated_at, updated_by
            FROM price_setting
            ORDER BY key ASC
        """
        async with self.db.execute(query) as cursor:
            rows = await cursor.fetchall()
        return [_row_to_price_setting(row) for row in rows]

    async def get_all_map(self) -> Dict[str, int]:
        settings = await self.list_all()
        return {item.key: item.value for item in settings}

    async def set_value(self, key: str, value: int, updated_by: Optional[int]) -> None:
        query = """
            INSERT INTO price_setting (key, value, updated_at, updated_by)
            VALUES (?, ?, CURRENT_TIMESTAMP, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = CURRENT_TIMESTAMP,
                updated_by = excluded.updated_by
        """
        async with self._lock:
            await self.db.execute(query, (key, int(value), updated_by))
            await self.db.commit()

    async def get_value(self, key: str) -> Optional[int]:
        query = """
            SELECT key, value, updated_at, updated_by
            FROM price_setting
            WHERE key = ?
            LIMIT 1
        """
        async with self.db.execute(query, (key,)) as cursor:
            row = await cursor.fetchone()
        if not row:
            return None
        return int(row["value"])


class GiftNotificationRepository:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self.db = db
        self._lock = asyncio.Lock()

    async def create(
        self,
        recipient_telegram_id: int,
        sender_telegram_id: Optional[int],
        months: int,
        days: int,
        message: str,
        purchase_id: Optional[int] = None,
    ) -> int:
        query = """
            INSERT INTO gift_notification (
                recipient_telegram_id, sender_telegram_id, months, days, message, purchase_id, delivered
            )
            VALUES (?, ?, ?, ?, ?, ?, 0)
        """
        params = (
            int(recipient_telegram_id),
            sender_telegram_id,
            int(months),
            int(days),
            message,
            purchase_id,
        )
        async with self._lock:
            cursor = await self.db.execute(query, params)
            await self.db.commit()
        return int(cursor.lastrowid)

    async def list_pending_by_recipient(self, recipient_telegram_id: int, limit: int = 10) -> List[GiftNotification]:
        query = """
            SELECT *
            FROM gift_notification
            WHERE recipient_telegram_id = ?
              AND delivered = 0
            ORDER BY created_at ASC
            LIMIT ?
        """
        async with self.db.execute(query, (int(recipient_telegram_id), limit)) as cursor:
            rows = await cursor.fetchall()
        return [_row_to_gift_notification(row) for row in rows]

    async def mark_delivered(self, notification_ids: Sequence[int]) -> None:
        if not notification_ids:
            return
        placeholders = ",".join("?" for _ in notification_ids)
        query = f"""
            UPDATE gift_notification
            SET delivered = 1,
                delivered_at = CURRENT_TIMESTAMP
            WHERE id IN ({placeholders})
        """
        async with self._lock:
            await self.db.execute(query, tuple(int(x) for x in notification_ids))
            await self.db.commit()


class DuoPurchaseMemberRepository:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self.db = db
        self._lock = asyncio.Lock()

    async def replace_members(self, purchase_id: int, member_telegram_ids: Sequence[int]) -> None:
        unique_ids: List[int] = []
        for raw in member_telegram_ids:
            try:
                value = int(raw)
            except Exception:
                continue
            if value <= 0 or value in unique_ids:
                continue
            unique_ids.append(value)

        async with self._lock:
            await self.db.execute("DELETE FROM duo_purchase_member WHERE purchase_id = ?", (int(purchase_id),))
            if unique_ids:
                await self.db.executemany(
                    """
                    INSERT OR IGNORE INTO duo_purchase_member (purchase_id, member_telegram_id)
                    VALUES (?, ?)
                    """,
                    [(int(purchase_id), member_id) for member_id in unique_ids],
                )
            await self.db.commit()

    async def list_member_ids(self, purchase_id: int) -> List[int]:
        query = """
            SELECT member_telegram_id
            FROM duo_purchase_member
            WHERE purchase_id = ?
            ORDER BY id ASC
        """
        async with self.db.execute(query, (int(purchase_id),)) as cursor:
            rows = await cursor.fetchall()
        return [int(row["member_telegram_id"]) for row in rows]
