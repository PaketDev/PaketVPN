import aiosqlite


CREATE_SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS customer (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id       INTEGER UNIQUE,
    expire_at         TEXT,
    created_at        TEXT DEFAULT CURRENT_TIMESTAMP,
    subscription_link TEXT,
    language          TEXT DEFAULT 'en',
    username          TEXT,
    language_selected INTEGER NOT NULL DEFAULT 0,
    notifications_enabled INTEGER NOT NULL DEFAULT 1,
    broadcast_enabled INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_customer_telegram_id ON customer(telegram_id);

CREATE TABLE IF NOT EXISTS purchase (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    amount             REAL NOT NULL,
    customer_id        INTEGER REFERENCES customer(id) ON DELETE CASCADE,
    created_at         TEXT DEFAULT CURRENT_TIMESTAMP,
    month              INTEGER NOT NULL,
    paid_at            TEXT,
    currency           TEXT,
    expire_at          TEXT,
    status             TEXT,
    invoice_type       TEXT,
    plan               TEXT,
    crypto_invoice_id  INTEGER,
    crypto_invoice_url TEXT,
    yookasa_url        TEXT,
    yookasa_id         TEXT,
    gift_sender_telegram_id INTEGER,
    gift_recipient_telegram_id INTEGER,
    platega_transaction_id TEXT,
    platega_redirect_url   TEXT
);

CREATE TABLE IF NOT EXISTS sales_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    purchase_id      INTEGER UNIQUE NOT NULL REFERENCES purchase(id) ON DELETE CASCADE,
    customer_id      INTEGER NOT NULL REFERENCES customer(id) ON DELETE CASCADE,
    telegram_id      INTEGER NOT NULL,
    amount           REAL NOT NULL,
    currency         TEXT,
    amount_rub       REAL NOT NULL,
    invoice_type     TEXT,
    plan             TEXT,
    is_new_customer  INTEGER NOT NULL DEFAULT 0,
    paid_at          TEXT NOT NULL,
    created_at       TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sales_log_paid_at ON sales_log(paid_at);
CREATE INDEX IF NOT EXISTS idx_sales_log_telegram_id ON sales_log(telegram_id);

CREATE TABLE IF NOT EXISTS price_setting (
    key         TEXT PRIMARY KEY,
    value       INTEGER NOT NULL,
    updated_at  TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_by  INTEGER
);

CREATE TABLE IF NOT EXISTS gift_notification (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    recipient_telegram_id INTEGER NOT NULL,
    sender_telegram_id    INTEGER,
    months                INTEGER NOT NULL DEFAULT 0,
    days                  INTEGER NOT NULL DEFAULT 0,
    message               TEXT NOT NULL,
    purchase_id           INTEGER REFERENCES purchase(id) ON DELETE SET NULL,
    delivered             INTEGER NOT NULL DEFAULT 0,
    created_at            TEXT DEFAULT CURRENT_TIMESTAMP,
    delivered_at          TEXT
);

CREATE INDEX IF NOT EXISTS idx_gift_notification_recipient_delivered
    ON gift_notification(recipient_telegram_id, delivered);

CREATE TABLE IF NOT EXISTS duo_purchase_member (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    purchase_id         INTEGER NOT NULL REFERENCES purchase(id) ON DELETE CASCADE,
    member_telegram_id  INTEGER NOT NULL,
    created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(purchase_id, member_telegram_id)
);

CREATE INDEX IF NOT EXISTS idx_duo_purchase_member_purchase
    ON duo_purchase_member(purchase_id);

CREATE TABLE IF NOT EXISTS referral (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    referrer_id    INTEGER NOT NULL REFERENCES customer(telegram_id) ON DELETE CASCADE,
    referee_id     INTEGER NOT NULL REFERENCES customer(telegram_id) ON DELETE CASCADE,
    used_at        TEXT DEFAULT CURRENT_TIMESTAMP,
    bonus_granted  INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_referral_referrer_id ON referral(referrer_id);
CREATE INDEX IF NOT EXISTS idx_referral_referee_id ON referral(referee_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_referral_pair ON referral(referrer_id, referee_id);

CREATE TABLE IF NOT EXISTS promo_code (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    code        TEXT UNIQUE NOT NULL,
    days        INTEGER NOT NULL DEFAULT 0,
    traffic_gb  INTEGER NOT NULL DEFAULT 0,
    max_uses    INTEGER NOT NULL DEFAULT 1,
    used        INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
    created_by  INTEGER
);

CREATE TABLE IF NOT EXISTS promo_redemption (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    promo_id    INTEGER NOT NULL REFERENCES promo_code(id) ON DELETE CASCADE,
    customer_id INTEGER NOT NULL REFERENCES customer(id) ON DELETE CASCADE,
    used_at     TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(promo_id, customer_id)
);
"""


async def run_migrations(db: aiosqlite.Connection) -> None:
    """Apply the minimal schema required for the bot."""
    async def _has_column(table: str, column: str) -> bool:
        async with db.execute(f"PRAGMA table_info({table})") as cursor:
            rows = await cursor.fetchall()
        return any(str(row[1]) == column for row in rows)

    async def _has_columns(table: str, columns: tuple[str, ...]) -> bool:
        for column in columns:
            if not await _has_column(table, column):
                return False
        return True

    await db.executescript(CREATE_SCHEMA)
    try:
        await db.execute("ALTER TABLE purchase ADD COLUMN plan TEXT")
    except Exception:
        pass
    try:
        await db.execute("ALTER TABLE purchase ADD COLUMN platega_transaction_id TEXT")
    except Exception:
        pass
    try:
        await db.execute("ALTER TABLE purchase ADD COLUMN platega_redirect_url TEXT")
    except Exception:
        pass
    try:
        if await _has_columns("purchase", ("invoice_type", "status")):
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_purchase_invoice_type_status ON purchase(invoice_type, status)"
            )
    except Exception:
        pass
    try:
        if await _has_column("purchase", "platega_transaction_id"):
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_purchase_platega_transaction ON purchase(platega_transaction_id)"
            )
    except Exception:
        pass
    try:
        await db.execute("ALTER TABLE customer ADD COLUMN username TEXT")
    except Exception:
        pass
    try:
        await db.execute("ALTER TABLE customer ADD COLUMN language_selected INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass
    try:
        await db.execute("ALTER TABLE customer ADD COLUMN notifications_enabled INTEGER NOT NULL DEFAULT 1")
    except Exception:
        pass
    try:
        await db.execute("ALTER TABLE customer ADD COLUMN broadcast_enabled INTEGER NOT NULL DEFAULT 1")
    except Exception:
        pass
    try:
        await db.execute("ALTER TABLE purchase ADD COLUMN gift_sender_telegram_id INTEGER")
    except Exception:
        pass
    try:
        await db.execute("ALTER TABLE purchase ADD COLUMN gift_recipient_telegram_id INTEGER")
    except Exception:
        pass
    try:
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_referral_pair ON referral(referrer_id, referee_id)")
    except Exception:
        pass
    # Backward-compat for old promo_code schema versions.
    try:
        if not await _has_column("promo_code", "traffic_gb"):
            await db.execute("ALTER TABLE promo_code ADD COLUMN traffic_gb INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass
    try:
        if not await _has_column("promo_code", "created_by"):
            await db.execute("ALTER TABLE promo_code ADD COLUMN created_by INTEGER")
    except Exception:
        pass
    try:
        if not await _has_column("promo_code", "created_at"):
            await db.execute("ALTER TABLE promo_code ADD COLUMN created_at TEXT DEFAULT CURRENT_TIMESTAMP")
    except Exception:
        pass
    try:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS promo_code (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                code        TEXT UNIQUE NOT NULL,
                days        INTEGER NOT NULL DEFAULT 0,
                traffic_gb  INTEGER NOT NULL DEFAULT 0,
                max_uses    INTEGER NOT NULL DEFAULT 1,
                used        INTEGER NOT NULL DEFAULT 0,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                created_by  INTEGER
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS promo_redemption (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                promo_id    INTEGER NOT NULL REFERENCES promo_code(id) ON DELETE CASCADE,
                customer_id INTEGER NOT NULL REFERENCES customer(id) ON DELETE CASCADE,
                used_at     TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(promo_id, customer_id)
            )
            """
        )
    except Exception:
        pass
    try:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS sales_log (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                purchase_id      INTEGER UNIQUE NOT NULL REFERENCES purchase(id) ON DELETE CASCADE,
                customer_id      INTEGER NOT NULL REFERENCES customer(id) ON DELETE CASCADE,
                telegram_id      INTEGER NOT NULL,
                amount           REAL NOT NULL,
                currency         TEXT,
                amount_rub       REAL NOT NULL,
                invoice_type     TEXT,
                plan             TEXT,
                is_new_customer  INTEGER NOT NULL DEFAULT 0,
                paid_at          TEXT NOT NULL,
                created_at       TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_sales_log_paid_at ON sales_log(paid_at)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_sales_log_telegram_id ON sales_log(telegram_id)")
    except Exception:
        pass
    try:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS price_setting (
                key         TEXT PRIMARY KEY,
                value       INTEGER NOT NULL,
                updated_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_by  INTEGER
            )
            """
        )
    except Exception:
        pass
    try:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS gift_notification (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                recipient_telegram_id INTEGER NOT NULL,
                sender_telegram_id    INTEGER,
                months                INTEGER NOT NULL DEFAULT 0,
                days                  INTEGER NOT NULL DEFAULT 0,
                message               TEXT NOT NULL,
                purchase_id           INTEGER REFERENCES purchase(id) ON DELETE SET NULL,
                delivered             INTEGER NOT NULL DEFAULT 0,
                created_at            TEXT DEFAULT CURRENT_TIMESTAMP,
                delivered_at          TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_gift_notification_recipient_delivered
            ON gift_notification(recipient_telegram_id, delivered)
            """
        )
    except Exception:
        pass
    try:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS duo_purchase_member (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                purchase_id         INTEGER NOT NULL REFERENCES purchase(id) ON DELETE CASCADE,
                member_telegram_id  INTEGER NOT NULL,
                created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(purchase_id, member_telegram_id)
            )
            """
        )
        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_duo_purchase_member_purchase
            ON duo_purchase_member(purchase_id)
            """
        )
    except Exception:
        pass
    try:
        await db.execute(
            """
            INSERT OR IGNORE INTO sales_log (
                purchase_id, customer_id, telegram_id, amount, currency, amount_rub,
                invoice_type, plan, is_new_customer, paid_at
            )
            SELECT
                p.id,
                p.customer_id,
                c.telegram_id,
                p.amount,
                p.currency,
                CASE
                    WHEN UPPER(COALESCE(p.currency, '')) IN ('STARS', 'XTR') THEN p.amount
                    ELSE p.amount
                END AS amount_rub,
                p.invoice_type,
                p.plan,
                CASE
                    WHEN p.paid_at IS NOT NULL
                         AND p.paid_at = (
                            SELECT MIN(p2.paid_at)
                            FROM purchase p2
                            WHERE p2.customer_id = p.customer_id
                              AND p2.status = 'paid'
                              AND p2.paid_at IS NOT NULL
                         )
                    THEN 1
                    ELSE 0
                END AS is_new_customer,
                COALESCE(p.paid_at, p.created_at)
            FROM purchase p
            JOIN customer c ON c.id = p.customer_id
            WHERE p.status = 'paid'
            """
        )
    except Exception:
        pass
    await db.commit()
