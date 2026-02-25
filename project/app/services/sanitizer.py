import re
import unicodedata
from typing import Optional

URL_PATTERNS = [
    re.compile(r"(?i)https?://\S+"),
    re.compile(r"(?i)www\.\S+"),
    re.compile(r"(?i)tg://\S+"),
    re.compile(r"(?i)telegram\.me\S*"),
    re.compile(r"(?i)t\.me/\+\S*"),
    re.compile(r"(?i)joinchat\S*"),
]

OBFUSCATED_DOMAIN_PATTERNS = [
    re.compile(r"(?i)t[\s\.\-/\\]*m[\s\.\-/\\]*e"),
    re.compile(r"(?i)telegram[\s\.\-/\\]*support"),
]

BANNED_TOKENS = {
    "telegram",
    "t.me",
    "telegramme",
    "telegrarn",
    "notification",
    "moderation",
    "review",
    "compliance",
    "abuse",
    "spam",
    "report",
}

DANGEROUS_COMBINATIONS = [
    ("telegram", "support"),
    ("telegram", "admin"),
    ("service", "support"),
    ("system", "admin"),
    ("security", "admin"),
]


def _normalize(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"\s+", " ", normalized).lower()
    normalized = re.sub(r"[^a-z0-9]+", "", normalized)
    return normalized


def _contains_dangerous_combo(normalized: str) -> bool:
    return any(combo[0] in normalized and combo[1] in normalized for combo in DANGEROUS_COMBINATIONS)


def _strip_patterns(value: str) -> str:
    updated = value
    for pattern in URL_PATTERNS + OBFUSCATED_DOMAIN_PATTERNS:
        updated = pattern.sub(" ", updated)
    return updated


def _finalize(clean: str, original: str) -> Optional[str]:
    compacted = re.sub(r"\s+", " ", clean).strip(" \t\r\n-_.,/\\")
    if not compacted:
        return None
    normalized_original = _normalize(original)
    if any(token in normalized_original for token in BANNED_TOKENS):
        return None
    if _contains_dangerous_combo(normalized_original):
        return None
    normalized = _normalize(compacted)
    if any(token in normalized for token in BANNED_TOKENS):
        return None
    if _contains_dangerous_combo(normalized):
        return None
    return compacted


def sanitize_display_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    clean = value.replace("@", " ")
    clean = _strip_patterns(clean)
    return _finalize(clean, value)


def sanitize_username(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    clean = value.strip().lstrip("@")
    clean = _strip_patterns(clean)
    return _finalize(clean, value)


def username_for_display(username: Optional[str], with_at: bool = True) -> str:
    sanitized = sanitize_username(username)
    if sanitized:
        return f"@{sanitized}" if with_at else sanitized
    return "username"


def display_name_or_fallback(first_name: Optional[str], fallback: str = "") -> str:
    sanitized = sanitize_display_name(first_name)
    if sanitized:
        return sanitized
    return fallback or "username"


def _contains_alphanumeric(text: str) -> bool:
    return any(ch.isalnum() for ch in text)


def is_suspicious_user(username: Optional[str], first_name: Optional[str], last_name: Optional[str]) -> bool:
    if username and _contains_alphanumeric(username) and sanitize_username(username) is None:
        return True
    if first_name and _contains_alphanumeric(first_name) and sanitize_display_name(first_name) is None:
        return True
    if last_name and _contains_alphanumeric(last_name) and sanitize_display_name(last_name) is None:
        return True
    return False
