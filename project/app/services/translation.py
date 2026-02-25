import json
from pathlib import Path
from typing import Dict


Translation = Dict[str, str]


class TranslationManager:
    def __init__(self, default_language: str = "en") -> None:
        self.translations: Dict[str, Translation] = {}
        self.default_language = default_language

    def load(self, translations_dir: Path) -> None:
        if not translations_dir.exists():
            raise FileNotFoundError(f"Translations dir not found: {translations_dir}")

        for file in translations_dir.glob("*.json"):
            lang_code = file.stem
            content = json.loads(file.read_text(encoding="utf-8"))
            self.translations[lang_code] = content

        if self.default_language not in self.translations:
            raise RuntimeError(f"Default language {self.default_language} not found in translations")

    def get_text(self, lang_code: str, key: str) -> str:
        if lang_code in self.translations and key in self.translations[lang_code]:
            value = self.translations[lang_code][key]
            if value:
                return value

        default = self.translations.get(self.default_language, {})
        return default.get(key, key)

