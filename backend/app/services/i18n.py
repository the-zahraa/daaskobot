import json
import os
from typing import Any, Dict, Optional, Callable

# -------- cache & resolver (fast + sync) -------------------------------------
_lang_cache: Dict[int, str] = {}          # user_id -> "en"/"fr"
_lang_resolver: Optional[Callable[[int], Optional[str]]] = None  # optional custom resolver

def remember_language(user_id: int, lang: Optional[str]) -> None:
    """Remember language in-memory for instant lookups."""
    if not user_id or not lang:
        return
    _lang_cache[user_id] = lang

def forget_language(user_id: int) -> None:
    _lang_cache.pop(user_id, None)

def set_language_resolver(resolver: Callable[[int], Optional[str]]) -> None:
    """Set a synchronous resolver (e.g., a cache fetch)."""
    global _lang_resolver
    _lang_resolver = resolver

# -------- i18n core ----------------------------------------------------------
class I18n:
    def __init__(self, locales_dir: str, default_lang: str = "en", repositories=None):
        self.locales_dir = locales_dir
        self.default_lang = default_lang
        self.repositories = repositories   # optional: expects .users.get_language(user_id) (sync) if provided
        self._catalogs: Dict[str, Dict[str, Any]] = {}
        self.load()

    def load(self) -> None:
        """(Re)load all locale files."""
        self._catalogs.clear()
        if not os.path.isdir(self.locales_dir):
            return
        for fname in os.listdir(self.locales_dir):
            if not fname.endswith(".json"):
                continue
            lang = fname.split(".")[0]
            path = os.path.join(self.locales_dir, fname)
            with open(path, "r", encoding="utf-8") as f:
                self._catalogs[lang] = json.load(f)

    def _lookup(self, lang: str, key: str) -> Optional[str]:
        node: Any = self._catalogs.get(lang, {})
        for part in key.split("."):
            if isinstance(node, dict):
                node = node.get(part)
            else:
                return None
        return node if isinstance(node, str) else None

    def translate(self, key: str, lang: Optional[str] = None, **kwargs) -> str:
        lang = lang or self.default_lang
        text = self._lookup(lang, key)
        if text is None and lang != self.default_lang:
            text = self._lookup(self.default_lang, key)
        if text is None:
            # fallback to key so missing strings are obvious in dev
            text = key
        try:
            return text.format(**kwargs)
        except Exception:
            return text

    def t(self, key: str, user_id: Optional[int] = None, lang: Optional[str] = None, **kwargs) -> str:
        # Resolution order:
        # 1) explicit lang param
        # 2) in-memory cache
        # 3) optional sync resolver set via set_language_resolver()
        # 4) optional repositories.users.get_language(user_id) IF it is sync
        resolved: Optional[str] = lang
        if resolved is None and user_id is not None:
            # cache
            resolved = _lang_cache.get(user_id)
            # external resolver
            if resolved is None and _lang_resolver is not None:
                try:
                    resolved = _lang_resolver(user_id)
                except Exception:
                    resolved = None
            # repositories (only if sync function is provided)
            if resolved is None and self.repositories:
                try:
                    maybe = self.repositories.users.get_language(user_id)
                    # ignore coroutine objects (async funcs) to avoid blocking loop
                    if not hasattr(maybe, "__await__"):
                        resolved = maybe
                except Exception:
                    resolved = None
        return self.translate(key, lang=resolved, **kwargs)

# ---- module-level helpers for easy import ----
_i18n: Optional[I18n] = None

def init_i18n(repositories=None, default_lang: str = "en") -> I18n:
    global _i18n
    locales_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "locales")
    _i18n = I18n(locales_dir=locales_dir, default_lang=default_lang, repositories=repositories)
    return _i18n

def t(key: str, user_id: Optional[int] = None, lang: Optional[str] = None, **kwargs) -> str:
    global _i18n
    if _i18n is None:
        init_i18n()
    return _i18n.t(key, user_id=user_id, lang=lang, **kwargs)
