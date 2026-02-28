"""
ai_router.py  ·  LiteLLM Multi-Provider Router  ·  v2.4
=========================================================
FIXES in v2.4 (on top of v2.3):
  ─────────────────────────────────────────────────────
  PROBLEM 1 — Cooldown persists even after timer expires:
    _status_registry lives in module memory. Cooldown of 60s is set
    but the STATUS PANEL reads it correctly — however if Gemini keeps
    hitting rate limits on every test run, it stays in cooldown.
    FIX: Graduated backoff — 30s / 60s / 90s (not flat 60s).
         Added reset_cooldowns() for a manual UI reset button.
         rate_limit_count resets to 0 on first success.

  PROBLEM 2 — Gemini model string may be wrong for your LiteLLM version:
    "gemini/gemini-2.0-flash" fails with 404/model_not_found on older
    LiteLLM. The router was treating this as permanent (model_gone=True)
    and never retrying other Gemini strings.
    FIX: GEMINI_MODEL_FALLBACKS list — tries 5 Gemini model strings
         automatically. Caches the first one that works. If model_gone
         error, removes that string and re-probes.

  PROBLEM 3 — get_active_provider() showed NEXT provider, not LAST USED:
    The header always said "Gemini" even when Groq actually answered,
    because get_active_provider() just returned the top available model.
    FIX: _LAST_USED_PROVIDER / _LAST_USED_MODEL globals updated by
         every successful call. get_active_provider() returns the real
         last-used provider. get_next_provider() shows what WILL be used.

  PROBLEM 4 — Inline rate limit sleep froze the UI for 8-12s:
    FIX: Reduced to 3s (Gemini) / 5s (others) for the inline retry.
  ─────────────────────────────────────────────────────

FIXES from v2.3 (retained):
  Gemini quality=5 > Groq quality=4 — Gemini always wins sort
  _get_key() reads st.secrets directly — bulletproof Streamlit Cloud

PROVIDER PRIORITY (auto-detected from available keys):
  Tier 1 — Gemini 2.0 Flash    (FREE · 1M TPM · quality=5) <- PRIMARY
  Tier 2 — Gemini 1.5 Flash    (FREE · 1M TPM · quality=5) <- BACKUP
  Tier 3 — Groq Llama-3.3-70b  (FREE · 6k  TPM · quality=4)
  Tier 4 — Groq DeepSeek-R1    (FREE · 6k  TPM · quality=4)
  Tier 5 — Groq Llama3-70b     (FREE · 6k  TPM · quality=3)
  Tier 6 — Groq Gemma2-9b      (FREE · 15k TPM · quality=3)
  Tier 7 — Mistral Small       (FREE tier · quality=3)
  Tier 8 — GPT-4o-mini         (PAID · quality=4)
  Tier 9 — GPT-4o              (PAID · quality=5)
  Tier 10 — Claude Sonnet      (PAID · quality=5 · best ETL)
  Tier 11 — Ollama local       (FREE · offline · quality=3)
"""

from __future__ import annotations
import os
import time
import logging
from dataclasses import dataclass, field
from typing import Literal

logger = logging.getLogger("AI_ROUTER")

# ─────────────────────────────────────────────────────────────
# GLOBAL LAST-USED TRACKING
# Updated by every successful call — tells the UI what ACTUALLY responded
# ─────────────────────────────────────────────────────────────
_LAST_USED_PROVIDER = "None yet"
_LAST_USED_MODEL    = "—"


# ─────────────────────────────────────────────────────────────
# BULLETPROOF KEY LOOKUP — Layer 1: os.environ, Layer 2: st.secrets
# ─────────────────────────────────────────────────────────────
def _get_key(env_key: str) -> str:
    if not env_key:
        return ""
    val = os.environ.get(env_key, "").strip()
    if val:
        return val
    try:
        import streamlit as st
        val = str(st.secrets.get(env_key, "")).strip()
        if val:
            os.environ[env_key] = val
            logger.info(f"[ROUTER] Key {env_key} loaded from st.secrets")
        return val
    except Exception:
        return ""


# ─────────────────────────────────────────────────────────────
# MODEL REGISTRY
# ─────────────────────────────────────────────────────────────
@dataclass
class ModelConfig:
    model: str
    env_key: str
    tpm: int
    rpd: int
    quality: int
    cost_per_1k: float
    provider: str
    task_types: list = field(default_factory=lambda: ["code", "jira", "summary"])


ALL_MODELS: list[ModelConfig] = [

    # FREE - Gemini PRIMARY (quality=5 beats Groq quality=4)
    ModelConfig(
        model="gemini/gemini-2.0-flash",
        env_key="GEMINI_API_KEY",
        tpm=1_000_000, rpd=1500,
        quality=5, cost_per_1k=0.0,
        provider="Google Gemini 2.0 Flash",
    ),
    ModelConfig(
        model="gemini/gemini-1.5-flash",
        env_key="GEMINI_API_KEY",
        tpm=1_000_000, rpd=1500,
        quality=5, cost_per_1k=0.0,
        provider="Google Gemini 1.5 Flash",
    ),

    # FREE - Groq FALLBACK
    ModelConfig(
        model="groq/llama-3.3-70b-versatile",
        env_key="GROQ_API_KEY",
        tpm=6_000, rpd=0,
        quality=4, cost_per_1k=0.0,
        provider="Groq · Llama-3.3-70b",
    ),
    ModelConfig(
        model="groq/deepseek-r1-distill-llama-70b",
        env_key="GROQ_API_KEY",
        tpm=6_000, rpd=0,
        quality=4, cost_per_1k=0.0,
        provider="Groq · DeepSeek-R1",
        task_types=["code", "summary"],
    ),
    ModelConfig(
        model="groq/llama3-70b-8192",
        env_key="GROQ_API_KEY",
        tpm=6_000, rpd=0,
        quality=3, cost_per_1k=0.0,
        provider="Groq · Llama3-70b",
        task_types=["code", "summary"],
    ),
    ModelConfig(
        model="groq/gemma2-9b-it",
        env_key="GROQ_API_KEY",
        tpm=15_000, rpd=0,
        quality=3, cost_per_1k=0.0,
        provider="Groq · Gemma2-9b",
        task_types=["code", "summary"],
    ),

    # FREE - Mistral
    ModelConfig(
        model="mistral/mistral-small-latest",
        env_key="MISTRAL_API_KEY",
        tpm=500_000, rpd=0,
        quality=3, cost_per_1k=0.0,
        provider="Mistral Small",
        task_types=["code", "summary"],
    ),

    # PAID - OpenAI
    ModelConfig(
        model="gpt-4o-mini",
        env_key="OPENAI_API_KEY",
        tpm=2_000_000, rpd=0,
        quality=4, cost_per_1k=0.00015,
        provider="OpenAI · GPT-4o-mini",
    ),
    ModelConfig(
        model="gpt-4o",
        env_key="OPENAI_API_KEY",
        tpm=800_000, rpd=0,
        quality=5, cost_per_1k=0.005,
        provider="OpenAI · GPT-4o",
    ),

    # PAID - Anthropic
    ModelConfig(
        model="claude-3-5-sonnet-20241022",
        env_key="ANTHROPIC_API_KEY",
        tpm=200_000, rpd=0,
        quality=5, cost_per_1k=0.003,
        provider="Anthropic · Claude Sonnet",
    ),

    # LOCAL - Ollama
    ModelConfig(
        model="ollama/codellama",
        env_key="",
        tpm=999_999, rpd=0,
        quality=3, cost_per_1k=0.0,
        provider="Ollama · CodeLlama (Local)",
        task_types=["code"],
    ),
]

TASK_TOKENS = {"code": 1200, "summary": 350, "jira": 2800}

# Gemini model string fallbacks — tries each until one works
# Needed because different LiteLLM versions recognise different strings
GEMINI_MODEL_FALLBACKS = [
    "gemini/gemini-2.0-flash",
    "gemini/gemini-2.0-flash-exp",
    "gemini/gemini-2.0-flash-001",
    "gemini/gemini-1.5-flash",
    "gemini/gemini-1.5-flash-latest",
]
_gemini_working_model: str | None = None  # cached after first successful probe


# ─────────────────────────────────────────────────────────────
# PER-MODEL RUNTIME STATUS
# ─────────────────────────────────────────────────────────────
@dataclass
class _ModelStatus:
    rate_limited_until: float = 0.0
    auth_failed: bool = False
    model_gone: bool = False
    total_calls: int = 0
    total_tokens: int = 0
    last_used_ts: float = 0.0
    rate_limit_count: int = 0  # tracks consecutive hits for graduated backoff

_status_registry: dict[str, _ModelStatus] = {}

def _st(model: str) -> _ModelStatus:
    if model not in _status_registry:
        _status_registry[model] = _ModelStatus()
    return _status_registry[model]


def reset_cooldowns() -> int:
    """Clear all rate-limit cooldowns immediately. Call from UI Reset button."""
    cleared = 0
    for s in _status_registry.values():
        if s.rate_limited_until > time.time():
            s.rate_limited_until = 0.0
            s.rate_limit_count = 0
            cleared += 1
    logger.info(f"[ROUTER] Manual reset — {cleared} cooldown(s) cleared")
    return cleared


# ─────────────────────────────────────────────────────────────
# AVAILABILITY CHECK
# ─────────────────────────────────────────────────────────────
def _is_available(cfg: ModelConfig) -> bool:
    s = _st(cfg.model)
    if s.auth_failed or s.model_gone:
        return False
    if time.time() < s.rate_limited_until:
        return False
    if cfg.env_key and not _get_key(cfg.env_key):
        return False
    if "ollama" in cfg.model and not _get_key("OLLAMA_ENABLED"):
        return False
    return True


def _mark_rate_limit(model: str):
    """Graduated backoff: 30s → 60s → 90s (not flat 60s like before)."""
    s = _st(model)
    s.rate_limit_count += 1
    backoff = min(30 * s.rate_limit_count, 90)
    s.rate_limited_until = time.time() + backoff
    logger.warning(f"[ROUTER] {model} cooldown={backoff}s (hit #{s.rate_limit_count})")


def _mark_success(model: str, tokens: int = 0, provider: str = "", display_model: str = ""):
    global _LAST_USED_PROVIDER, _LAST_USED_MODEL
    s = _st(model)
    s.total_calls += 1
    s.total_tokens += tokens
    s.last_used_ts = time.time()
    s.rate_limited_until = 0.0
    s.rate_limit_count = 0
    if provider:
        _LAST_USED_PROVIDER = provider
    if display_model:
        _LAST_USED_MODEL = display_model


# ─────────────────────────────────────────────────────────────
# ERROR CLASSIFIERS
# ─────────────────────────────────────────────────────────────
def _is_rate_err(exc: Exception) -> bool:
    s = (str(type(exc).__name__) + str(exc)).lower()
    return any(x in s for x in [
        "rate", "429", "ratelimit", "quota", "tokens per",
        "requests per", "toomanyrequests", "resource_exhausted",
        "too many requests",
    ])

def _is_auth_err(exc: Exception) -> bool:
    s = str(exc).lower()
    return any(x in s for x in [
        "401", "403", "invalid_api_key", "authentication",
        "unauthorized", "permission_denied",
    ])

def _is_gone_err(exc: Exception) -> bool:
    s = str(exc).lower()
    return any(x in s for x in [
        "decommissioned", "not supported", "no longer supported",
        "model_not_found", "does not exist", "404",
        "invalid argument", "unsupported",
    ])


RATE_LIMIT_SENTINEL = "__RATE_LIMIT__"


# ─────────────────────────────────────────────────────────────
# GEMINI MODEL STRING AUTO-RESOLVER
# Different LiteLLM versions need different Gemini model strings.
# Probes the list and caches the first one that works.
# ─────────────────────────────────────────────────────────────
def _resolve_gemini_model(api_key: str) -> str | None:
    global _gemini_working_model

    if _gemini_working_model:
        return _gemini_working_model

    try:
        import litellm
        litellm.drop_params = True
    except ImportError:
        return None

    remaining = [m for m in GEMINI_MODEL_FALLBACKS]
    for candidate in remaining:
        try:
            logger.info(f"[ROUTER] Probing Gemini: {candidate}")
            resp = litellm.completion(
                model=candidate,
                messages=[{"role": "user", "content": "Hi"}],
                temperature=0.0,
                max_tokens=5,
                api_key=api_key,
            )
            if resp.choices:
                _gemini_working_model = candidate
                logger.info(f"[ROUTER] Gemini confirmed: {candidate}")
                return candidate
        except Exception as e:
            logger.warning(f"[ROUTER] {candidate} failed: {e}")
            if _is_auth_err(e):
                logger.error("[ROUTER] Gemini auth failed — check GEMINI_API_KEY")
                return None
            # model-specific error — try next string
            continue
    return None


# ─────────────────────────────────────────────────────────────
# CORE ROUTER
# ─────────────────────────────────────────────────────────────
def call_ai(
    messages: list[dict],
    temperature: float = 0.1,
    max_tokens: int = 1200,
    task: Literal["code", "summary", "jira"] = "code",
    require_free: bool = False,
    force_provider: str | None = None,
) -> str:
    global _gemini_working_model

    try:
        import litellm
        litellm.drop_params = True
        litellm.set_verbose = False
    except ImportError:
        raise RuntimeError("litellm not installed — pip install litellm")

    token_limit = TASK_TOKENS.get(task, max_tokens)

    candidates = [
        c for c in ALL_MODELS
        if task in c.task_types
        and _is_available(c)
        and (not require_free or c.cost_per_1k == 0.0)
        and (not force_provider or c.provider == force_provider)
    ]

    if not candidates:
        logger.error("[ROUTER] No providers available.")
        return RATE_LIMIT_SENTINEL

    # Gemini (quality=5) always sorts above Groq (quality=4)
    candidates.sort(key=lambda c: (-c.quality, c.cost_per_1k))
    logger.info(f"[ROUTER] Order: {[c.provider for c in candidates]}")

    # De-duplicate Gemini entries — only keep one Gemini config per call
    # (both 2.0-flash and 1.5-flash configs exist; the resolver picks the right model)
    seen_gemini = False
    deduped = []
    for c in candidates:
        if "gemini" in c.model:
            if seen_gemini:
                continue  # skip duplicate Gemini config
            seen_gemini = True
        deduped.append(c)
    candidates = deduped

    for cfg in candidates:
        actual_model = cfg.model

        # Auto-resolve Gemini model string
        if "gemini" in cfg.model:
            gemini_key = _get_key(cfg.env_key)
            resolved = _resolve_gemini_model(gemini_key)
            if resolved is None:
                logger.warning("[ROUTER] Gemini unavailable — falling back")
                _st(cfg.model).model_gone = True
                continue
            actual_model = resolved

        for attempt in range(2):
            try:
                logger.info(f"[ROUTER] -> {cfg.provider} [{actual_model}] attempt={attempt+1}")
                kwargs = {"api_key": _get_key(cfg.env_key)} if cfg.env_key else {}

                resp = litellm.completion(
                    model=actual_model,
                    messages=messages,
                    temperature=temperature,
                    max_tokens=token_limit,
                    **kwargs,
                )
                content = resp.choices[0].message.content or ""
                tokens  = getattr(resp.usage, "total_tokens", 0)
                _mark_success(
                    cfg.model, tokens,
                    provider=cfg.provider,
                    display_model=actual_model.split("/")[-1],
                )
                logger.info(f"[ROUTER] SUCCESS: {cfg.provider} | {tokens} tokens")
                return content

            except Exception as exc:
                logger.warning(f"[ROUTER] {cfg.provider} error: {exc}")

                if _is_auth_err(exc):
                    _st(cfg.model).auth_failed = True
                    logger.error(f"[ROUTER] Auth failed — check {cfg.env_key}")
                    break

                if _is_gone_err(exc):
                    if "gemini" in cfg.model:
                        # Invalidate cached model string and let resolver try next
                        logger.warning(f"[ROUTER] {actual_model} gone — resetting Gemini probe")
                        _gemini_working_model = None
                        if actual_model in GEMINI_MODEL_FALLBACKS:
                            GEMINI_MODEL_FALLBACKS.remove(actual_model)
                    else:
                        _st(cfg.model).model_gone = True
                    break

                if _is_rate_err(exc):
                    if attempt == 0:
                        wait = 3 if "gemini" in cfg.model else 5
                        logger.info(f"[ROUTER] Rate limit — retry in {wait}s")
                        time.sleep(wait)
                        continue
                    _mark_rate_limit(cfg.model)  # graduated backoff
                    break

                break  # unknown error — try next provider

    logger.error("[ROUTER] All providers exhausted.")
    return RATE_LIMIT_SENTINEL


# ─────────────────────────────────────────────────────────────
# BACKWARD-COMPATIBLE SHIM
# ─────────────────────────────────────────────────────────────
def call_ai_compat(
    messages: list,
    temperature: float = 0.1,
    max_tokens: int = 2000,
    model: str = None,
    task: str = "code",
) -> str:
    return call_ai(messages, temperature=temperature, task=task)


# ─────────────────────────────────────────────────────────────
# UI HELPERS
# ─────────────────────────────────────────────────────────────
def get_router_status() -> list[dict]:
    rows = []
    for cfg in ALL_MODELS:
        is_local = "ollama" in cfg.model
        has_key  = bool(_get_key(cfg.env_key)) if cfg.env_key else True
        s   = _st(cfg.model)
        now = time.time()

        if s.auth_failed:
            status = "🔴 Auth Failed"
        elif s.model_gone:
            status = "⚫ Unavailable"
        elif not has_key and not is_local:
            status = "⚪ No Key"
        elif now < s.rate_limited_until:
            remaining = int(s.rate_limited_until - now)
            status = f"🟡 Cooldown {remaining}s (#{s.rate_limit_count})"
        else:
            status = "🟢 Ready"

        is_last = (cfg.provider == _LAST_USED_PROVIDER)
        rows.append({
            "Provider":  ("⭐ LAST USED → " if is_last else "") + cfg.provider,
            "Model":     cfg.model.split("/")[-1],
            "Cost":      "FREE" if cfg.cost_per_1k == 0 else f"${cfg.cost_per_1k:.4f}/1k",
            "TPM":       f"{cfg.tpm:,}",
            "Quality":   "★" * cfg.quality + "☆" * (5 - cfg.quality),
            "Status":    status,
            "Calls":     s.total_calls,
            "Tokens":    f"{s.total_tokens:,}",
        })
    return rows


def get_active_provider() -> str:
    """Returns the provider that ACTUALLY answered the last call."""
    if _LAST_USED_PROVIDER != "None yet":
        return _LAST_USED_PROVIDER
    # No call made yet — return next-up
    for cfg in sorted(ALL_MODELS, key=lambda c: (-c.quality, c.cost_per_1k)):
        if _is_available(cfg) and "code" in cfg.task_types:
            return cfg.provider
    return "None configured"


def get_active_model() -> str:
    """Returns the model that ACTUALLY answered the last call."""
    if _LAST_USED_MODEL != "—":
        return _LAST_USED_MODEL
    for cfg in sorted(ALL_MODELS, key=lambda c: (-c.quality, c.cost_per_1k)):
        if _is_available(cfg) and "code" in cfg.task_types:
            return cfg.model.split("/")[-1]
    return "—"


def get_next_provider() -> str:
    """Returns the provider that WILL be used on the next call."""
    for cfg in sorted(ALL_MODELS, key=lambda c: (-c.quality, c.cost_per_1k)):
        if _is_available(cfg) and "code" in cfg.task_types:
            return cfg.provider
    return "None available"
