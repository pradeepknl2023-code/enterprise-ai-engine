"""
ai_router.py  ·  LiteLLM Multi-Provider Router  ·  v2.5
=========================================================
ROOT CAUSE FIX in v2.5:
  ─────────────────────────────────────────────────────
  PROBLEM (v2.4 and earlier):
    _resolve_gemini_model() made a PROBE API call ("Hi") before
    every real ETL call. This wasted 1 of Gemini's 15 free RPM
    slots per run. With 2 calls per ETL (probe + real), you hit
    the rate limit twice as fast. The probe itself could also
    get rate-limited, which then marked Gemini as unavailable
    and fell through to Groq — even for tiny prompts.

  FIX (v2.5):
    ✅ REMOVED the probe call entirely.
    ✅ Router just tries Gemini directly with the real prompt.
    ✅ If it fails with 404/model_not_found, tries the next
       Gemini model string automatically (same fallback list,
       but now driven by real calls not wasted probes).
    ✅ Gemini model string that works is cached after first
       SUCCESS (not after a probe) — zero wasted quota.
    ✅ Result: 1 Gemini call per ETL run instead of 2.
       Your 15 RPM free tier now lasts 15 real ETL runs/min.

  OTHER FIXES retained from v2.3/v2.4:
    ✅ Gemini quality=5 > Groq quality=4 — always sorts first
    ✅ _get_key() reads st.secrets directly — bulletproof
    ✅ Graduated backoff: 30s → 60s → 90s (not flat 60s)
    ✅ reset_cooldowns() for manual UI reset button
    ✅ _LAST_USED_PROVIDER tracks who actually answered
    ✅ get_next_provider() shows who will answer next
  ─────────────────────────────────────────────────────

PROVIDER PRIORITY:
  Tier 1 — Gemini 2.0 Flash    (FREE · 1M TPM · 15 RPM) ← PRIMARY
  Tier 2 — Gemini 1.5 Flash    (FREE · 1M TPM · 15 RPM) ← BACKUP
  Tier 3 — Groq Llama-3.3-70b  (FREE · 6k TPM · 30 RPM) ← FALLBACK
  Tier 4 — Groq DeepSeek-R1    (FREE · 6k TPM)
  Tier 5 — Groq Llama3-70b     (FREE · 6k TPM)
  Tier 6 — Groq Gemma2-9b      (FREE · 15k TPM)
  Tier 7 — Mistral Small       (FREE tier)
  Tier 8 — GPT-4o-mini         (PAID)
  Tier 9 — GPT-4o              (PAID)
  Tier 10 — Claude Sonnet      (PAID · best ETL)
  Tier 11 — Ollama local       (FREE · offline)
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
# ─────────────────────────────────────────────────────────────
_LAST_USED_PROVIDER = "None yet"
_LAST_USED_MODEL    = "—"


# ─────────────────────────────────────────────────────────────
# BULLETPROOF KEY LOOKUP
# Layer 1: os.environ  |  Layer 2: st.secrets
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
            os.environ[env_key] = val          # cache for next call
            logger.info(f"[ROUTER] {env_key} loaded from st.secrets")
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

    # FREE — Gemini PRIMARY
    # quality=5 guarantees it sorts above Groq (quality=4)
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

    # FREE — Groq FALLBACK
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

    # FREE — Mistral
    ModelConfig(
        model="mistral/mistral-small-latest",
        env_key="MISTRAL_API_KEY",
        tpm=500_000, rpd=0,
        quality=3, cost_per_1k=0.0,
        provider="Mistral Small",
        task_types=["code", "summary"],
    ),

    # PAID — OpenAI
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

    # PAID — Anthropic
    ModelConfig(
        model="claude-3-5-sonnet-20241022",
        env_key="ANTHROPIC_API_KEY",
        tpm=200_000, rpd=0,
        quality=5, cost_per_1k=0.003,
        provider="Anthropic · Claude Sonnet",
    ),

    # LOCAL — Ollama
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

# Gemini model string fallback order
# Tried in order when the primary string fails with a 404/model_not_found error
# The FIRST one that succeeds is cached — no probe calls, zero wasted quota
GEMINI_FALLBACK_MODELS = [
    "gemini/gemini-2.0-flash",
    "gemini/gemini-2.0-flash-exp",
    "gemini/gemini-2.0-flash-001",
    "gemini/gemini-1.5-flash",
    "gemini/gemini-1.5-flash-latest",
]

# Cached after first successful Gemini call — never changes until a model_gone error
_gemini_confirmed_model: str | None = None


# ─────────────────────────────────────────────────────────────
# PER-MODEL RUNTIME STATUS
# ─────────────────────────────────────────────────────────────
@dataclass
class _ModelStatus:
    rate_limited_until: float = 0.0
    auth_failed: bool        = False
    model_gone: bool         = False
    total_calls: int         = 0
    total_tokens: int        = 0
    last_used_ts: float      = 0.0
    rate_limit_count: int    = 0    # for graduated backoff

_status_registry: dict[str, _ModelStatus] = {}

def _st(model: str) -> _ModelStatus:
    if model not in _status_registry:
        _status_registry[model] = _ModelStatus()
    return _status_registry[model]


def reset_cooldowns() -> int:
    """Clear all rate-limit cooldowns. Called by the UI Reset button."""
    cleared = 0
    for s in _status_registry.values():
        if s.rate_limited_until > time.time():
            s.rate_limited_until = 0.0
            s.rate_limit_count   = 0
            cleared += 1
    # Also reset the Gemini confirmed model so it re-discovers on next call
    global _gemini_confirmed_model
    _gemini_confirmed_model = None
    logger.info(f"[ROUTER] reset_cooldowns — {cleared} model(s) cleared")
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
    """Graduated backoff: 30s first hit, 60s second, 90s third+."""
    s = _st(model)
    s.rate_limit_count += 1
    backoff = min(30 * s.rate_limit_count, 90)
    s.rate_limited_until = time.time() + backoff
    logger.warning(f"[ROUTER] {model} → cooldown {backoff}s (hit #{s.rate_limit_count})")


def _mark_success(model: str, tokens: int = 0, provider: str = "", display_model: str = ""):
    global _LAST_USED_PROVIDER, _LAST_USED_MODEL
    s = _st(model)
    s.total_calls       += 1
    s.total_tokens      += tokens
    s.last_used_ts       = time.time()
    s.rate_limited_until = 0.0
    s.rate_limit_count   = 0
    if provider:
        _LAST_USED_PROVIDER = provider
    if display_model:
        _LAST_USED_MODEL = display_model
    logger.info(f"[ROUTER] ✅ SUCCESS — {provider} · {tokens} tokens")


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
    global _gemini_confirmed_model

    try:
        import litellm
        litellm.drop_params = True
        litellm.set_verbose = False
    except ImportError:
        raise RuntimeError("litellm not installed — pip install litellm")

    token_limit = TASK_TOKENS.get(task, max_tokens)

    # Build candidate list
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

    # Sort: quality DESC → cost ASC
    # Gemini quality=5 always above Groq quality=4
    candidates.sort(key=lambda c: (-c.quality, c.cost_per_1k))

    # De-duplicate Gemini — only process one Gemini config entry per call
    # (both 2.0-flash and 1.5-flash configs exist; we handle both via fallback list)
    seen_gemini = False
    deduped = []
    for c in candidates:
        if "gemini" in c.model:
            if seen_gemini:
                continue
            seen_gemini = True
        deduped.append(c)
    candidates = deduped

    logger.info(f"[ROUTER] Candidate order: {[c.provider for c in candidates]}")

    for cfg in candidates:

        # ── GEMINI: use confirmed model or walk the fallback list ──
        if "gemini" in cfg.model:
            gemini_key = _get_key(cfg.env_key)
            # Which model strings to try this turn?
            if _gemini_confirmed_model:
                models_to_try = [_gemini_confirmed_model]
            else:
                models_to_try = GEMINI_FALLBACK_MODELS[:]

            last_gemini_err = None
            for gemini_model in models_to_try:
                for attempt in range(2):
                    try:
                        logger.info(f"[ROUTER] → Gemini [{gemini_model}] attempt={attempt+1}")
                        resp = litellm.completion(
                            model=gemini_model,
                            messages=messages,
                            temperature=temperature,
                            max_tokens=token_limit,
                            api_key=gemini_key,
                        )
                        content = resp.choices[0].message.content or ""
                        tokens  = getattr(resp.usage, "total_tokens", 0)
                        # Cache this model string — it works!
                        _gemini_confirmed_model = gemini_model
                        _mark_success(
                            cfg.model, tokens,
                            provider=cfg.provider,
                            display_model=gemini_model.split("/")[-1],
                        )
                        return content

                    except Exception as exc:
                        last_gemini_err = exc
                        logger.warning(f"[ROUTER] Gemini [{gemini_model}] error: {exc}")

                        if _is_auth_err(exc):
                            # Bad key — no point trying other models
                            _st(cfg.model).auth_failed = True
                            logger.error("[ROUTER] 🔑 Gemini auth failed — check GEMINI_API_KEY")
                            break   # break attempt loop

                        if _is_gone_err(exc):
                            # This model string doesn't exist — try next string
                            logger.warning(f"[ROUTER] {gemini_model} not found — trying next Gemini string")
                            break   # break attempt loop, continue gemini_model loop

                        if _is_rate_err(exc):
                            if attempt == 0:
                                # Short sleep then retry same model
                                logger.info("[ROUTER] Gemini rate limit — retry in 3s")
                                time.sleep(3)
                                continue
                            # Second attempt also rate-limited — mark cooldown and give up Gemini
                            _mark_rate_limit(cfg.model)
                            logger.warning("[ROUTER] Gemini rate-limited — falling back to Groq")
                            break   # break attempt loop

                        # Unknown error — try next Gemini string
                        break

                else:
                    # attempt loop exhausted without break — shouldn't happen, safety net
                    continue

                # If auth failed, stop all Gemini strings
                if _st(cfg.model).auth_failed:
                    break
                # If rate-limited, stop all Gemini strings (cooldown set)
                if time.time() < _st(cfg.model).rate_limited_until:
                    break
                # Otherwise (model_gone or unknown) — continue to next Gemini string

            # All Gemini strings tried — move to next provider (Groq)
            continue

        # ── NON-GEMINI PROVIDERS ──────────────────────────────────
        for attempt in range(2):
            try:
                logger.info(f"[ROUTER] → {cfg.provider} [{cfg.model}] attempt={attempt+1}")
                kwargs = {"api_key": _get_key(cfg.env_key)} if cfg.env_key else {}
                resp = litellm.completion(
                    model=cfg.model,
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
                    display_model=cfg.model.split("/")[-1],
                )
                return content

            except Exception as exc:
                logger.warning(f"[ROUTER] {cfg.provider} error: {exc}")
                if _is_auth_err(exc):
                    _st(cfg.model).auth_failed = True
                    break
                if _is_gone_err(exc):
                    _st(cfg.model).model_gone = True
                    break
                if _is_rate_err(exc):
                    if attempt == 0:
                        time.sleep(5)
                        continue
                    _mark_rate_limit(cfg.model)
                    break
                break

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
            status = f"🟡 Cooldown {remaining}s (hit #{s.rate_limit_count})"
        else:
            status = "🟢 Ready"

        is_last = (cfg.provider == _LAST_USED_PROVIDER)
        rows.append({
            "Provider":  ("⭐ " if is_last else "") + cfg.provider,
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
    """Who actually answered the last call."""
    if _LAST_USED_PROVIDER != "None yet":
        return _LAST_USED_PROVIDER
    for cfg in sorted(ALL_MODELS, key=lambda c: (-c.quality, c.cost_per_1k)):
        if _is_available(cfg) and "code" in cfg.task_types:
            return cfg.provider
    return "None configured"


def get_active_model() -> str:
    """Model string that actually answered the last call."""
    if _LAST_USED_MODEL != "—":
        return _LAST_USED_MODEL
    for cfg in sorted(ALL_MODELS, key=lambda c: (-c.quality, c.cost_per_1k)):
        if _is_available(cfg) and "code" in cfg.task_types:
            return cfg.model.split("/")[-1]
    return "—"


def get_next_provider() -> str:
    """Who will answer the NEXT call (based on current availability)."""
    for cfg in sorted(ALL_MODELS, key=lambda c: (-c.quality, c.cost_per_1k)):
        if _is_available(cfg) and "code" in cfg.task_types:
            return cfg.provider
    return "None available"
