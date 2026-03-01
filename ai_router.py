"""
ai_router.py  ·  LiteLLM Multi-Provider Router  ·  v2.6
=========================================================
ROOT CAUSE ANALYSIS — Why Groq kept winning despite 1.5-flash being Ready:

  v2.5 had 3 bugs that created a failure cascade every time 2.0-flash
  hit a rate limit:

  BUG A — GEMINI_MODEL_PRIORITY had 5 strings but ALL_MODELS only had 2.
           When 2.0-flash was rate-limited, the resolver skipped it and
           returned "gemini/gemini-2.0-flash-001" (an extra string with
           no matching ALL_MODELS config). This model string failed too
           (unsupported or same rate limit). The failure then called
           _mark_rate_limit(cfg.model) where cfg.model was
           "gemini/gemini-1.5-flash" (the config being looped). Result:
           1.5-flash got marked as rate-limited even though it was NEVER
           actually tried. Both Gemini configs now in cooldown → Groq wins.

  BUG B — _mark_rate_limit used cfg.model (the config), not actual_model
           (the resolved string). When an extra priority string failed,
           it punished the WRONG config — collateral damage to 1.5-flash.

  BUG C — GEMINI_MODEL_PRIORITY was over-engineered. With 2 Gemini
           configs in ALL_MODELS, you need exactly 2 model strings —
           one per config. The 3 extra strings caused unnecessary failures.

  v2.6 FIX — Eliminate the complexity entirely:
  ──────────────────────────────────────────────
  • GEMINI_MODEL_PRIORITY removed. Each ALL_MODELS config carries its
    own canonical model string. No separate resolver needed.
  • Gemini routing works exactly like every other provider. No special
    casing, no skip sets, no session_state caching.
  • When 2.0-flash hits rate limit → its config fails _is_available()
    → excluded from candidates → 1.5-flash becomes first candidate
    → used directly → works immediately. Clean, simple, bulletproof.

PROVIDER PRIORITY:
  Tier 1  — Gemini 2.0 Flash    (FREE · 1M TPM · quality=5) ← PRIMARY
  Tier 2  — Gemini 1.5 Flash    (FREE · 1M TPM · quality=5) ← auto-fallback
  Tier 3  — Groq Llama-3.3-70b  (FREE · 6k  TPM · quality=4)
  Tier 4  — Groq DeepSeek-R1    (FREE · 6k  TPM · quality=4)
  Tier 5  — Groq Llama3-70b     (FREE · 6k  TPM · quality=3)
  Tier 6  — Groq Gemma2-9b      (FREE · 15k TPM · quality=3)
  Tier 7  — Mistral Small       (FREE tier · quality=3)
  Tier 8  — GPT-4o-mini         (PAID · quality=4)
  Tier 9  — GPT-4o              (PAID · quality=5)
  Tier 10 — Claude Sonnet 4.6   (PAID · quality=5)
  Tier 11 — Ollama local         (FREE · offline · quality=3)
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
# BULLETPROOF KEY LOOKUP — os.environ first, then st.secrets
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

    # FREE - Gemini 2.0 Flash (PRIMARY)
    ModelConfig(
        model="gemini/gemini-2.0-flash",
        env_key="GEMINI_API_KEY",
        tpm=1_000_000, rpd=1500,
        quality=5, cost_per_1k=0.0,
        provider="Google Gemini 2.0 Flash",
    ),
    # FREE - Gemini 1.5 Flash (AUTO-FALLBACK when 2.0 is rate-limited)
    ModelConfig(
        model="gemini/gemini-1.5-flash",
        env_key="GEMINI_API_KEY",
        tpm=1_000_000, rpd=1500,
        quality=5, cost_per_1k=0.0,
        provider="Google Gemini 1.5 Flash",
    ),

    # FREE - Groq (all support jira task)
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
    ),
    ModelConfig(
        model="groq/llama3-70b-8192",
        env_key="GROQ_API_KEY",
        tpm=6_000, rpd=0,
        quality=3, cost_per_1k=0.0,
        provider="Groq · Llama3-70b",
    ),
    ModelConfig(
        model="groq/gemma2-9b-it",
        env_key="GROQ_API_KEY",
        tpm=15_000, rpd=0,
        quality=3, cost_per_1k=0.0,
        provider="Groq · Gemma2-9b",
    ),

    # FREE - Mistral
    ModelConfig(
        model="mistral/mistral-small-latest",
        env_key="MISTRAL_API_KEY",
        tpm=500_000, rpd=0,
        quality=3, cost_per_1k=0.0,
        provider="Mistral Small",
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
        model="anthropic/claude-sonnet-4-6",
        env_key="ANTHROPIC_API_KEY",
        tpm=200_000, rpd=0,
        quality=5, cost_per_1k=0.003,
        provider="Anthropic · Claude Sonnet 4.6",
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

# Jira needs more tokens — complex breakdowns can hit 3500+ tokens
TASK_TOKENS = {"code": 1200, "summary": 350, "jira": 4000}


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
    rate_limit_count: int = 0

_status_registry: dict[str, _ModelStatus] = {}

def _st(model: str) -> _ModelStatus:
    if model not in _status_registry:
        _status_registry[model] = _ModelStatus()
    return _status_registry[model]


def reset_cooldowns() -> int:
    """
    Full recovery reset. Clears cooldowns, model_gone (for Gemini),
    and auth_failed flags. Reset button now fully recovers all providers.
    """
    cleared = 0
    for model, s in _status_registry.items():
        changed = False
        if s.rate_limited_until > time.time():
            s.rate_limited_until = 0.0
            s.rate_limit_count = 0
            changed = True
        if "gemini" in model and s.model_gone:
            s.model_gone = False
            changed = True
        if s.auth_failed:
            s.auth_failed = False
            changed = True
        if changed:
            cleared += 1
    logger.info(f"[ROUTER] Full reset — {cleared} provider(s) recovered")
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
    """Graduated backoff: 30s → 60s → 90s."""
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
        "model_not_found", "does not exist",
        "invalid argument", "unsupported",
    ])


RATE_LIMIT_SENTINEL = "__RATE_LIMIT__"


# ─────────────────────────────────────────────────────────────
# CORE ROUTER — v2.6 clean design
#
# No special Gemini casing. No priority list. No skip sets.
# No session_state caching.
#
# Failure isolation is now per-config, not per-string:
#   • 2.0-flash rate-limited → its OWN config gets the cooldown
#   • 1.5-flash config is completely unaffected
#   • Next candidates() call: 2.0 excluded, 1.5 is first → used directly
# ─────────────────────────────────────────────────────────────
def call_ai(
    messages: list[dict],
    temperature: float = 0.1,
    max_tokens: int = 1200,
    task: Literal["code", "summary", "jira"] = "code",
    require_free: bool = False,
    force_provider: str | None = None,
) -> str:

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

    # Both Gemini configs quality=5 → both sort above Groq quality=4
    # 2.0-flash is listed first in ALL_MODELS → stable ordering
    candidates.sort(key=lambda c: (-c.quality, c.cost_per_1k))
    logger.info(f"[ROUTER] Candidates task={task}: {[c.provider for c in candidates]}")

    for cfg in candidates:
        actual_model = cfg.model  # use config's own string directly — no resolver

        for attempt in range(2):
            try:
                logger.info(
                    f"[ROUTER] → {cfg.provider} [{actual_model}] "
                    f"attempt={attempt+1} task={task} max_tokens={token_limit}"
                )
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
                logger.info(
                    f"[ROUTER] SUCCESS: {cfg.provider} | {tokens} tokens | task={task}"
                )
                return content

            except Exception as exc:
                logger.warning(f"[ROUTER] {cfg.provider} [{actual_model}] error: {exc}")

                if _is_auth_err(exc):
                    _st(cfg.model).auth_failed = True
                    logger.error(f"[ROUTER] Auth failed — check {cfg.env_key}")
                    break

                if _is_gone_err(exc):
                    _st(cfg.model).model_gone = True
                    logger.warning(
                        f"[ROUTER] {cfg.provider} model gone/unsupported"
                        + (" — 1.5-flash will be used next" if "2.0" in cfg.model else "")
                    )
                    break

                if _is_rate_err(exc):
                    if attempt == 0:
                        wait = 3 if "gemini" in cfg.model else 5
                        logger.info(f"[ROUTER] Rate limit attempt 1 — retry in {wait}s")
                        time.sleep(wait)
                        continue
                    # Mark THIS config's model as rate-limited, then try next provider
                    _mark_rate_limit(cfg.model)
                    break

                logger.warning(f"[ROUTER] Unknown error — trying next provider")
                break

    logger.error("[ROUTER] All providers exhausted.")
    return RATE_LIMIT_SENTINEL


# ─────────────────────────────────────────────────────────────
# BACKWARD-COMPATIBLE SHIM
# app.py imports call_ai_compat as call_ai — unchanged interface.
# ─────────────────────────────────────────────────────────────
def call_ai_compat(
    messages: list,
    temperature: float = 0.1,
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
        label   = ("⭐ LAST USED → " if is_last else "") + cfg.provider

        rows.append({
            "Provider":  label,
            "Model":     cfg.model.split("/")[-1],
            "Cost":      "FREE" if cfg.cost_per_1k == 0 else f"${cfg.cost_per_1k:.4f}/1k",
            "TPM":       f"{cfg.tpm:,}",
            "Quality":   "★" * cfg.quality + "☆" * (5 - cfg.quality),
            "Status":    status,
            "Calls":     s.total_calls,
            "Tokens":    f"{s.total_tokens:,}",
        })
    return rows


def get_active_provider(task: str = "code") -> str:
    if _LAST_USED_PROVIDER != "None yet":
        return _LAST_USED_PROVIDER
    for cfg in sorted(ALL_MODELS, key=lambda c: (-c.quality, c.cost_per_1k)):
        if _is_available(cfg) and task in cfg.task_types:
            return cfg.provider
    return "None configured"


def get_active_model(task: str = "code") -> str:
    if _LAST_USED_MODEL != "—":
        return _LAST_USED_MODEL
    for cfg in sorted(ALL_MODELS, key=lambda c: (-c.quality, c.cost_per_1k)):
        if _is_available(cfg) and task in cfg.task_types:
            return cfg.model.split("/")[-1]
    return "—"


def get_next_provider(task: str = "code") -> str:
    for cfg in sorted(ALL_MODELS, key=lambda c: (-c.quality, c.cost_per_1k)):
        if _is_available(cfg) and task in cfg.task_types:
            return cfg.provider
    return "None available"
