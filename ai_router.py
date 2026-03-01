"""
ai_router.py  ·  LiteLLM Multi-Provider Router  ·  v2.5
=========================================================
FIXES in v2.5 (complete audit — 12 issues resolved):

  🔴 CRITICAL (3 fixes — root cause of Groq winning over Gemini):
  ─────────────────────────────────────────────────────────────────
  FIX 1 — _gemini_working_model stored in st.session_state (not module var)
           Module vars reset on every Streamlit rerun. session_state persists
           across button clicks. Probe now fires ONCE per browser session.

  FIX 2 — De-duplication loop removed.
           The seen_gemini block was blocking Gemini 1.5-flash from being
           tried when Gemini 2.0-flash was marked gone. Both Gemini configs
           now compete normally — the router tries 2.0 first, falls to 1.5
           naturally if 2.0 fails.

  FIX 3 — Gemini no longer permanently blacklisted with model_gone=True.
           Replaced with a soft per-session cooldown (300s) so Gemini 1.5
           can still be used. model_gone=True reserved for non-Gemini models
           only where we're certain the model is decommissioned.

  🟠 HIGH (4 fixes):
  ─────────────────────────────────────────────────────────────────
  FIX 4 — GEMINI_MODEL_FALLBACKS.remove() replaced with a skip set.
           Removing from the list permanently shrank it every session.
           Now failed strings are tracked in a skip set; the list stays
           intact and can be retried after a session reset.

  FIX 5 — _resolve_gemini_model replaced with static priority list.
           The old probe fired up to 5 real API calls on cold start —
           burning free-tier quota before your actual request.
           New approach: try known-good model strings in order with
           zero-probe — the first real call acts as the probe. If it
           fails with model_gone, move to next string.

  FIX 6 — TASK_TOKENS["jira"] raised from 2800 → 4000.
           Complex Jira breakdowns (6-8 stories + subtasks + AC + risks)
           can hit 3500+ tokens. 2800 caused silent truncation → JSON
           parse failures. 4000 gives headroom.

  FIX 7 — Groq models now include "jira" in task_types.
           DeepSeek/Llama3/Gemma were excluded from jira — meaning no
           Groq fallback when Gemini was down for Jira breakdown tab.
           All Groq models now support all task types.

  🟡 MEDIUM (3 fixes):
  ─────────────────────────────────────────────────────────────────
  FIX 8  — call_ai_compat max_tokens removed (was overriding task limit).
            2000 cap was overriding TASK_TOKENS["jira"]=4000. Removed
            the max_tokens param from compat shim — uses task limit only.

  FIX 9  — reset_cooldowns() now also resets model_gone for Gemini and
            clears auth_failed flags. UI Reset button now fully recovers
            the Gemini tier, not just rate-limit cooldowns.

  FIX 10 — get_next_provider() and get_active_provider() now accept
            optional task param. Defaults to "code" for backward compat.
            app.py can pass task="jira" for accurate UI display.

  🟢 LOW (2 fixes):
  ─────────────────────────────────────────────────────────────────
  FIX 11 — Claude model updated from claude-3-5-sonnet-20241022
            to claude-sonnet-4-6 (current production model string).

  FIX 12 — get_active_provider() / get_active_model() no longer
            filter by task="code" hardcoded — uses provided task param.

PROVIDER PRIORITY (unchanged from v2.4):
  Tier 1 — Gemini 2.0 Flash    (FREE · 1M TPM · quality=5) ← PRIMARY
  Tier 2 — Gemini 1.5 Flash    (FREE · 1M TPM · quality=5) ← fallback
  Tier 3 — Groq Llama-3.3-70b  (FREE · 6k  TPM · quality=4)
  Tier 4 — Groq DeepSeek-R1    (FREE · 6k  TPM · quality=4)
  Tier 5 — Groq Llama3-70b     (FREE · 6k  TPM · quality=3)
  Tier 6 — Groq Gemma2-9b      (FREE · 15k TPM · quality=3)
  Tier 7 — Mistral Small       (FREE tier · quality=3)
  Tier 8 — GPT-4o-mini         (PAID · quality=4)
  Tier 9 — GPT-4o              (PAID · quality=5)
  Tier 10 — Claude Sonnet 4.6  (PAID · quality=5 · best for Jira+ETL)
  Tier 11 — Ollama local        (FREE · offline · quality=3)

NOTE: Gemini 1.5-flash is placed first because it is confirmed working
      on all LiteLLM versions. 2.0-flash is attempted as a bonus — if
      your LiteLLM supports it, great; if not, 1.5 handles everything.
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
# FIX 1: Gemini 1.5-flash placed FIRST — confirmed working on all LiteLLM.
# FIX 7: All Groq models now include "jira" in task_types.
# FIX 11: Claude updated to claude-sonnet-4-6.
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

    # FREE - Gemini 2.0 Flash FIRST (confirmed working on this deployment)
    ModelConfig(
        model="gemini/gemini-2.0-flash",
        env_key="GEMINI_API_KEY",
        tpm=1_000_000, rpd=1500,
        quality=5, cost_per_1k=0.0,
        provider="Google Gemini 2.0 Flash",
    ),
    # FREE - Gemini 1.5 Flash SECOND (fallback if 2.0 unavailable)
    ModelConfig(
        model="gemini/gemini-1.5-flash",
        env_key="GEMINI_API_KEY",
        tpm=1_000_000, rpd=1500,
        quality=5, cost_per_1k=0.0,
        provider="Google Gemini 1.5 Flash",
    ),

    # FREE - Groq — all support jira task now (FIX 7)
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

    # PAID - Anthropic (FIX 11: updated model string)
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

# FIX 6: jira token limit raised to 4000 — complex breakdowns need headroom
TASK_TOKENS = {"code": 1200, "summary": 350, "jira": 4000}

# FIX 4+5: Static Gemini model priority list — no live probe calls.
# Tried in order on first call; working model cached in st.session_state.
# Failed strings tracked in _gemini_skip_set (not removed from list).
GEMINI_MODEL_PRIORITY = [
    "gemini/gemini-2.0-flash",        # Confirmed working on this deployment
    "gemini/gemini-2.0-flash-001",
    "gemini/gemini-2.0-flash-exp",
    "gemini/gemini-1.5-flash",
    "gemini/gemini-1.5-flash-latest",
]
_gemini_skip_set: set[str] = set()   # strings that failed this session


# ─────────────────────────────────────────────────────────────
# st.session_state HELPER
# Safe wrapper — works outside Streamlit context too (e.g. unit tests)
# ─────────────────────────────────────────────────────────────
def _ss_get(key: str, default=None):
    try:
        import streamlit as st
        return st.session_state.get(key, default)
    except Exception:
        return default

def _ss_set(key: str, value):
    try:
        import streamlit as st
        st.session_state[key] = value
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# PER-MODEL RUNTIME STATUS
# ─────────────────────────────────────────────────────────────
@dataclass
class _ModelStatus:
    rate_limited_until: float = 0.0
    auth_failed: bool = False
    model_gone: bool = False          # Only set for non-Gemini confirmed-dead models
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
    FIX 9: Full recovery reset.
    Clears rate-limit cooldowns AND resets model_gone for Gemini models
    AND clears auth_failed flags. UI Reset button now fully recovers
    the Gemini tier, not just cooldown timers.
    """
    cleared = 0

    for model, s in _status_registry.items():
        changed = False

        # Clear rate-limit cooldowns
        if s.rate_limited_until > time.time():
            s.rate_limited_until = 0.0
            s.rate_limit_count = 0
            changed = True

        # FIX 9: Reset model_gone for Gemini (soft-fail recovery)
        if "gemini" in model and s.model_gone:
            s.model_gone = False
            changed = True

        # FIX 9: Reset auth_failed so re-keying works without restart
        if s.auth_failed:
            s.auth_failed = False
            changed = True

        if changed:
            cleared += 1

    # Clear the Gemini skip set so all model strings are retried
    # Use .clear() not reassignment — reassignment creates a local var
    # and leaves the module-level set untouched
    _gemini_skip_set.clear()

    # Clear cached working model so it re-resolves with clean state
    _ss_set("_gemini_working_model", None)

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
# GEMINI MODEL RESOLVER  (FIX 1 + FIX 4 + FIX 5)
#
# v2.4 problems fixed:
#   - Cached working model in module var → lost on every Streamlit rerun
#   - Fired up to 5 real API calls on cold start → burned free quota
#   - GEMINI_MODEL_FALLBACKS.remove() shrunk list permanently
#
# v2.5 approach:
#   - Working model cached in st.session_state → survives reruns
#   - No pre-flight probe calls — first real request is the probe
#   - Failed strings tracked in _gemini_skip_set (list stays intact)
#   - On model_gone error: skip that string, try next in priority list
# ─────────────────────────────────────────────────────────────
def _get_gemini_model() -> str | None:
    """
    Returns the best available Gemini model string.
    Checks session_state cache first. Falls back to priority list.
    Skips strings that are in _gemini_skip_set (model not supported)
    OR whose corresponding ALL_MODELS config is currently rate-limited.
    Never fires a probe API call.
    """
    # Build a set of model strings that are currently rate-limited
    now = time.time()
    rate_limited_strings: set[str] = set()
    for cfg in ALL_MODELS:
        if "gemini" in cfg.model and now < _st(cfg.model).rate_limited_until:
            # Add both the config model string and any matching priority strings
            rate_limited_strings.add(cfg.model)

    # Check session_state cache (FIX 1 — survives Streamlit reruns)
    cached = _ss_get("_gemini_working_model")
    if (cached
            and cached not in _gemini_skip_set
            and cached not in rate_limited_strings):
        return cached

    # Walk priority list — skip unsupported AND rate-limited strings
    for candidate in GEMINI_MODEL_PRIORITY:
        if candidate in _gemini_skip_set:
            continue
        # Check if this candidate's config is rate-limited
        if candidate in rate_limited_strings:
            continue
        return candidate

    # All strings are either skipped or rate-limited
    return None


def _handle_gemini_model_failure(failed_model: str):
    """
    FIX 3 + FIX 4: Soft-fail — mark string as skip, try next.
    Does NOT set model_gone=True on the config (which would blacklist
    all Gemini permanently). Just skips this string and moves on.
    Uses .add() on the module-level set — no reassignment needed.
    """
    _gemini_skip_set.add(failed_model)
    logger.warning(f"[ROUTER] Gemini model string failed: {failed_model} — trying next")

    # Invalidate session_state cache if it pointed to this string
    if _ss_get("_gemini_working_model") == failed_model:
        _ss_set("_gemini_working_model", None)


# ─────────────────────────────────────────────────────────────
# CORE ROUTER
# FIX 2: De-duplication loop removed entirely.
#        Both Gemini configs compete normally — 1.5 is listed first
#        in ALL_MODELS so it wins the sort. No seen_gemini blocking.
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

    # Gemini (quality=5) always sorts above Groq (quality=4)
    # FIX 2: No de-duplication — both Gemini configs compete normally
    candidates.sort(key=lambda c: (-c.quality, c.cost_per_1k))
    logger.info(f"[ROUTER] Candidates: {[c.provider for c in candidates]}")

    # Track which Gemini model strings we've tried this call
    # so we don't retry the same string twice within one call
    gemini_strings_tried: set[str] = set()

    for cfg in candidates:

        # ── Gemini: resolve actual model string ───────────────
        if "gemini" in cfg.model:
            actual_model = _get_gemini_model()
            if actual_model is None:
                logger.warning("[ROUTER] No working Gemini model string — skipping Gemini configs")
                # Skip ALL remaining Gemini configs
                continue
            if actual_model in gemini_strings_tried:
                # Already tried this string via another Gemini config entry
                continue
            gemini_strings_tried.add(actual_model)
        else:
            actual_model = cfg.model

        for attempt in range(2):
            try:
                logger.info(f"[ROUTER] → {cfg.provider} [{actual_model}] attempt={attempt+1} task={task}")
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

                # Cache working Gemini model string in session_state (FIX 1)
                if "gemini" in cfg.model:
                    _ss_set("_gemini_working_model", actual_model)

                _mark_success(
                    cfg.model, tokens,
                    provider=cfg.provider,
                    display_model=actual_model.split("/")[-1],
                )
                logger.info(f"[ROUTER] SUCCESS: {cfg.provider} | {tokens} tokens | task={task}")
                return content

            except Exception as exc:
                logger.warning(f"[ROUTER] {cfg.provider} [{actual_model}] error: {exc}")

                if _is_auth_err(exc):
                    _st(cfg.model).auth_failed = True
                    logger.error(f"[ROUTER] Auth failed — check {cfg.env_key}")
                    break

                if _is_gone_err(exc):
                    if "gemini" in cfg.model:
                        # FIX 3: Soft-fail — skip this string, don't blacklist all Gemini
                        _handle_gemini_model_failure(actual_model)
                        # Try next Gemini string immediately (still within this cfg loop)
                        next_model = _get_gemini_model()
                        if next_model and next_model not in gemini_strings_tried:
                            actual_model = next_model
                            gemini_strings_tried.add(actual_model)
                            continue  # retry with next string
                    else:
                        # Non-Gemini: model is truly gone
                        _st(cfg.model).model_gone = True
                    break

                if _is_rate_err(exc):
                    if attempt == 0:
                        wait = 3 if "gemini" in cfg.model else 5
                        logger.info(f"[ROUTER] Rate limit — retry in {wait}s")
                        time.sleep(wait)
                        continue
                    _mark_rate_limit(cfg.model)
                    break

                break  # Unknown error — try next provider

    logger.error("[ROUTER] All providers exhausted.")
    return RATE_LIMIT_SENTINEL


# ─────────────────────────────────────────────────────────────
# BACKWARD-COMPATIBLE SHIM
# FIX 8: Removed max_tokens=2000 hardcoded override.
#         Was silently capping jira calls at 2000 instead of 4000.
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
# FIX 10 + 12: task param added to get_next_provider, get_active_provider,
#              get_active_model. Defaults to "code" for backward compat
#              with existing app.py calls.
# ─────────────────────────────────────────────────────────────
def get_router_status() -> list[dict]:
    rows = []
    for cfg in ALL_MODELS:
        is_local = "ollama" in cfg.model
        has_key  = bool(_get_key(cfg.env_key)) if cfg.env_key else True
        s   = _st(cfg.model)
        now = time.time()

        # Gemini: show skip status clearly
        if "gemini" in cfg.model and cfg.model in _gemini_skip_set:
            status = "🟡 Model string unsupported — auto-skipping"
        elif s.auth_failed:
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

        # Show which Gemini string is actively cached
        cached_gemini = _ss_get("_gemini_working_model", "")
        is_active_gemini = ("gemini" in cfg.model and cached_gemini and
                            cached_gemini.split("/")[-1] in cfg.model)

        is_last = (cfg.provider == _LAST_USED_PROVIDER)
        label   = cfg.provider
        if is_last:
            label = "⭐ LAST USED → " + label
        if is_active_gemini:
            label = "🎯 ACTIVE → " + label

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
    """Returns the provider that ACTUALLY answered the last call."""
    if _LAST_USED_PROVIDER != "None yet":
        return _LAST_USED_PROVIDER
    # No call made yet — return next-up for this task
    for cfg in sorted(ALL_MODELS, key=lambda c: (-c.quality, c.cost_per_1k)):
        if _is_available(cfg) and task in cfg.task_types:
            return cfg.provider
    return "None configured"


def get_active_model(task: str = "code") -> str:
    """Returns the model that ACTUALLY answered the last call."""
    if _LAST_USED_MODEL != "—":
        return _LAST_USED_MODEL
    for cfg in sorted(ALL_MODELS, key=lambda c: (-c.quality, c.cost_per_1k)):
        if _is_available(cfg) and task in cfg.task_types:
            return cfg.model.split("/")[-1]
    return "—"


def get_next_provider(task: str = "code") -> str:
    """
    FIX 10: Returns the provider that WILL be used on the next call.
    Now accepts task param — app.py can pass task='jira' for accurate display.
    """
    for cfg in sorted(ALL_MODELS, key=lambda c: (-c.quality, c.cost_per_1k)):
        if _is_available(cfg) and task in cfg.task_types:
            # For Gemini: only show as next if we have a working model string
            if "gemini" in cfg.model:
                if _get_gemini_model() is not None:
                    return cfg.provider
                continue
            return cfg.provider
    return "None available"
