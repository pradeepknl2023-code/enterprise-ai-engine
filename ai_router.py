"""
ai_router.py  ·  LiteLLM Multi-Provider Router  ·  v2.3
=========================================================
FIXES in v2.3:
  ─────────────────────────────────────────────────────
  BUG (v2.2 and earlier):
    Gemini 2.0 Flash had quality=4, same as Groq Llama-3.3-70b.
    Both had cost_per_1k=0.0.
    Sort key: (-quality, cost_per_1k) → PERFECT TIE.
    Python stable sort preserves list order, which should favour
    Gemini (listed first), BUT the filtered candidate list order
    is non-deterministic across Python versions / environments.
    Result: Groq sometimes won the tie and was selected as primary.

  FIX (v2.3):
    Gemini 2.0 Flash  → quality = 5  (was 4)
    Gemini 1.5 Flash  → quality = 5  (was 4)
    This gives Gemini a strict sort advantage over all Groq models
    (quality=4) regardless of cost, list order, or env differences.
    Gemini will ALWAYS be selected when its key is present and valid.
  ─────────────────────────────────────────────────────

ROOT CAUSE FIX from v2.2 (retained):
  _get_key() reads st.secrets DIRECTLY as a fallback.
  Two-layer lookup:
    Layer 1: os.environ   (local dev + works if sync succeeded)
    Layer 2: st.secrets   (bulletproof for Streamlit Cloud)
  ─────────────────────────────────────────────────────

PROVIDER PRIORITY (auto-detected from available keys):
  Tier 1 — Gemini 2.0 Flash    (FREE · 1M TPM · quality=5) ← PRIMARY
  Tier 2 — Gemini 1.5 Flash    (FREE · 1M TPM · quality=5) ← BACKUP
  Tier 3 — Groq Llama-3.3-70b  (FREE · 6k TPM · quality=4)
  Tier 4 — Groq DeepSeek-R1    (FREE · 6k TPM · quality=4)
  Tier 5 — Groq Llama3-70b     (FREE · 6k TPM · quality=3)
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
# ✅ BULLETPROOF KEY LOOKUP — Layer 1: os.environ, Layer 2: st.secrets
# ─────────────────────────────────────────────────────────────
def _get_key(env_key: str) -> str:
    """
    Read an API key from os.environ first, then st.secrets as fallback.
    This makes the router work on Streamlit Cloud even if the secrets
    sync block in app.py silently failed.
    """
    if not env_key:
        return ""

    # Layer 1 — os.environ (local dev / after successful sync)
    val = os.environ.get(env_key, "").strip()
    if val:
        return val

    # Layer 2 — st.secrets direct (Streamlit Cloud bulletproof fallback)
    try:
        import streamlit as st
        val = str(st.secrets.get(env_key, "")).strip()
        if val:
            # Also write to os.environ so future calls skip Layer 2
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

    # ── FREE — Gemini PRIMARY (1M TPM) ──────────────────────
    # ✅ v2.3 FIX: quality bumped to 5 (was 4) to guarantee sort priority over Groq (quality=4)
    ModelConfig(
        model="gemini/gemini-2.0-flash",
        env_key="GEMINI_API_KEY",
        tpm=1_000_000, rpd=1500,
        quality=5,           # ← FIXED: was 4, now 5 → always beats Groq in sort
        cost_per_1k=0.0,
        provider="Google Gemini 2.0 Flash",
    ),
    ModelConfig(
        model="gemini/gemini-1.5-flash",
        env_key="GEMINI_API_KEY",
        tpm=1_000_000, rpd=1500,
        quality=5,           # ← FIXED: was 4, now 5 → always beats Groq in sort
        cost_per_1k=0.0,
        provider="Google Gemini 1.5 Flash",
    ),

    # ── FREE — Groq FALLBACK ─────────────────────────────────
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

    # ── FREE — Mistral ───────────────────────────────────────
    ModelConfig(
        model="mistral/mistral-small-latest",
        env_key="MISTRAL_API_KEY",
        tpm=500_000, rpd=0,
        quality=3, cost_per_1k=0.0,
        provider="Mistral Small",
        task_types=["code", "summary"],
    ),

    # ── PAID — OpenAI ────────────────────────────────────────
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

    # ── PAID — Anthropic ─────────────────────────────────────
    ModelConfig(
        model="claude-3-5-sonnet-20241022",
        env_key="ANTHROPIC_API_KEY",
        tpm=200_000, rpd=0,
        quality=5, cost_per_1k=0.003,
        provider="Anthropic · Claude Sonnet",
    ),

    # ── LOCAL — Ollama ───────────────────────────────────────
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

_status_registry: dict[str, _ModelStatus] = {}

def _st(model: str) -> _ModelStatus:
    if model not in _status_registry:
        _status_registry[model] = _ModelStatus()
    return _status_registry[model]


# ─────────────────────────────────────────────────────────────
# AVAILABILITY CHECK — uses _get_key() not os.environ directly
# ─────────────────────────────────────────────────────────────
def _is_available(cfg: ModelConfig) -> bool:
    s = _st(cfg.model)
    if s.auth_failed or s.model_gone:
        return False
    if time.time() < s.rate_limited_until:
        return False
    # ✅ Uses bulletproof _get_key() — reads st.secrets if os.environ empty
    if cfg.env_key and not _get_key(cfg.env_key):
        return False
    if "ollama" in cfg.model and not _get_key("OLLAMA_ENABLED"):
        return False
    return True

def _mark_rate_limit(model: str, backoff: int = 60):
    _st(model).rate_limited_until = time.time() + backoff
    logger.warning(f"[ROUTER] {model} → cooldown {backoff}s")

def _mark_success(model: str, tokens: int = 0):
    s = _st(model)
    s.total_calls += 1
    s.total_tokens += tokens
    s.last_used_ts = time.time()
    s.rate_limited_until = 0.0


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
        "401", "403", "invalid_api_key",
        "authentication", "unauthorized", "permission_denied",
    ])

def _is_gone_err(exc: Exception) -> bool:
    s = str(exc).lower()
    return any(x in s for x in [
        "decommissioned", "not supported", "no longer supported",
        "model_not_found", "does not exist", "404",
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
    try:
        import litellm
        litellm.drop_params = True
        litellm.set_verbose = False
    except ImportError:
        raise RuntimeError("litellm not installed → pip install litellm")

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

    # Sort: highest quality first → lowest cost first (stable = preserves list order for ties)
    # v2.3: Gemini quality=5 > Groq quality=4, so Gemini always wins when key is present
    candidates.sort(key=lambda c: (-c.quality, c.cost_per_1k))

    logger.info(f"[ROUTER] Priority order: {[c.provider for c in candidates]}")

    for cfg in candidates:
        for attempt in range(2):
            try:
                logger.info(f"[ROUTER] → {cfg.provider} (attempt {attempt+1})")
                kwargs = {}
                if cfg.env_key:
                    # ✅ Use _get_key() — bulletproof st.secrets fallback
                    kwargs["api_key"] = _get_key(cfg.env_key)

                resp = litellm.completion(
                    model=cfg.model,
                    messages=messages,
                    temperature=temperature,
                    max_tokens=token_limit,
                    **kwargs,
                )
                content = resp.choices[0].message.content or ""
                tokens  = getattr(resp.usage, "total_tokens", 0)
                _mark_success(cfg.model, tokens)
                logger.info(f"[ROUTER] ✅ {cfg.provider} · {tokens} tokens")
                return content

            except Exception as exc:
                logger.warning(f"[ROUTER] {cfg.provider} error: {exc}")
                if _is_auth_err(exc):
                    _st(cfg.model).auth_failed = True
                    logger.error(f"[ROUTER] 🔑 Auth failed — check {cfg.env_key}")
                    break
                if _is_gone_err(exc):
                    _st(cfg.model).model_gone = True
                    break
                if _is_rate_err(exc):
                    if attempt == 0:
                        wait = 8 if "gemini" in cfg.model else 12
                        logger.info(f"[ROUTER] ⏳ Rate limit — retry in {wait}s")
                        time.sleep(wait)
                        continue
                    _mark_rate_limit(cfg.model, 60)
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
# UI HELPERS — uses _get_key() so status reflects reality
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
            status = f"🟡 Cooldown {int(s.rate_limited_until - now)}s"
        else:
            status = "🟢 Ready"

        rows.append({
            "Provider": cfg.provider,
            "Model":    cfg.model.split("/")[-1],
            "Cost":     "FREE" if cfg.cost_per_1k == 0 else f"${cfg.cost_per_1k:.4f}/1k",
            "TPM":      f"{cfg.tpm:,}",
            "Quality":  "★" * cfg.quality + "☆" * (5 - cfg.quality),
            "Status":   status,
            "Calls":    s.total_calls,
            "Tokens":   f"{s.total_tokens:,}",
        })
    return rows


def get_active_provider() -> str:
    for cfg in sorted(ALL_MODELS, key=lambda c: (-c.quality, c.cost_per_1k)):
        if _is_available(cfg) and "code" in cfg.task_types:
            return cfg.provider
    return "None configured"


def get_active_model() -> str:
    for cfg in sorted(ALL_MODELS, key=lambda c: (-c.quality, c.cost_per_1k)):
        if _is_available(cfg) and "code" in cfg.task_types:
            return cfg.model.split("/")[-1]
    return "—"
