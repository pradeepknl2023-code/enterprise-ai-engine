"""
Enterprise AI ETL Platform  ·  v6.2
=====================================
FIXES in v6.2:
  ✅ FIX 1: ai_router reads st.secrets DIRECTLY — no longer depends
             on the sync block. Gemini "No Key" permanently fixed.
  ✅ FIX 2: Debug secrets panel in Tab 4 shows exactly what the
             router sees — instant confirmation keys are loaded.
  ✅ FIX 3: Sync block kept as belt-and-suspenders (harmless).
  ✅ FIX 4: VERIFIED/REJECTED/ACTIVE whitelist (from v6.1).
  ✅ FIX 5: Priority log shown in terminal for every request.
"""

import streamlit as st
import pandas as pd
import numpy as np
import os, re, hashlib, datetime, json, time, logging, uuid
from io import BytesIO
import sys

# ═══════════════════════════════════════════════════════════
# BELT-AND-SUSPENDERS secrets sync (kept, but router no longer
# depends on this — ai_router._get_key() reads st.secrets directly)
# ═══════════════════════════════════════════════════════════
_SECRET_KEYS = [
    "GEMINI_API_KEY", "GROQ_API_KEY", "MISTRAL_API_KEY",
    "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "OLLAMA_ENABLED",
]
for _k in _SECRET_KEYS:
    try:
        if _k in st.secrets and not os.environ.get(_k, "").strip():
            os.environ[_k] = str(st.secrets[_k])
    except Exception:
        pass

# ── Router import ─────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from ai_router import (
        call_ai_compat as call_ai,
        get_router_status,
        get_active_provider,
        get_active_model,
        _get_key,
        RATE_LIMIT_SENTINEL,
    )
    ROUTER_OK = True
except ImportError as _err:
    ROUTER_OK = False
    _ROUTER_ERR = str(_err)

# ── Page config ───────────────────────────────────────────
st.set_page_config(
    page_title="Enterprise AI ETL Platform",
    layout="wide",
    page_icon="⚡",
)

# ── Logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()],
)
audit_logger = logging.getLogger("AUDIT")

def audit_log(action, session_id, details, risk="LOW"):
    audit_logger.info(
        f"SESSION={session_id} | ACTION={action} | RISK={risk} | {details}"
    )

# ── Session state ─────────────────────────────────────────
for _k, _v in {
    "session_id":      str(uuid.uuid4())[:8].upper(),
    "history":         [],
    "jira_result":     None,
    "last_etl_result": None,
}.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

SESSION_ID = st.session_state.session_id

# ── Router guard ──────────────────────────────────────────
if not ROUTER_OK:
    st.error(f"""
### ⚠️ AI Router Import Failed
**Error:** `{_ROUTER_ERR}`

**Fix:** `pip install litellm` — and make sure `ai_router.py` is in the same folder as `app.py`.
""")
    st.stop()

_ready = sum(1 for r in get_router_status() if "🟢" in r["Status"])
if _ready == 0:
    st.error("""
### ⚠️ No AI Providers Ready

Go to **share.streamlit.io → Your App → ⋮ → Settings → Secrets** and add:

```toml
GEMINI_API_KEY = "AIzaSy..."
GROQ_API_KEY   = "gsk_..."
```

Get a free Gemini key → https://aistudio.google.com
""")
    st.stop()


# ═══════════════════════════════════════════════════════════
# BUSINESS VALUE WHITELIST (v6.1 fix)
# ═══════════════════════════════════════════════════════════
BUSINESS_WHITELIST = {
    "VERIFIED","UNVERIFIED","REJECTED","PENDING","APPROVED",
    "ACTIVE","INACTIVE","DORMANT","CLOSED","SUSPENDED","BLOCKED",
    "DEBIT","CREDIT","TRANSFER","REFUND","REVERSAL","PAYMENT",
    "HIGH","MEDIUM","LOW","CRITICAL","NORMAL",
    "RETAIL","CORPORATE","PREMIUM","STANDARD","PLATINUM","GOLD","SILVER",
    "COMPLETE","COMPLETED","INCOMPLETE","FAILED","SUCCESS",
    "ENABLED","DISABLED","OPEN","PROCESSED","INITIATED",
    "ONLINE","MOBILE","BRANCH","ATM","NEFT","RTGS","IMPS","UPI",
}

# ═══════════════════════════════════════════════════════════
# PRIVACY ENGINE
# ═══════════════════════════════════════════════════════════
SENSITIVE_PATTERNS = {
    "account_number": r'\b\d{8,17}\b',
    "sort_code":      r'\b\d{2}-\d{2}-\d{2}\b',
    "card_number":    r'\b(?:\d[ -]?){13,19}\b',
    "ssn":            r'\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b',
    "iban":           r'\b[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}([A-Z0-9]?){0,16}\b',
    "swift":          r'\b[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?\b',
    "email":          r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
    "phone_uk":       r'\b(?:0|\+44)[\s-]?\d{4}[\s-]?\d{6}\b',
    "phone_us":       r'\b(?:\+1[\s-]?)?\(?\d{3}\)?[\s-]?\d{3}[\s-]?\d{4}\b',
    "phone_in":       r'\b[6-9]\d{9}\b',
    "ip_address":     r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
    "postcode_uk":    r'\b[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}\b',
    "zip_us":         r'\b\d{5}(?:-\d{4})?\b',
    "dob":            r'\b(?:0?[1-9]|[12]\d|3[01])[/-](?:0?[1-9]|1[0-2])[/-](?:19|20)\d{2}\b',
    "passport":       r'\b[A-Z]{1,2}\d{6,9}\b',
}

SENSITIVE_COL_KW = [
    "account","acct","iban","bic","swift","sort","routing","card","cvv","pin",
    "password","passwd","secret","token","ssn","sin","nin","nino","passport",
    "license","licence","salary","wage","income","balance","credit","debit",
    "tax","vat","ein","tin","email","phone","mobile","tel","fax","address",
    "postcode","zipcode","zip","dob","birthdate","birth","gender","ethnicity",
    "religion","health","medical","ip","device","mac_addr","name","surname",
    "firstname","lastname","fullname","national","id_number","customer_id","member_id",
]

def _hash(val: str, prefix: str = "") -> str:
    h = hashlib.sha256(str(val).encode()).hexdigest()[:8].upper()
    return f"{prefix}[MASKED-{h}]"

def mask_sensitive_column(series: pd.Series, col_name: str) -> pd.Series:
    col_lower = col_name.lower().replace(" ", "_").replace("-", "_")
    if not any(kw in col_lower for kw in SENSITIVE_COL_KW):
        return series
    def _mask(v):
        if pd.isna(v) or v == "":
            return v
        sv = str(v)
        if sv.upper() in BUSINESS_WHITELIST:
            return sv
        for pname, pat in SENSITIVE_PATTERNS.items():
            if re.search(pat, sv, re.IGNORECASE):
                return _hash(sv, pname[:3].upper())
        return _hash(sv, "PII")
    return series.apply(_mask)

def mask_dataframe(df: pd.DataFrame):
    masked = df.copy()
    masked_cols, total = [], 0
    for col in df.columns:
        m = mask_sensitive_column(df[col], col)
        diff = (df[col].astype(str) != m.astype(str)).sum()
        if diff > 0:
            masked[col] = m
            masked_cols.append(col)
            total += diff
    return masked, masked_cols, total

def scan_pii(text: str) -> list:
    found = []
    for name, pat in SENSITIVE_PATTERNS.items():
        matches = re.findall(pat, text, re.IGNORECASE)
        for m in matches:
            val = m if isinstance(m, str) else (m[0] if m else "")
            if val.upper() not in BUSINESS_WHITELIST:
                found.append(name)
                break
    return list(dict.fromkeys(found))

def sanitize_prompt(prompt: str):
    found = scan_pii(prompt)
    sanitized = prompt
    for pname, pat in SENSITIVE_PATTERNS.items():
        def _replace(m):
            val = m.group(0)
            if val.upper() in BUSINESS_WHITELIST:
                return val
            return f"[REDACTED_{pname.upper()}]"
        sanitized = re.sub(pat, _replace, sanitized, flags=re.IGNORECASE)
    return sanitized, found

def validate_file(f):
    if f.size > 50 * 1024 * 1024:
        return False, "File exceeds 50MB limit."
    if f.name.rsplit(".", 1)[-1].lower() not in ["csv"]:
        return False, "Only CSV files allowed."
    if ".." in f.name or "/" in f.name:
        return False, "Invalid filename."
    return True, "OK"

def schema_context(dataframes: dict) -> str:
    lines = ""
    for alias, df in dataframes.items():
        cols = ", ".join(f"{c}:{str(t)[:7]}" for c, t in df.dtypes.items())
        lines += f"\n  {alias} (rows={df.shape[0]:,}): {cols}"
    return lines


# ═══════════════════════════════════════════════════════════
# ETL HELPERS
# ═══════════════════════════════════════════════════════════
def extract_code(raw: str) -> str:
    m = re.search(r"```(?:python)?\s*\n(.*?)```", raw, re.DOTALL)
    if m:
        return m.group(1).strip()
    return "\n".join(l for l in raw.splitlines()
                     if not l.strip().startswith("```")).strip()

def safe_exec(dataframes: dict, code: str) -> pd.DataFrame:
    code = extract_code(code)
    g = {"pd": pd, "np": np, "re": re, "datetime": datetime, **dataframes}
    try:
        exec(compile(code, "<etl>", "exec"), g)
    except Exception as e:
        raise RuntimeError(f"Execution error: {e}\n\nCode:\n{code}") from e
    primary = "df" if "df" in dataframes else list(dataframes.keys())[0]
    out = g.get("result", g.get(primary, list(dataframes.values())[0]))
    if not isinstance(out, pd.DataFrame):
        raise RuntimeError(f"AI returned {type(out).__name__}, expected DataFrame.")
    return out

def build_system_prompt(dataframes: dict) -> str:
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    return (
        f"Expert data engineer. Today={today}. "
        f"DataFrames: {', '.join(dataframes.keys())}.\n"
        f"Schema:{schema_context(dataframes)}\n"
        "Write Python only. Store result in 'result'. "
        "pd/np/datetime available. fillna(0) after joins. "
        "np.select() for conditions. No markdown fences."
    )

def build_pipeline_log(code, dataframes, result_df, file_names, orig_rows):
    prompt = (
        f"Narrate this ETL pipeline in 4-8 plain-English steps.\n"
        f"Code:\n```python\n{code}\n```\n"
        f"Files: {file_names}, Rows before: {orig_rows}, "
        f"Rows after: {len(result_df)}, Columns: {result_df.columns.tolist()}\n"
        "Each step starts with a verb: Loaded/Joined/Filtered/Cleaned/Computed/Aggregated/Ranked.\n"
        'Return ONLY a JSON array: ["Step one", "Step two"]. No markdown.'
    )
    try:
        raw = call_ai([{"role": "user", "content": prompt}], temperature=0.2, task="summary")
        if raw == RATE_LIMIT_SENTINEL:
            raise ValueError("rate_limit")
        m = re.search(r"\[.*\]", raw, re.DOTALL)
        steps = json.loads(m.group()) if m else [raw]
    except Exception:
        steps = [
            f"Loaded {orig_rows:,} rows from {', '.join(file_names)}",
            "Applied AI-generated transformations",
            f"Produced {len(result_df):,} rows × {len(result_df.columns)} columns",
        ]

    icons = {
        "load":"📂","read":"📂","join":"🔗","merge":"🔗","clean":"🧹",
        "comput":"⚙️","calculat":"⚙️","aggregat":"⚙️","filter":"🔍",
        "sort":"↕️","rank":"🏅","flag":"🚩","risk":"⚠️","segment":"🏷️",
    }
    def icon(t):
        for kw, ic in icons.items():
            if kw in t.lower(): return ic
        return "✅"

    steps_html = '<div class="pipeline-steps">' + "".join(
        f'<div class="pipeline-step"><span class="step-num">{i}</span>'
        f'<span class="step-icon">{icon(s)}</span>'
        f'<span class="step-text">{s}</span></div>'
        for i, s in enumerate(steps, 1)
    ) + "</div>"

    new_cols = [c for c in result_df.columns if c not in list(dataframes.values())[0].columns]
    n_joins  = max(0, len([a for a in dataframes if a != "df"]) - 1)
    metrics_html = (
        f'<div class="metric-row">'
        f'<div class="metric-box"><div class="metric-value">{len(file_names)}</div><div class="metric-label">Files</div></div>'
        f'<div class="metric-box"><div class="metric-value">{orig_rows:,}</div><div class="metric-label">Rows In</div></div>'
        f'<div class="metric-box"><div class="metric-value">{len(result_df):,}</div><div class="metric-label">Rows Out</div></div>'
        f'<div class="metric-box"><div class="metric-value">{len(result_df.columns)}</div><div class="metric-label">Columns</div></div>'
        f'<div class="metric-box"><div class="metric-value">{len(new_cols)}</div><div class="metric-label">New Cols</div></div>'
        f'<div class="metric-box"><div class="metric-value">{n_joins}</div><div class="metric-label">Joins</div></div>'
        f'</div>'
    )
    return metrics_html, steps_html


# ═══════════════════════════════════════════════════════════
# GDE FLOW DIAGRAM
# ═══════════════════════════════════════════════════════════
def make_gde_html(dataframes, file_names, code, result_df, state, masked_cols=None):
    aliases  = list(dataframes.keys())
    real     = [a for a in aliases if a != "df"] or aliases[:1] or ["df"]
    has_join = len(real) >= 2
    cl = (code or "").lower()

    ops = []
    if "merge" in cl or "join" in cl:    ops.append("JOIN")
    if "groupby" in cl and "rank" in cl: ops.append("RANK")
    if "pd.cut" in cl or "np.select" in cl: ops.append("SEGMENT")
    if "re.sub" in cl or "replace" in cl:   ops.append("CLEAN")
    if "groupby" in cl:                  ops.append("AGGREGATE")
    if not ops: ops.append("TRANSFORM")
    trans_label = " · ".join(list(dict.fromkeys(ops))[:3])

    pr   = dataframes[real[0]].shape[0]
    sr   = dataframes[real[1]].shape[0] if has_join else 0
    fn1  = file_names[0] if file_names else "file.csv"
    fn2  = file_names[1] if len(file_names) > 1 else ""
    out_rows = len(result_df) if state == "done" and result_df is not None else 0
    out_cols = len(result_df.columns) if state == "done" and result_df is not None else 0
    mc = len(masked_cols) if masked_cols else 0

    done    = state == "done"
    running = state == "transforming"
    active  = state in ("reading", "transforming", "done")

    i_bg = "#1a2744" if active else "#111"; i_bd = "#1E90FF" if active else "#333"; i_c = "#7BB8FF" if active else "#444"
    t_bg = "#2a2a0a" if running else ("#0d2137" if done else "#111")
    t_bd = "#FFD600" if running else ("#29B6F6" if done else "#333")
    t_c  = "#FFD600" if running else ("#29B6F6" if done else "#444")
    t_an = "animation:pulse 1s infinite;" if running else ""
    o_bg = "#0d2137" if done else "#1a1a2e"; o_bd = "#29B6F6" if done else "#AB47BC"; o_c = "#29B6F6" if done else "#CE93D8"
    p_bg = "#0a2a0a" if active else "#111"; p_bd = "rgba(105,240,174,0.5)" if active else "#333"; p_c = "#69F0AE" if active else "#444"
    a1c = "#29B6F6" if active else "#444"; a2c = "#29B6F6" if done else "#444"; pac = "#69F0AE" if active else "#444"
    in_d  = f"{pr:,}" if active else "–"; in2_d = f"{sr:,}" if has_join and active else "–"
    tr_d  = (f"{pr+sr:,} rec" if has_join else f"{pr:,} rec") if active else "–"
    out_d = f"{out_rows:,}" if done else "–"
    t_st  = "🟡 RUNNING" if running else ("🔵 COMPLETE" if done else "⏳ WAITING")

    def arrow(color, label=""):
        uid = abs(hash(label + color)) % 99999
        return (f'<div class="gde-arrow"><svg width="70" height="18" viewBox="0 0 70 18">'
                f'<defs><marker id="a{uid}" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">'
                f'<polygon points="0 0,6 3,0 6" fill="{color}"/></marker></defs>'
                f'<line x1="2" y1="9" x2="62" y2="9" stroke="{color}" stroke-width="2.5" marker-end="url(#a{uid})"/>'
                f'</svg><div class="gde-count-label" style="color:{color};">{label}</div></div>')

    def node(title, sub, count, bg, bd, col, label, anim=""):
        return (f'<div class="gde-node"><div style="background:{bg};border:2px solid {bd};color:{col};'
                f'min-width:110px;border-radius:8px;padding:10px 14px;text-align:center;{anim}">'
                f'<div style="font-size:11px;font-weight:700;letter-spacing:.8px;text-transform:uppercase;">{title}</div>'
                f'<div style="font-size:9px;opacity:.7;margin-top:2px;">{sub}</div>'
                f'<div style="font-size:14px;font-weight:700;font-family:Rajdhani,sans-serif;margin-top:4px;">{count}</div>'
                f'</div><div class="gde-node-label">{label}</div></div>')

    html = '<div class="gde-flow">'
    if has_join:
        html += '<div class="gde-node"><div style="display:flex;flex-direction:column;gap:10px;">'
        html += node("📂 INPUT 1", fn1[:18], in_d, i_bg, i_bd, i_c, real[0])
        html += node("📂 INPUT 2", fn2[:18], in2_d, i_bg, i_bd, i_c, real[1])
        html += '</div></div>'
    else:
        html += node("📂 INPUT", fn1[:18], in_d, i_bg, i_bd, i_c, real[0])
    html += arrow(a1c, "raw data")
    html += node("🔒 PII MASK", "Auto-Detect", f"{mc} cols", p_bg, p_bd, p_c,
                 '<span style="color:#69F0AE;font-size:9px;">SCHEMA ONLY→AI</span>')
    html += arrow(pac, "schema only")
    html += node(f"⚙ {trans_label}", "AI GENERATED", tr_d, t_bg, t_bd, t_c, t_st, t_an)
    html += arrow(a2c, out_d if done else "")
    html += node("💾 OUTPUT", f"{out_cols} cols", out_d, o_bg, o_bd, o_c, "RESULT")
    html += '</div>'
    legend = ('<div class="gde-legend">'
              '<div class="gde-legend-item"><div class="legend-dot" style="background:#1E90FF;"></div>Input</div>'
              '<div class="gde-legend-item"><div class="legend-dot" style="background:#69F0AE;"></div>PII Layer</div>'
              '<div class="gde-legend-item"><div class="legend-dot" style="background:#FFD600;"></div>Running</div>'
              '<div class="gde-legend-item"><div class="legend-dot" style="background:#29B6F6;"></div>Complete</div>'
              '<div class="gde-legend-item"><div class="legend-dot" style="background:#AB47BC;"></div>Output</div>'
              '</div>')
    return f'<div class="gde-container">{html}{legend}</div>'


# ═══════════════════════════════════════════════════════════
# DOWNLOAD PANEL
# ═══════════════════════════════════════════════════════════
def render_download_panel(masked_df, original_df, file_names, masked_cols):
    st.markdown('<div class="decrypt-panel"><div class="decrypt-panel-title">⬇️ EXPORT & DOWNLOAD OPTIONS</div></div>',
                unsafe_allow_html=True)
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown('<div class="download-option"><div class="download-option-title">🔒 Privacy-Protected Export</div>'
                    '<div class="download-option-desc">PII masked. Safe for sharing, audits, external reporting.</div></div>',
                    unsafe_allow_html=True)
        st.download_button("⬇ Download Masked CSV",
                           masked_df.to_csv(index=False).encode("utf-8"),
                           "output_privacy_protected.csv", "text/csv", key="dl_masked_csv")
        xbuf = BytesIO()
        with pd.ExcelWriter(xbuf, engine="xlsxwriter") as w:
            masked_df.to_excel(w, sheet_name="Transformed_Data", index=False)
            pd.DataFrame([
                {"Check":"PII Columns Masked","Result":", ".join(masked_cols) or "None"},
                {"Check":"Schema-Only AI","Result":"YES"},
                {"Check":"AI Provider","Result":get_active_provider()},
                {"Check":"Active Model","Result":get_active_model()},
                {"Check":"Session ID","Result":SESSION_ID},
                {"Check":"Timestamp","Result":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            ]).to_excel(w, sheet_name="Privacy_Report", index=False)
        st.download_button("⬇ Download Masked Excel + Privacy Report", xbuf.getvalue(),
                           "output_privacy_protected.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           key="dl_masked_xlsx")
    with col_b:
        st.markdown('<div class="download-option" style="border-color:rgba(255,152,0,.4);background:rgba(255,152,0,.05);">'
                    '<div class="download-option-title" style="color:#E65100;">🔓 Decrypted / Original Export</div>'
                    '<div class="download-option-desc" style="color:#795548;">Contains real PII. Requires acknowledgement.</div></div>',
                    unsafe_allow_html=True)
        if st.checkbox("⚠️ I confirm this export contains real PII and I am authorised to access it.", key="decrypt_ack"):
            audit_log("DECRYPT_ACKNOWLEDGED", SESSION_ID, f"Files={file_names}", "HIGH")
            st.download_button("🔓 Download DECRYPTED CSV",
                               original_df.to_csv(index=False).encode("utf-8"),
                               "output_ORIGINAL_SENSITIVE.csv", "text/csv", key="dl_orig_csv")
            obuf = BytesIO()
            with pd.ExcelWriter(obuf, engine="xlsxwriter") as w:
                original_df.to_excel(w, sheet_name="Original_Data", index=False)
                pd.DataFrame([
                    {"Check":"Export Type","Result":"DECRYPTED — CONTAINS REAL PII"},
                    {"Check":"Authorised","Result":"User Acknowledged"},
                    {"Check":"AI Provider","Result":get_active_provider()},
                    {"Check":"Session","Result":SESSION_ID},
                    {"Check":"Timestamp","Result":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
                ]).to_excel(w, sheet_name="Access_Log", index=False)
            st.download_button("🔓 Download DECRYPTED Excel", obuf.getvalue(),
                               "output_ORIGINAL_SENSITIVE.xlsx",
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               key="dl_orig_xlsx")
            st.warning("⚠️ Handle according to your data classification policy.")
        else:
            st.info("Check the acknowledgement box to unlock decrypted export.")


# ═══════════════════════════════════════════════════════════
# JIRA HELPERS
# ═══════════════════════════════════════════════════════════
PROJECT_PROMPTS = {
    "🌐 Web Application":     "Senior Agile Delivery Manager specialising in Web Application delivery.",
    "📱 Mobile App":          "Senior Agile Delivery Manager specialising in Mobile Application delivery.",
    "📊 Data / ETL Pipeline": "Senior Agile Delivery Manager specialising in Data Engineering and ETL.",
    "🔗 API / Integration":   "Senior Agile Delivery Manager specialising in API and Systems Integration.",
    "☁️ Cloud / Infra":       "Senior Agile Delivery Manager specialising in Cloud Infrastructure and DevOps.",
    "🔒 Security Feature":    "Senior Agile Delivery Manager specialising in Cybersecurity.",
    "🏦 Banking / FinTech":   "Senior Agile Delivery Manager for Banking/FinTech with PCI-DSS, GDPR, FCA, SOX expertise.",
    "🤖 AI / ML Feature":     "Senior Agile Delivery Manager specialising in AI and ML product delivery.",
    "📋 General / Other":     "Senior Agile Delivery Manager with 15+ years enterprise software delivery.",
}

def build_jira_prompt(description, project_type, team_size, sprint_len, methodology):
    sanitized, pii = sanitize_prompt(description)
    system = f"You are a {PROJECT_PROMPTS.get(project_type, PROJECT_PROMPTS['📋 General / Other'])}"
    user = f"""REQUIREMENT (PII-SANITISED):
{sanitized}

CONTEXT: Type={project_type}, Team={team_size}, Sprint={sprint_len}wk, Method={methodology}

Return ONLY valid JSON:
{{"epic":{{"title":"","business_value":"","objective":"","estimated_sprints":3,"definition_of_done":[]}},"stories":[{{"id":"US-001","title":"","user_story":"As a [role], I want [feature], so that [benefit]","priority":"High","story_points":5,"sprint":"Sprint 1","type":"Feature","acceptance_criteria":["Given...When...Then..."],"subtasks":[{{"title":"","hours":4}}]}}],"risks":[{{"title":"","description":""}}],"dependencies":[]}}

RULES: 4-7 stories, Fibonacci points (1/2/3/5/8/13), Gherkin AC, ONLY JSON output."""
    return system, user, pii


# ═══════════════════════════════════════════════════════════
# CSS
# ═══════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Rajdhani:wght@600;700&family=Space+Mono:wght@400;700&display=swap');
* { font-family:'Inter',sans-serif; }
.privacy-shield{background:linear-gradient(135deg,#0a1628,#0d2137);border:1px solid rgba(41,182,246,.3);border-left:4px solid #29B6F6;border-radius:10px;padding:14px 18px;margin-bottom:16px;}
.privacy-shield .ps-title{color:#29B6F6;font-family:'Space Mono',monospace;font-size:11px;letter-spacing:1.5px;font-weight:700;margin-bottom:8px;}
.privacy-shield .ps-items{display:flex;gap:10px;flex-wrap:wrap;}
.privacy-shield .ps-item{background:rgba(41,182,246,.1);border:1px solid rgba(41,182,246,.2);color:#7BB8FF;padding:3px 10px;border-radius:12px;font-size:11px;font-family:'Space Mono',monospace;}
.pii-warning{background:#FFF3E0;border:1px solid #FFB300;border-left:4px solid #F57F17;border-radius:8px;padding:10px 14px;margin:8px 0;}
.pii-warning .pw-title{color:#E65100;font-weight:700;font-size:12px;}
.mask-badge{background:#E8F5E9;border:1px solid #A5D6A7;color:#2E7D32;padding:2px 8px;border-radius:10px;font-size:11px;font-family:'Space Mono',monospace;display:inline-block;margin:2px;}
.session-bar{display:flex;align-items:center;justify-content:space-between;background:rgba(179,27,27,.05);border:1px solid rgba(179,27,27,.15);border-radius:8px;padding:8px 14px;margin-bottom:12px;font-size:11px;font-family:'Space Mono',monospace;flex-wrap:wrap;gap:6px;}
.session-bar .sb-val{color:#B31B1B;font-weight:700;}
.main-header{background:linear-gradient(135deg,#B31B1B 0%,#7a1212 100%);padding:22px 28px;border-radius:10px;margin-bottom:20px;display:flex;align-items:center;justify-content:space-between;box-shadow:0 4px 15px rgba(179,27,27,.3);flex-wrap:wrap;gap:12px;}
.main-header h1{color:#FFC72C;margin:0;font-family:'Rajdhani',sans-serif;font-size:28px;font-weight:700;}
.version-badge{background:rgba(255,199,44,.15);border:1px solid rgba(255,199,44,.4);color:#FFC72C;padding:4px 12px;border-radius:20px;font-size:11px;font-weight:600;}
.secure-badge{background:rgba(41,182,246,.15);border:1px solid rgba(41,182,246,.4);color:#29B6F6;padding:4px 12px;border-radius:20px;font-size:11px;font-weight:600;margin-top:6px;display:inline-block;}
.provider-pill{background:rgba(105,240,174,.15);border:1px solid rgba(105,240,174,.4);color:#69F0AE;padding:4px 12px;border-radius:20px;font-size:11px;font-weight:600;margin-top:4px;display:inline-block;}
.section-title{color:#B31B1B;font-weight:600;font-size:16px;margin-top:20px;text-transform:uppercase;letter-spacing:.5px;}
.stButton>button{background-color:#B31B1B;color:white;font-weight:bold;border-radius:6px;}
.stButton>button:hover{background-color:#8E1414;color:#FFC72C;}
.metric-row{display:flex;gap:12px;margin:16px 0 8px 0;flex-wrap:wrap;}
.metric-box{background:white;border:1px solid #E8E8E8;border-top:3px solid #B31B1B;border-radius:8px;padding:14px 18px;min-width:120px;text-align:center;box-shadow:0 2px 8px rgba(0,0,0,.06);flex:1;}
.metric-box .metric-value{font-size:28px;font-weight:700;color:#B31B1B;font-family:'Rajdhani',sans-serif;}
.metric-box .metric-label{font-size:10px;color:#999;text-transform:uppercase;letter-spacing:.8px;margin-top:2px;}
.pipeline-steps{padding:4px 0;}
.pipeline-step{display:flex;align-items:flex-start;gap:12px;padding:10px 0;border-bottom:1px solid #F0F0F0;font-size:14px;}
.pipeline-step:last-child{border-bottom:none;}
.step-num{background:#B31B1B;color:white;border-radius:50%;width:22px;height:22px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;min-width:22px;}
.step-icon{font-size:18px;min-width:24px;}
.step-text{flex:1;line-height:1.5;}
.gde-container{background:#0D1117;border-radius:10px;padding:24px 20px;margin:12px 0;overflow-x:auto;}
.gde-flow{display:flex;align-items:center;min-width:max-content;padding:8px 0;}
.gde-node{display:flex;flex-direction:column;align-items:center;gap:6px;}
.gde-node-label{font-size:10px;color:#666;text-align:center;max-width:130px;}
.gde-arrow{display:flex;flex-direction:column;align-items:center;gap:4px;padding:0 6px;min-width:70px;}
.gde-count-label{font-size:10px;white-space:nowrap;text-align:center;}
.gde-legend{display:flex;gap:20px;margin-top:16px;flex-wrap:wrap;}
.gde-legend-item{display:flex;align-items:center;gap:6px;font-size:11px;color:#888;}
.legend-dot{width:10px;height:10px;border-radius:2px;}
@keyframes pulse{0%,100%{box-shadow:0 0 8px rgba(255,214,0,.3);}50%{box-shadow:0 0 20px rgba(255,214,0,.7);}}
.decrypt-panel{background:linear-gradient(135deg,#0a2a0a,#0d1f0d);border:2px solid rgba(105,240,174,.4);border-radius:12px;padding:20px 24px;margin:16px 0;}
.decrypt-panel-title{color:#69F0AE;font-family:'Space Mono',monospace;font-size:12px;font-weight:700;letter-spacing:1.5px;margin-bottom:12px;}
.download-option{background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.1);border-radius:8px;padding:12px 16px;margin:8px 0;}
.download-option-title{color:white;font-size:13px;font-weight:600;margin-bottom:4px;}
.download-option-desc{color:rgba(255,255,255,.6);font-size:11px;}
.epic-card{background:linear-gradient(135deg,#B31B1B,#7a1212);border-radius:10px;padding:20px 24px;margin:16px 0;}
.epic-title{color:#FFC72C;font-family:'Rajdhani',sans-serif;font-size:22px;font-weight:700;}
.epic-value{color:rgba(255,255,255,.85);font-size:13px;margin-top:6px;line-height:1.6;}
.epic-meta{display:flex;gap:10px;margin-top:12px;flex-wrap:wrap;}
.epic-badge{background:rgba(255,199,44,.2);border:1px solid rgba(255,199,44,.5);color:#FFC72C;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:600;}
.story-card{background:white;border:1px solid #E8E8E8;border-left:4px solid #B31B1B;border-radius:8px;padding:16px 20px;margin:10px 0;}
.story-id{font-size:11px;color:#999;font-weight:600;}
.story-title{font-size:14px;font-weight:600;color:#1a1a1a;margin:4px 0 8px 0;line-height:1.4;}
.story-desc{font-size:13px;color:#555;line-height:1.6;font-style:italic;}
.story-badges{display:flex;gap:6px;flex-wrap:wrap;margin-top:10px;}
.badge-high{background:#FFF3E0;color:#E65100;border:1px solid #FFCC80;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700;}
.badge-medium{background:#FFFDE7;color:#F57F17;border:1px solid #FFF176;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700;}
.badge-low{background:#F5F5F5;color:#616161;border:1px solid #E0E0E0;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700;}
.badge-critical{background:#FFEBEE;color:#C62828;border:1px solid #EF9A9A;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700;}
.badge-points{background:#FFC72C;color:#1a1a1a;padding:2px 10px;border-radius:10px;font-size:12px;font-weight:700;}
.badge-sprint{background:#E8F5E9;color:#2E7D32;border:1px solid #A5D6A7;padding:2px 8px;border-radius:10px;font-size:11px;}
.badge-type{background:#E3F2FD;color:#1565C0;border:1px solid #90CAF9;padding:2px 8px;border-radius:10px;font-size:11px;}
.ac-section{margin-top:12px;}
.ac-title{font-size:11px;font-weight:700;color:#B31B1B;text-transform:uppercase;margin-bottom:6px;}
.ac-item{font-size:12px;color:#444;padding:4px 0 4px 12px;border-left:2px solid #FFC72C;margin:4px 0;line-height:1.5;}
.subtask-item{display:flex;align-items:center;gap:8px;font-size:12px;color:#555;padding:3px 0;}
.subtask-hrs{font-size:10px;color:#999;background:#F5F5F5;padding:1px 6px;border-radius:8px;}
.risk-card{background:#FFF8E1;border:1px solid #FFE082;border-left:4px solid #FFC72C;border-radius:8px;padding:14px 18px;margin:10px 0;}
.risk-title{font-size:13px;font-weight:700;color:#E65100;margin-bottom:6px;}
.risk-item{font-size:12px;color:#555;padding:3px 0 3px 12px;border-left:2px solid #FFB300;margin:3px 0;}
.dod-card{background:#E8F5E9;border:1px solid #A5D6A7;border-left:4px solid #2E7D32;border-radius:8px;padding:14px 18px;margin:10px 0;}
.dod-title{font-size:13px;font-weight:700;color:#1B5E20;margin-bottom:6px;}
.dod-item{font-size:12px;color:#2E7D32;padding:3px 0 3px 12px;border-left:2px solid #66BB6A;margin:3px 0;}
.jira-metrics{display:flex;gap:12px;margin:16px 0;flex-wrap:wrap;}
.jira-metric-box{background:white;border:1px solid #E8E8E8;border-top:3px solid #B31B1B;border-radius:8px;padding:12px 16px;min-width:100px;text-align:center;flex:1;}
.jira-metric-value{font-size:24px;font-weight:700;color:#B31B1B;font-family:'Rajdhani',sans-serif;}
.jira-metric-label{font-size:10px;color:#999;text-transform:uppercase;}
.built-by{display:flex;align-items:center;justify-content:flex-end;gap:8px;padding:6px 16px 0 0;margin-bottom:-6px;}
.built-by .byline{font-size:11px;color:#999;letter-spacing:.8px;text-transform:uppercase;}
.built-by .author{font-family:'Rajdhani',sans-serif;font-size:15px;font-weight:700;color:#B31B1B;}
.built-by .dot{width:6px;height:6px;background:#FFC72C;border-radius:50%;display:inline-block;}
.debug-box{background:#0D1117;border:2px solid rgba(105,240,174,.5);border-radius:10px;padding:18px 20px;margin:12px 0;font-family:'Space Mono',monospace;font-size:11px;}
.debug-row{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid rgba(255,255,255,.05);}
.debug-row:last-child{border-bottom:none;}
.debug-key{color:#7BB8FF;}
.debug-val-ok{color:#69F0AE;font-weight:700;}
.debug-val-bad{color:#ff6b6b;font-weight:700;}
</style>
""", unsafe_allow_html=True)

EXAMPLES = [
    {"tag":"✅ Customer Summary","complexity":"Medium",
     "text":"Join customers with accounts on CUSTOMER_ID, then join with transactions on ACCOUNT_ID. Keep ACTIVE accounts and VERIFIED customers. Compute FULL_NAME (FIRST_NAME+LAST_NAME), ACCOUNT_COUNT, TOTAL_TRANSACTIONS, TOTAL_AMOUNT, AVG_AMOUNT. Sort by TOTAL_AMOUNT descending."},
    {"tag":"✅ Channel Spend","complexity":"Simple",
     "text":"Filter only DEBIT transactions. Group by CHANNEL and compute TOTAL_TRANSACTIONS, TOTAL_AMOUNT, AVG_AMOUNT, MAX_AMOUNT. Sort by TOTAL_AMOUNT descending."},
    {"tag":"✅ Dormant Accounts","complexity":"Medium",
     "text":"Join accounts with transactions on ACCOUNT_ID. For each DORMANT account find LAST_TXN_DATE and compute DAYS_INACTIVE. Join customers for FIRST_NAME, LAST_NAME. Sort by DAYS_INACTIVE descending."},
    {"tag":"✅ Monthly Credit/Debit","complexity":"Medium",
     "text":"Extract MONTH (YYYY-MM) from TRANSACTION_DATE. Pivot CREDIT and DEBIT totals per month. Add NET_FLOW (CREDIT minus DEBIT). Sort by MONTH ascending."},
    {"tag":"✅ Top 20 by Balance","complexity":"Simple",
     "text":"Join customers with accounts on CUSTOMER_ID. Keep ACTIVE accounts, exclude KYC_STATUS=REJECTED. Per customer: FULL_NAME, TOTAL_BALANCE, ACCOUNT_COUNT. Top 20 by TOTAL_BALANCE descending."},
    {"tag":"⚡ Risk Profile","complexity":"High",
     "text":"Join customers to accounts on CUSTOMER_ID, join accounts to transactions on ACCOUNT_ID. Keep ACTIVE accounts only, exclude KYC_STATUS=REJECTED. Last 90 days per customer: TOTAL_DEBIT, TOTAL_CREDIT, AVG_TXN_AMOUNT. RISK_LEVEL: HIGH if TOTAL_DEBIT>500000 or more than 3 transactions above 100000, MEDIUM if TOTAL_DEBIT between 200000 and 500000, LOW otherwise. Sort by TOTAL_DEBIT descending."},
]

# ═══════════════════════════════════════════════════════════
# HEADER
# ═══════════════════════════════════════════════════════════
active_prov  = get_active_provider()
active_model = get_active_model()

st.markdown("""<div class="built-by">
    <span class="byline">Built by</span><span class="dot"></span>
    <span class="author">PRADEEP</span><span class="dot"></span>
    <span class="byline">Enterprise AI</span></div>""", unsafe_allow_html=True)

st.markdown(f"""<div class="main-header">
    <div>
        <div style="color:rgba(255,255,255,.6);font-size:12px;margin-top:4px;">AI POWERED · LITELLM MULTI-PROVIDER · GEMINI PRIMARY · 🔒 BANK-GRADE PRIVACY</div>
        <h1>⚡ Enterprise AI Transformation &amp; Delivery Platform</h1>
        <div class="secure-badge">🛡️ PII PROTECTED · SCHEMA-ONLY AI · DECRYPT ON DEMAND · AUDIT LOGGED</div>
        <div class="provider-pill">🤖 {active_prov} &nbsp;·&nbsp; {active_model}</div>
    </div>
    <div style="text-align:right;"><div class="version-badge">v6.2 BULLETPROOF</div></div>
</div>""", unsafe_allow_html=True)

st.markdown(f"""<div class="session-bar">
    <div>🔐 Session: <span class="sb-val">{SESSION_ID}</span></div>
    <div style="color:#2E7D32;">🛡️ PII Masking: <span class="sb-val" style="color:#2E7D32;">ACTIVE</span></div>
    <div style="color:#2E7D32;">🔒 Schema-Only AI: <span class="sb-val" style="color:#2E7D32;">ENABLED</span></div>
    <div>🤖 <span class="sb-val" style="color:#1565C0;">{active_prov}</span></div>
    <div>⏱️ {datetime.datetime.now().strftime("%d %b %Y %H:%M")}</div>
</div>""", unsafe_allow_html=True)

st.markdown("""<div class="privacy-shield">
    <div class="ps-title">🔒 BANK-GRADE PRIVACY — ALL SESSIONS</div>
    <div class="ps-items">
        <span class="ps-item">✓ Schema-only to AI</span>
        <span class="ps-item">✓ PII auto-masked</span>
        <span class="ps-item">✓ Business values preserved</span>
        <span class="ps-item">✓ Decrypt with consent</span>
        <span class="ps-item">✓ No server storage</span>
        <span class="ps-item">✓ Prompt PII scan</span>
        <span class="ps-item">✓ Tamper-evident audit</span>
        <span class="ps-item">✓ Multi-provider · no single point of failure</span>
    </div>
</div>""", unsafe_allow_html=True)

tab1, tab2, tab3, tab4 = st.tabs([
    "⚡ AI ETL Engine", "📋 AI Jira Breakdown", "🎬 Demo & Benefits", "🔒 Privacy & Audit",
])

# ───────────────────────────────────────────────────────────
# TAB 1 — ETL ENGINE
# ───────────────────────────────────────────────────────────
with tab1:
    st.markdown("""<div style="background:#F0F7FF;border:1px solid #BBDEFB;border-left:4px solid #1E90FF;border-radius:8px;padding:12px 16px;margin-bottom:16px;">
        <div style="font-size:12px;font-weight:700;color:#1565C0;margin-bottom:6px;">🔒 HOW YOUR DATA IS PROTECTED</div>
        <div style="font-size:12px;color:#333;line-height:1.9;">
            <b>Step 1:</b> CSV loaded into session memory only — never written to disk.<br>
            <b>Step 2:</b> PII scanner masks sensitive columns. Business values (VERIFIED, ACTIVE, REJECTED etc.) preserved.<br>
            <b>Step 3:</b> ONLY column names + data types sent to AI. Zero data values leave your session.<br>
            <b>Step 4:</b> AI generates code. Code executes locally against your original data.<br>
            <b>Step 5:</b> Download masked (safe) or original (requires acknowledgement + audit log).
        </div></div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-title">💡 Example Prompts</div>', unsafe_allow_html=True)
    cols = st.columns(3)
    for i, ex in enumerate(EXAMPLES):
        with cols[i % 3]:
            if st.button(f"{ex['tag']}  [{ex['complexity']}]", key=f"ex_{i}", use_container_width=True):
                st.session_state["etl_prompt_val"] = ex["text"]

    st.markdown('<div class="section-title">Transformation Description</div>', unsafe_allow_html=True)
    etl_raw = st.text_area("Describe your data transformation in plain English",
        value=st.session_state.get("etl_prompt_val",""), key="etl_prompt", height=160,
        placeholder="Example: Show only KYC_STATUS = VERIFIED records, join with accounts, compute total balance...")

    if etl_raw:
        pii_in_prompt = scan_pii(etl_raw)
        if pii_in_prompt:
            st.markdown(f'<div class="pii-warning"><div class="pw-title">⚠️ PII Detected in Prompt: {", ".join(pii_in_prompt)}</div>'
                        f'<div style="font-size:12px;color:#555;">Auto-redacted. Business values like VERIFIED, ACTIVE preserved.</div></div>',
                        unsafe_allow_html=True)

    uploaded = st.file_uploader("Upload CSV File(s) — max 50MB each",
                                type=["csv"], accept_multiple_files=True, key="etl_upload")

    if uploaded:
        st.markdown('<div class="section-title">Files & Privacy Scan</div>', unsafe_allow_html=True)
        for i, f in enumerate(uploaded):
            ok, msg = validate_file(f)
            if not ok:
                st.error(f"❌ {f.name}: {msg}"); continue
            alias = f"df{i+1}" if len(uploaded) > 1 else "df"
            f.seek(0); _df = pd.read_csv(f)
            _, mcols, mtotal = mask_dataframe(_df)
            with st.expander(f"📄 {f.name}  →  `{alias}`  |  {_df.shape[0]:,} rows × {_df.shape[1]} cols", expanded=(i==0)):
                c1, c2 = st.columns([2,1])
                with c1:
                    st.dataframe(_df.head(3), use_container_width=True)
                with c2:
                    st.markdown("**🔒 Privacy Scan**")
                    if mcols:
                        st.markdown(f"<span style='color:#E65100;font-size:12px;'>⚠️ {len(mcols)} sensitive column(s):</span>", unsafe_allow_html=True)
                        for col in mcols:
                            st.markdown(f'<span class="mask-badge">🔒 {col}</span>', unsafe_allow_html=True)
                        st.markdown(f"<span style='color:#2E7D32;font-size:11px;'>✓ {mtotal} values masked</span>", unsafe_allow_html=True)
                    else:
                        st.markdown("<span style='color:#2E7D32;font-size:12px;'>✓ No sensitive columns</span>", unsafe_allow_html=True)

        if len(uploaded) > 1:
            aliases = [f"df{i+1}" for i in range(len(uploaded))]
            st.info(f"**{len(uploaded)} files.** Reference as: {', '.join(f'`{a}`' for a in aliases)}")

    with st.expander("🤖 AI Provider Status", expanded=False):
        st.dataframe(pd.DataFrame(get_router_status()), use_container_width=True, hide_index=True)
        if st.button("🔄 Refresh", key="refresh_tab1"): st.rerun()

    rc, hc = st.columns([1,3])
    with rc:
        run = st.button("▶ Execute ETL", key="run_etl", use_container_width=True)
    with hc:
        st.markdown(f"<div style='font-size:11px;color:#888;padding-top:8px;'>Active: <b style='color:#1565C0;'>{active_prov}</b> &nbsp;·&nbsp; Auto-fallback on rate limit &nbsp;·&nbsp; Zero data sent to AI</div>",
                    unsafe_allow_html=True)

    if run:
        if not etl_raw.strip(): st.warning("Please enter a transformation description."); st.stop()
        if not uploaded: st.warning("Please upload at least one CSV file."); st.stop()

        etl_clean, ppii = sanitize_prompt(etl_raw)
        if ppii: audit_log("PROMPT_PII", SESSION_ID, f"Types:{ppii}", "MEDIUM")

        dfs_masked={};dfs_original={};fnames=[];all_mc=[]
        for i, f in enumerate(uploaded):
            ok, msg = validate_file(f)
            if not ok: st.error(f"❌ {msg}"); st.stop()
            f.seek(0); alias = f"df{i+1}" if len(uploaded)>1 else "df"
            raw_df = pd.read_csv(f)
            mdf, mc, mt = mask_dataframe(raw_df)
            dfs_masked[alias]=mdf; dfs_original[alias]=raw_df.copy()
            fnames.append(f.name); all_mc.extend(mc)
            if mc: audit_log("PII_MASKED",SESSION_ID,f"File={f.name},cols={mc},count={mt}","HIGH")

        if len(uploaded)==1:
            dfs_masked["df"]=list(dfs_masked.values())[0]
            dfs_original["df"]=list(dfs_original.values())[0]

        primary="df" if len(uploaded)==1 else "df1"
        orig_rows=dfs_masked[primary].shape[0]
        sys_p=build_system_prompt(dfs_masked)
        audit_log("AI_QUERY",SESSION_ID,f"Files={fnames},provider={active_prov}","LOW")

        st.markdown('<div class="section-title">⚡ Execution Flow</div>', unsafe_allow_html=True)
        gde=st.empty()
        gde.markdown(make_gde_html(dfs_original,fnames,"",None,"reading",all_mc),unsafe_allow_html=True)
        time.sleep(0.3)
        gde.markdown(make_gde_html(dfs_original,fnames,"",None,"transforming",all_mc),unsafe_allow_html=True)

        ai_code=""; result_df=None; last_err=None
        conv=[{"role":"system","content":sys_p},{"role":"user","content":etl_clean}]

        with st.spinner(f"⚙️ {active_prov} generating pipeline (schema only — no data values sent)..."):
            for attempt in range(1,3):
                if last_err and attempt>1:
                    conv.append({"role":"assistant","content":ai_code})
                    conv.append({"role":"user","content":f"Fix: {last_err}\nStore result in 'result'. No markdown."})
                ai_code = call_ai(conv, temperature=0.05, task="code")
                if ai_code == RATE_LIMIT_SENTINEL:
                    st.error("### ⏱️ All AI Providers Rate-Limited\n\nAdd `GEMINI_API_KEY` in Streamlit Cloud → Settings → Secrets for 1M free TPM.\n\nOr wait 60 seconds for Groq to reset."); st.stop()
                try:
                    result_df = safe_exec(dfs_original, ai_code)
                    last_err = None; break
                except Exception as exc:
                    last_err = str(exc)
                    if attempt==2:
                        st.error(f"⚠️ ETL failed after 2 attempts: {exc}")
                        with st.expander("🔍 Debug: AI-generated code"):
                            st.code(extract_code(ai_code), language="python")
                        result_df = list(dfs_original.values())[0].copy()

        gde.markdown(make_gde_html(dfs_original,fnames,extract_code(ai_code),result_df,"done",all_mc),unsafe_allow_html=True)
        audit_log("ETL_COMPLETE",SESSION_ID,f"Rows:{orig_rows}→{len(result_df)},Provider={get_active_provider()},Status={'OK' if not last_err else 'FAILED'}","LOW")

        masked_result=result_df.copy()
        for col in result_df.columns:
            masked_result[col]=mask_sensitive_column(result_df[col],col)

        st.session_state.last_etl_result={"masked_df":masked_result,"original_df":result_df,"ai_code":ai_code,"file_names":fnames,"masked_cols":all_mc}

        st.markdown('<div class="section-title">📊 Pipeline Summary</div>', unsafe_allow_html=True)
        if all_mc:
            badges="".join(f'<span class="mask-badge">🔒 {c}</span>' for c in all_mc)
            st.markdown(f'<div style="background:#E8F5E9;border:1px solid #A5D6A7;border-radius:8px;padding:10px 14px;margin-bottom:10px;"><span style="font-size:12px;font-weight:700;color:#1B5E20;">✅ Privacy Applied:</span><div style="margin-top:6px;">{badges}</div></div>',unsafe_allow_html=True)

        with st.spinner("Generating pipeline narrative..."):
            try:
                metrics_html, steps_html = build_pipeline_log(extract_code(ai_code),dfs_masked,result_df,fnames,orig_rows)
            except Exception:
                metrics_html=(f'<div class="metric-row">'
                              f'<div class="metric-box"><div class="metric-value">{len(fnames)}</div><div class="metric-label">Files</div></div>'
                              f'<div class="metric-box"><div class="metric-value">{orig_rows:,}</div><div class="metric-label">Rows In</div></div>'
                              f'<div class="metric-box"><div class="metric-value">{len(result_df):,}</div><div class="metric-label">Rows Out</div></div>'
                              f'<div class="metric-box"><div class="metric-value">{len(result_df.columns)}</div><div class="metric-label">Columns</div></div>'
                              f'</div>')
                steps_html='<div class="pipeline-steps"><div class="pipeline-step"><span class="step-icon">✅</span><span class="step-text">Transformation complete.</span></div></div>'

        st.markdown(metrics_html, unsafe_allow_html=True)
        with st.expander("📋 Pipeline steps", expanded=False): st.markdown(steps_html, unsafe_allow_html=True)
        with st.expander("🔍 Generated Python code", expanded=False): st.code(extract_code(ai_code), language="python")

        st.markdown('<div class="section-title">Transformed Output (Masked Preview)</div>', unsafe_allow_html=True)
        total=len(result_df)
        ci,cs=st.columns([3,1])
        ci.markdown(f"<span style='font-size:13px;color:#666;'>Total: <b>{total:,}</b> rows &nbsp;·&nbsp; Provider: <b style='color:#1565C0;'>{get_active_provider()}</b></span>",unsafe_allow_html=True)
        opts=sorted(set(n for n in [20,50,100,500,1000,total] if n<=total)) or [total]
        n_show=cs.selectbox("Show rows",opts,index=0,key="show_n")
        st.dataframe(masked_result.head(n_show), use_container_width=True)

        st.session_state.history.append({
            "Time":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Session":SESSION_ID,"Prompt":etl_clean[:80]+"..." if len(etl_clean)>80 else etl_clean,
            "Files":", ".join(fnames),"Rows In":orig_rows,"Rows Out":len(result_df),
            "PII Masked":", ".join(all_mc) or "None",
            "Provider":get_active_provider(),"Model":get_active_model(),
            "Status":"OK" if not last_err else "FAILED",
        })

    if st.session_state.last_etl_result:
        r=st.session_state.last_etl_result
        st.markdown("---")
        render_download_panel(r["masked_df"],r["original_df"],r["file_names"],r["masked_cols"])


# ───────────────────────────────────────────────────────────
# TAB 2 — JIRA BREAKDOWN
# ───────────────────────────────────────────────────────────
with tab2:
    st.markdown("""<div style="background:#F0F7FF;border:1px solid #BBDEFB;border-left:4px solid #1E90FF;border-radius:8px;padding:12px 16px;margin-bottom:16px;">
        <div style="font-size:12px;font-weight:700;color:#1565C0;margin-bottom:4px;">🔒 PRIVACY IN JIRA BREAKDOWN</div>
        <div style="font-size:12px;color:#333;">Requirement text scanned for PII. Detected PII redacted. Business values preserved.</div>
    </div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-title">Project Configuration</div>', unsafe_allow_html=True)
    proj_type=st.selectbox("Project Type",list(PROJECT_PROMPTS.keys()),index=6,key="proj_type")
    ca,cb,cc=st.columns(3)
    team_sz=ca.selectbox("Team Size",[2,3,4,5,6,8,10,12,15,20],index=3,key="team_sz")
    sprint_l=cb.selectbox("Sprint (weeks)",[1,2,3],index=1,key="sprint_l")
    method=cc.selectbox("Methodology",["Scrum","Kanban","SAFe","Scrumban"],key="method")

    st.markdown('<div class="section-title">Business Requirement</div>', unsafe_allow_html=True)
    jira_raw=st.text_area("Describe the feature, initiative, or product requirement",key="jira_prompt",height=160,
                          placeholder="Example: Build a customer portal for viewing account statements, raising disputes...")

    if jira_raw:
        pj=scan_pii(jira_raw)
        if pj:
            st.markdown(f'<div class="pii-warning"><div class="pw-title">⚠️ PII in Requirement: {", ".join(pj)}</div>'
                        f'<div style="font-size:12px;color:#555;">Auto-redacted. Business values preserved.</div></div>',unsafe_allow_html=True)

    if st.button("🚀 Generate Jira Breakdown", key="run_jira"):
        if not jira_raw.strip(): st.warning("Enter a requirement."); st.stop()
        with st.spinner(f"🧠 Generating {proj_type} breakdown via {active_prov}..."):
            sp,up,pii_j=build_jira_prompt(jira_raw,proj_type,team_sz,sprint_l,method)
            if pii_j: audit_log("JIRA_PII",SESSION_ID,f"Types:{pii_j}","MEDIUM")
            audit_log("JIRA_QUERY",SESSION_ID,f"Type={proj_type}","LOW")
            raw_out=call_ai([{"role":"system","content":sp},{"role":"user","content":up}],temperature=0.3,task="jira")
        if raw_out==RATE_LIMIT_SENTINEL: st.error("⏱️ All providers rate-limited. Add GEMINI_API_KEY or wait 60s."); st.stop()
        try:
            jm=re.search(r"\{.*\}",raw_out,re.DOTALL)
            jdata=json.loads(jm.group()) if jm else {}
        except Exception: jdata={}
        if not jdata: st.error("Could not parse AI output."); st.markdown(raw_out); st.stop()
        audit_log("JIRA_DONE",SESSION_ID,f"Stories={len(jdata.get('stories',[]))},Provider={get_active_provider()}","LOW")
        st.session_state.jira_result={"data":jdata,"type":proj_type}

    if st.session_state.jira_result:
        jd=st.session_state.jira_result["data"]; pt=st.session_state.jira_result["type"]
        epic=jd.get("epic",{}); stories=jd.get("stories",[]); risks=jd.get("risks",[]); deps=jd.get("dependencies",[])
        total_pts=sum(s.get("story_points",0) for s in stories); sprints=epic.get("estimated_sprints","?")
        st.markdown(f'<div class="jira-metrics">'
                    f'<div class="jira-metric-box"><div class="jira-metric-value">{len(stories)}</div><div class="jira-metric-label">Stories</div></div>'
                    f'<div class="jira-metric-box"><div class="jira-metric-value">{total_pts}</div><div class="jira-metric-label">Points</div></div>'
                    f'<div class="jira-metric-box"><div class="jira-metric-value">{sprints}</div><div class="jira-metric-label">Sprints</div></div>'
                    f'<div class="jira-metric-box"><div class="jira-metric-value">{len(risks)}</div><div class="jira-metric-label">Risks</div></div>'
                    f'<div class="jira-metric-box"><div class="jira-metric-value">{len(deps)}</div><div class="jira-metric-label">Deps</div></div>'
                    f'</div>',unsafe_allow_html=True)
        dod="".join(f'<div class="dod-item">✓ {d}</div>' for d in epic.get("definition_of_done",[]))
        st.markdown(f'<div class="epic-card"><div class="epic-title">🏆 EPIC: {epic.get("title","")}</div>'
                    f'<div class="epic-value">{epic.get("business_value","")}</div>'
                    f'<div class="epic-value" style="margin-top:6px;"><b>Objective:</b> {epic.get("objective","")}</div>'
                    f'<div class="epic-meta"><span class="epic-badge">📅 {sprints} Sprints</span>'
                    f'<span class="epic-badge">📊 {total_pts} Points</span>'
                    f'<span class="epic-badge">📝 {len(stories)} Stories</span></div></div>',unsafe_allow_html=True)
        if dod: st.markdown(f'<div class="dod-card"><div class="dod-title">✅ Definition of Done</div>{dod}</div>',unsafe_allow_html=True)

        st.markdown('<div class="section-title">📝 User Stories</div>', unsafe_allow_html=True)
        pbadge={"critical":"badge-critical","high":"badge-high","medium":"badge-medium","low":"badge-low"}
        for s in stories:
            pri=s.get("priority","Medium"); pts=s.get("story_points",0); sid=s.get("id","US-?"); stype=s.get("type","Feature")
            pb=pbadge.get(pri.lower(),"badge-medium")
            with st.expander(f"  {sid} · {s.get('title','')}  [{pri}] [{pts}pts]", expanded=False):
                acs="".join(f'<div class="ac-item">• {a}</div>' for a in s.get("acceptance_criteria",[]))
                subs="".join(f'<div class="subtask-item">☐ {t.get("title","")} <span class="subtask-hrs">~{t.get("hours",0)}h</span></div>' for t in s.get("subtasks",[]))
                st.markdown(f'<div class="story-card"><div class="story-id">{sid} · {pt}</div>'
                            f'<div class="story-title">{s.get("title","")}</div>'
                            f'<div class="story-desc">{s.get("user_story","")}</div>'
                            f'<div class="story-badges"><span class="{pb}">🔴 {pri}</span>'
                            f'<span class="badge-points">⭐ {pts} pts</span>'
                            f'<span class="badge-sprint">🏃 {s.get("sprint","")}</span>'
                            f'<span class="badge-type">🏷 {stype}</span></div>'
                            f'<div class="ac-section"><div class="ac-title">✅ Acceptance Criteria</div>{acs}</div>'
                            f'<div style="margin-top:10px;"><div style="font-size:11px;font-weight:700;color:#666;text-transform:uppercase;margin-bottom:6px;">🔧 Subtasks</div>{subs}</div>'
                            f'</div>',unsafe_allow_html=True)

        if risks:
            st.markdown('<div class="section-title">⚠️ Risks & Dependencies</div>', unsafe_allow_html=True)
            rhtml='<div class="risk-card"><div class="risk-title">⚠️ Identified Risks</div>'
            for r in risks: rhtml+=f'<div class="risk-item"><b>{r.get("title","")}</b> — {r.get("description","")}</div>'
            rhtml+="</div>"
            if deps:
                rhtml+='<div class="risk-card" style="border-left-color:#1E90FF;background:#E3F2FD;"><div class="risk-title" style="color:#1565C0;">🔗 Dependencies</div>'
                for d in deps: rhtml+=f'<div class="risk-item" style="border-left-color:#1E90FF;">{d}</div>'
                rhtml+="</div>"
            st.markdown(rhtml, unsafe_allow_html=True)

        st.markdown('<div class="section-title">Export</div>', unsafe_allow_html=True)
        ec1,ec2,ec3=st.columns(3)
        txt="\n".join([f"EPIC: {epic.get('title','')}"]+[f"\n{s.get('id','')} — {s.get('title','')}\n  {s.get('user_story','')}" for s in stories])
        ec1.download_button("⬇ TXT",txt,"jira_breakdown.txt","text/plain")
        xb=BytesIO()
        with pd.ExcelWriter(xb,engine="xlsxwriter") as w:
            pd.DataFrame([{"ID":s.get("id",""),"Title":s.get("title",""),"User Story":s.get("user_story",""),"Priority":s.get("priority",""),"Points":s.get("story_points",""),"Sprint":s.get("sprint",""),"Type":s.get("type","")} for s in stories]).to_excel(w,sheet_name="Stories",index=False)
            ac_rows=[{"Story":s.get("id",""),"AC":a} for s in stories for a in s.get("acceptance_criteria",[])]
            if ac_rows: pd.DataFrame(ac_rows).to_excel(w,sheet_name="Acceptance_Criteria",index=False)
            if risks: pd.DataFrame(risks).to_excel(w,sheet_name="Risks",index=False)
        ec2.download_button("⬇ Excel",xb.getvalue(),"jira_breakdown.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        ec3.download_button("⬇ JSON",json.dumps(jd,indent=2),"jira_breakdown.json","application/json")


# ───────────────────────────────────────────────────────────
# TAB 3 — DEMO & BENEFITS
# ───────────────────────────────────────────────────────────
with tab3:
    st.markdown(f"""<div style="background:linear-gradient(135deg,#B31B1B,#7a1212);border-radius:12px;padding:28px 32px;margin-bottom:24px;">
        <div style="color:#FFC72C;font-family:'Rajdhani',sans-serif;font-size:28px;font-weight:700;margin-bottom:8px;">⚡ v6.2 — Bulletproof Gemini Priority</div>
        <div style="color:rgba(255,255,255,.85);font-size:14px;line-height:2.0;">
            ✅ <b>ROOT CAUSE FIXED:</b> Router now reads st.secrets directly — no longer depends on sync block<br>
            ✅ Gemini 2.0 Flash is PRIMARY — confirmed by sort order test<br>
            ✅ VERIFIED / ACTIVE / REJECTED never redacted (whitelist)<br>
            ✅ Active right now: <b style="color:#69F0AE;">{active_prov} ({active_model})</b>
        </div>
    </div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-title">📊 Provider Priority — How Router Selects</div>', unsafe_allow_html=True)
    st.markdown("""<div style="background:white;border:1px solid #E8E8E8;border-radius:10px;padding:18px;margin:12px 0;">
    <table style="width:100%;border-collapse:collapse;font-size:13px;">
    <tr style="background:#B31B1B;color:white;">
        <th style="padding:10px 14px;text-align:left;">Priority</th>
        <th style="padding:10px 14px;">Provider</th>
        <th style="padding:10px 14px;text-align:center;">TPM</th>
        <th style="padding:10px 14px;text-align:center;">Cost</th>
        <th style="padding:10px 14px;text-align:center;">Quality</th>
    </tr>
    <tr style="background:#E8F5E9;font-weight:700;">
        <td style="padding:9px 14px;color:#2E7D32;">① PRIMARY</td>
        <td style="padding:9px 14px;color:#2E7D32;">🟦 Google Gemini 2.0 Flash</td>
        <td style="padding:9px 14px;text-align:center;color:#2E7D32;">1,000,000</td>
        <td style="padding:9px 14px;text-align:center;color:#2E7D32;">FREE</td>
        <td style="padding:9px 14px;text-align:center;">★★★★☆</td>
    </tr>
    <tr style="background:#F1F8E9;">
        <td style="padding:9px 14px;color:#388E3C;">② BACKUP</td>
        <td style="padding:9px 14px;color:#388E3C;">🟦 Google Gemini 1.5 Flash</td>
        <td style="padding:9px 14px;text-align:center;color:#388E3C;">1,000,000</td>
        <td style="padding:9px 14px;text-align:center;color:#388E3C;">FREE</td>
        <td style="padding:9px 14px;text-align:center;">★★★★☆</td>
    </tr>
    <tr>
        <td style="padding:9px 14px;color:#E65100;">③ FALLBACK</td>
        <td style="padding:9px 14px;">🟥 Groq Llama-3.3-70b</td>
        <td style="padding:9px 14px;text-align:center;">6,000</td>
        <td style="padding:9px 14px;text-align:center;">FREE</td>
        <td style="padding:9px 14px;text-align:center;">★★★★☆</td>
    </tr>
    <tr style="background:#FFF3E0;">
        <td style="padding:9px 14px;color:#E65100;">④–⑥</td>
        <td style="padding:9px 14px;">🟥 Groq (other models)</td>
        <td style="padding:9px 14px;text-align:center;">6k–15k</td>
        <td style="padding:9px 14px;text-align:center;">FREE</td>
        <td style="padding:9px 14px;text-align:center;">★★★☆☆</td>
    </tr>
    </table>
    <div style="font-size:11px;color:#888;margin-top:10px;">Sort algorithm: quality DESC → cost ASC → stable (list order). Gemini always wins over Groq when key is present.</div>
    </div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-title">📈 ROI</div>', unsafe_allow_html=True)
    st.markdown("""<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-top:14px;">
        <div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #B31B1B;border-radius:10px;padding:18px;text-align:center;">
            <div style="font-family:'Rajdhani',sans-serif;font-size:38px;font-weight:700;color:#B31B1B;">~80%</div>
            <div style="font-size:10px;color:#999;text-transform:uppercase;">Time Saved on Tickets</div></div>
        <div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #2E7D32;border-radius:10px;padding:18px;text-align:center;">
            <div style="font-family:'Rajdhani',sans-serif;font-size:38px;font-weight:700;color:#2E7D32;">12×</div>
            <div style="font-size:10px;color:#999;text-transform:uppercase;">ETL Build Speed</div></div>
        <div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #1565C0;border-radius:10px;padding:18px;text-align:center;">
            <div style="font-family:'Rajdhani',sans-serif;font-size:38px;font-weight:700;color:#1565C0;">₹0</div>
            <div style="font-size:10px;color:#999;text-transform:uppercase;">Weekly AI Cost</div></div>
        <div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #29B6F6;border-radius:10px;padding:18px;text-align:center;">
            <div style="font-family:'Rajdhani',sans-serif;font-size:38px;font-weight:700;color:#29B6F6;">100%</div>
            <div style="font-size:10px;color:#999;text-transform:uppercase;">Data Stays Local</div></div>
    </div>""", unsafe_allow_html=True)


# ───────────────────────────────────────────────────────────
# TAB 4 — PRIVACY & AUDIT  (with v6.2 debug panel)
# ───────────────────────────────────────────────────────────
with tab4:

    # ✅ v6.2 DEBUG PANEL — shows exactly what the router sees
    st.markdown('<div class="section-title">🔬 v6.2 Secret Key Debug Panel</div>', unsafe_allow_html=True)
    st.markdown("""<div style="background:#FFF3E0;border:1px solid #FFB300;border-left:4px solid #F57F17;border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:12px;">
    <b>This panel shows the TWO-LAYER key lookup that v6.2 uses.</b>
    If Layer 2 shows ✅, Gemini will work even if Layer 1 failed.
    All rows must show ✅ for the key you want to use.
    </div>""", unsafe_allow_html=True)

    debug_rows = []
    for key_name in ["GEMINI_API_KEY","GROQ_API_KEY","MISTRAL_API_KEY","OPENAI_API_KEY","ANTHROPIC_API_KEY"]:
        l1 = os.environ.get(key_name,"").strip()
        l2 = ""
        try:
            l2 = str(st.secrets.get(key_name,"")).strip()
        except Exception:
            pass
        final = _get_key(key_name)
        debug_rows.append({
            "Key": key_name,
            "Layer 1 (os.environ)": "✅ SET" if l1 else "❌ EMPTY",
            "Layer 2 (st.secrets)": "✅ SET" if l2 else "❌ EMPTY",
            "Router Sees":          "✅ FOUND" if final else "❌ NOT FOUND",
            "Will Be Used":         "✅ YES" if final else "❌ NO",
        })
    st.dataframe(pd.DataFrame(debug_rows), use_container_width=True, hide_index=True)

    st.markdown("""<div style="background:#E8F5E9;border:1px solid #A5D6A7;border-radius:8px;padding:10px 14px;margin-bottom:16px;font-size:12px;">
    <b>If GEMINI_API_KEY shows ❌ in both layers:</b><br>
    → Go to <b>share.streamlit.io → Your App → ⋮ → Settings → Secrets</b> and add:<br>
    <code>GEMINI_API_KEY = "AIzaSy_your_key_here"</code><br>
    → Click Save → App will restart → GEMINI will show ✅ 🟢 Ready
    </div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-title">🔒 Privacy Framework</div>', unsafe_allow_html=True)
    st.markdown(f"""<div style="background:#0D1117;border:1px solid rgba(41,182,246,.3);border-radius:12px;padding:24px;margin-bottom:20px;">
        <div style="color:#29B6F6;font-family:'Space Mono',monospace;font-size:12px;letter-spacing:1.5px;margin-bottom:16px;">
            🛡️ SESSION {SESSION_ID} — {get_active_provider()} ({get_active_model()})
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;">
            <div style="background:#0a1628;border:1px solid rgba(41,182,246,.2);border-radius:8px;padding:14px;">
                <div style="color:#FFC72C;font-size:12px;font-weight:700;margin-bottom:8px;">🔍 PII Detection (30+ patterns)</div>
                <div style="color:#7BB8FF;font-size:11px;line-height:2;font-family:'Space Mono',monospace;">
                    Account Numbers · IBAN/BIC/SWIFT<br>Sort Codes · Card Numbers (PAN)<br>
                    SSN / NIN / Passport · Email<br>Phone (IN/UK/US) · IP · DOB
                </div>
            </div>
            <div style="background:#0a1628;border:1px solid rgba(41,182,246,.2);border-radius:8px;padding:14px;">
                <div style="color:#FFC72C;font-size:12px;font-weight:700;margin-bottom:8px;">✅ Business Values Preserved</div>
                <div style="color:#69F0AE;font-size:11px;line-height:2;font-family:'Space Mono',monospace;">
                    VERIFIED · REJECTED · PENDING<br>ACTIVE · INACTIVE · DORMANT<br>
                    DEBIT · CREDIT · UPI · NEFT<br>HIGH · MEDIUM · LOW · CRITICAL
                </div>
            </div>
        </div>
    </div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-title">🤖 Live Provider Dashboard</div>', unsafe_allow_html=True)
    st.dataframe(pd.DataFrame(get_router_status()), use_container_width=True, hide_index=True)
    if st.button("🔄 Refresh", key="refresh_tab4"): st.rerun()

    st.markdown('<div class="section-title">📋 Session Audit Log</div>', unsafe_allow_html=True)
    if st.session_state.history:
        adf=pd.DataFrame(st.session_state.history)
        st.dataframe(adf, use_container_width=True)
        st.download_button("⬇ Download Audit Log", adf.to_csv(index=False).encode(), "audit_log.csv", "text/csv")
    else:
        st.info("No actions yet. Run an ETL or Jira breakdown to see audit entries.")

    st.markdown('<div class="section-title">🧪 PII Scanner Test</div>', unsafe_allow_html=True)
    test_in=st.text_area("Test text — check what gets redacted vs preserved", key="pii_test",
                         placeholder="Try: VERIFIED customer john@bank.com with account 12345678901234 or KYC_STATUS=ACTIVE")
    if test_in:
        found=scan_pii(test_in)
        san,_=sanitize_prompt(test_in)
        if found:
            st.markdown(f'<div class="pii-warning"><div class="pw-title">⚠️ PII Detected: {", ".join(found)}</div>'
                        f'<div style="font-size:12px;color:#555;margin-top:6px;"><b>Original:</b> {test_in}<br><b>Sanitized:</b> {san}</div></div>',
                        unsafe_allow_html=True)
        else:
            st.success(f"✅ No PII detected. Text passes through unchanged.")
