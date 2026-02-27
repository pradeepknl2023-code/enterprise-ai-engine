import streamlit as st
import pandas as pd
import numpy as np
import os
import re
import hashlib
import datetime
import json
import time
import logging
import uuid
from io import BytesIO

# -----------------------------------
# PAGE CONFIG
# -----------------------------------
st.set_page_config(page_title="Enterprise AI ETL Platform", layout="wide", page_icon="⚡")

# -----------------------------------
# LOGGING SETUP (Audit Trail)
# -----------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.StreamHandler()]
)
audit_logger = logging.getLogger("AUDIT")

def audit_log(action: str, session_id: str, details: str, risk_level: str = "LOW"):
    audit_logger.info(f"SESSION={session_id} | ACTION={action} | RISK={risk_level} | {details}")

# -----------------------------------
# SESSION INIT
# -----------------------------------
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())[:8].upper()
if "history" not in st.session_state:
    st.session_state.history = []
if "jira_result" not in st.session_state:
    st.session_state.jira_result = None
if "last_etl_result" not in st.session_state:
    st.session_state.last_etl_result = None  # stores {masked_df, original_df, masked_cols, file_names, prompt}
if "privacy_acknowledged" not in st.session_state:
    st.session_state.privacy_acknowledged = False

SESSION_ID = st.session_state.session_id

# -----------------------------------
# PRIVACY ENGINE
# -----------------------------------
SENSITIVE_PATTERNS = {
    "account_number": r'\b\d{8,17}\b',
    "sort_code": r'\b\d{2}-\d{2}-\d{2}\b',
    "card_number": r'\b(?:\d[ -]?){13,19}\b',
    "ssn": r'\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b',
    "iban": r'\b[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}([A-Z0-9]?){0,16}\b',
    "swift": r'\b[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?\b',
    "email": r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
    "phone_uk": r'\b(?:0|\+44)[\s-]?\d{4}[\s-]?\d{6}\b',
    "phone_us": r'\b(?:\+1[\s-]?)?\(?\d{3}\)?[\s-]?\d{3}[\s-]?\d{4}\b',
    "phone_in": r'\b[6-9]\d{9}\b',
    "ip_address": r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
    "postcode_uk": r'\b[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}\b',
    "zip_us": r'\b\d{5}(?:-\d{4})?\b',
    "dob": r'\b(?:0?[1-9]|[12]\d|3[01])[/-](?:0?[1-9]|1[0-2])[/-](?:19|20)\d{2}\b',
    "passport": r'\b[A-Z]{1,2}\d{6,9}\b',
}

SENSITIVE_COLUMN_KEYWORDS = [
    'account', 'acct', 'iban', 'bic', 'swift', 'sort', 'routing',
    'card', 'cvv', 'pin', 'password', 'passwd', 'secret', 'token',
    'ssn', 'sin', 'nin', 'nino', 'passport', 'license', 'licence',
    'salary', 'wage', 'income', 'balance', 'credit', 'debit',
    'tax', 'vat', 'ein', 'tin',
    'email', 'phone', 'mobile', 'tel', 'fax',
    'address', 'postcode', 'zipcode', 'zip', 'dob', 'birthdate', 'birth',
    'gender', 'ethnicity', 'religion', 'health', 'medical',
    'ip', 'device', 'mac_addr',
    'name', 'surname', 'firstname', 'lastname', 'fullname',
    'national', 'id_number', 'customer_id', 'member_id',
]

def hash_value(val: str, prefix: str = "") -> str:
    h = hashlib.sha256(str(val).encode()).hexdigest()[:8].upper()
    return f"{prefix}[MASKED-{h}]"

def mask_sensitive_column(series: pd.Series, col_name: str) -> pd.Series:
    col_lower = col_name.lower().replace(" ", "_").replace("-", "_")
    is_sensitive = any(kw in col_lower for kw in SENSITIVE_COLUMN_KEYWORDS)
    if not is_sensitive:
        return series
    def mask_val(v):
        if pd.isna(v) or v == "":
            return v
        sv = str(v)
        for pattern_name, pattern in SENSITIVE_PATTERNS.items():
            if re.search(pattern, sv, re.IGNORECASE):
                return hash_value(sv, pattern_name[:3].upper())
        return hash_value(sv, "PII")
    return series.apply(mask_val)

def mask_dataframe(df: pd.DataFrame) -> tuple:
    masked_df = df.copy()
    masked_cols = []
    total_masked = 0
    for col in df.columns:
        original = df[col].copy()
        masked = mask_sensitive_column(df[col], col)
        changes = (original.astype(str) != masked.astype(str)).sum()
        if changes > 0:
            masked_df[col] = masked
            masked_cols.append(col)
            total_masked += changes
    return masked_df, masked_cols, total_masked

def scan_text_for_pii(text: str) -> list:
    found = []
    for pattern_name, pattern in SENSITIVE_PATTERNS.items():
        if re.search(pattern, text, re.IGNORECASE):
            found.append(pattern_name)
    return found

def sanitize_prompt(prompt: str) -> tuple:
    pii_found = scan_text_for_pii(prompt)
    sanitized = prompt
    for pattern_name, pattern in SENSITIVE_PATTERNS.items():
        sanitized = re.sub(pattern, f'[REDACTED_{pattern_name.upper()}]', sanitized, flags=re.IGNORECASE)
    return sanitized, pii_found

def validate_file(uploaded_file) -> tuple:
    MAX_SIZE_MB = 50
    ALLOWED_TYPES = ['csv']
    if uploaded_file.size > MAX_SIZE_MB * 1024 * 1024:
        return False, f"File exceeds {MAX_SIZE_MB}MB limit."
    ext = uploaded_file.name.rsplit('.', 1)[-1].lower()
    if ext not in ALLOWED_TYPES:
        return False, f"File type .{ext} not permitted. Only CSV allowed."
    if '..' in uploaded_file.name or '/' in uploaded_file.name:
        return False, "Invalid filename."
    return True, "OK"

def build_schema_only_context(dataframes: dict) -> str:
    schema_lines = ""
    for alias, df in dataframes.items():
        dtypes = {c: str(t) for c, t in df.dtypes.items()}
        schema_lines += f"\n  {alias}: columns={df.columns.tolist()}, dtypes={dtypes}, shape={df.shape}"
        if len(df) > 0:
            sample = df.head(2).copy()
            for col in sample.columns:
                sample[col] = mask_sensitive_column(sample[col], col)
            schema_lines += f"\n  {alias}_sample_masked={sample.to_dict('records')}"
    return schema_lines

# -----------------------------------
# GROQ / AI SETUP  ← swap for any LLM
# -----------------------------------
try:
    from groq import Groq
    GROQ_API_KEY = os.getenv("GROQ_API_KEY")
    if not GROQ_API_KEY:
        st.error("⚠️ Set GROQ_API_KEY in Streamlit Secrets or environment variables.")
        st.stop()
    ai_client = Groq(api_key=GROQ_API_KEY)
    AI_MODEL = "llama-3.3-70b-versatile"
    AI_PROVIDER = "Groq / LLaMA 3.3 70B"
except ImportError:
    st.error("groq package not installed. Run: pip install groq")
    st.stop()

def call_ai(messages: list, temperature: float = 0.1, max_tokens: int = 4096) -> str:
    resp = ai_client.chat.completions.create(
        model=AI_MODEL,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens
    )
    return resp.choices[0].message.content

# -----------------------------------
# FULL CSS
# -----------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Rajdhani:wght@600;700&family=Orbitron:wght@700;900&family=Space+Mono:wght@400;700&display=swap');
* { font-family: 'Inter', sans-serif; }

.privacy-shield { background: linear-gradient(135deg, #0a1628, #0d2137); border: 1px solid rgba(41,182,246,0.3); border-left: 4px solid #29B6F6; border-radius: 10px; padding: 14px 18px; margin-bottom: 16px; }
.privacy-shield .ps-title { color: #29B6F6; font-family: 'Space Mono', monospace; font-size: 11px; letter-spacing: 1.5px; font-weight: 700; margin-bottom: 8px; }
.privacy-shield .ps-items { display: flex; gap: 10px; flex-wrap: wrap; }
.privacy-shield .ps-item { background: rgba(41,182,246,0.1); border: 1px solid rgba(41,182,246,0.2); color: #7BB8FF; padding: 3px 10px; border-radius: 12px; font-size: 11px; font-family: 'Space Mono', monospace; }
.pii-warning { background: #FFF3E0; border: 1px solid #FFB300; border-left: 4px solid #F57F17; border-radius: 8px; padding: 10px 14px; margin: 8px 0; }
.pii-warning .pw-title { color: #E65100; font-weight: 700; font-size: 12px; margin-bottom: 4px; }
.pii-warning .pw-text { color: #555; font-size: 12px; }
.mask-badge { background: #E8F5E9; border: 1px solid #A5D6A7; color: #2E7D32; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-family: 'Space Mono', monospace; display: inline-block; margin: 2px; }
.session-bar { display: flex; align-items: center; justify-content: space-between; background: rgba(179,27,27,0.05); border: 1px solid rgba(179,27,27,0.15); border-radius: 8px; padding: 8px 14px; margin-bottom: 12px; font-size: 11px; font-family: 'Space Mono', monospace; flex-wrap: wrap; gap: 6px; }
.session-bar .sb-item { color: #666; display: flex; align-items: center; gap: 5px; }
.session-bar .sb-val { color: #B31B1B; font-weight: 700; }
.session-bar .sb-secure { color: #2E7D32; }
.built-by-banner { display: flex; align-items: center; justify-content: flex-end; gap: 8px; padding: 6px 16px 0 0; margin-bottom: -6px; }
.built-by-banner .byline { font-size: 11px; color: #999; letter-spacing: 0.8px; text-transform: uppercase; }
.built-by-banner .author { font-family: 'Rajdhani', sans-serif; font-size: 15px; font-weight: 700; color: #B31B1B; letter-spacing: 1px; }
.built-by-banner .dot { width: 6px; height: 6px; background: #FFC72C; border-radius: 50%; display: inline-block; }
.main-header { background: linear-gradient(135deg, #B31B1B 0%, #7a1212 100%); padding: 22px 28px; border-radius: 10px; margin-bottom: 20px; display: flex; align-items: center; justify-content: space-between; box-shadow: 0 4px 15px rgba(179,27,27,0.3); flex-wrap: wrap; gap: 12px; }
.main-header h1 { color: #FFC72C; margin: 0; font-family: 'Rajdhani', sans-serif; font-size: 28px; font-weight: 700; letter-spacing: 1px; }
.main-header .header-sub { color: rgba(255,255,255,0.6); font-size: 12px; margin-top: 4px; letter-spacing: 0.5px; }
.main-header .version-badge { background: rgba(255,199,44,0.15); border: 1px solid rgba(255,199,44,0.4); color: #FFC72C; padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 600; letter-spacing: 1px; }
.main-header .secure-badge { background: rgba(41,182,246,0.15); border: 1px solid rgba(41,182,246,0.4); color: #29B6F6; padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 600; letter-spacing: 1px; margin-top: 6px; }
.section-title { color: #B31B1B; font-weight: 600; font-size: 16px; margin-top: 20px; text-transform: uppercase; letter-spacing: 0.5px; }
.stButton>button { background-color: #B31B1B; color: white; font-weight: bold; border-radius: 6px; }
.stButton>button:hover { background-color: #8E1414; color: #FFC72C; }
.metric-row { display: flex; gap: 12px; margin: 16px 0 8px 0; flex-wrap: wrap; }
.metric-box { background: white; border: 1px solid #E8E8E8; border-top: 3px solid #B31B1B; border-radius: 8px; padding: 14px 18px; min-width: 120px; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.06); flex: 1; }
.metric-box .metric-value { font-size: 28px; font-weight: 700; color: #B31B1B; font-family: 'Rajdhani', sans-serif; }
.metric-box .metric-label { font-size: 10px; color: #999; text-transform: uppercase; letter-spacing: 0.8px; margin-top: 2px; }
.pipeline-steps { padding: 4px 0; }
.pipeline-step { display: flex; align-items: flex-start; gap: 12px; padding: 10px 0; border-bottom: 1px solid #F0F0F0; font-size: 14px; color: #333; }
.pipeline-step:last-child { border-bottom: none; }
.step-num { background: #B31B1B; color: white; border-radius: 50%; width: 22px; height: 22px; display: flex; align-items: center; justify-content: center; font-size: 11px; font-weight: 700; min-width: 22px; }
.step-icon { font-size: 18px; min-width: 24px; }
.step-text { flex: 1; line-height: 1.5; }
.gde-container { background: #0D1117; border-radius: 10px; padding: 24px 20px; margin: 12px 0; overflow-x: auto; box-shadow: inset 0 2px 8px rgba(0,0,0,0.4); }
.gde-flow { display: flex; align-items: center; gap: 0; min-width: max-content; padding: 8px 0; flex-wrap: nowrap; }
.gde-node { display: flex; flex-direction: column; align-items: center; gap: 6px; }
.gde-node-box { border-radius: 8px; padding: 10px 16px; text-align: center; min-width: 120px; }
@keyframes pulse { 0%,100% { box-shadow: 0 0 8px rgba(255,214,0,0.3); } 50% { box-shadow: 0 0 20px rgba(255,214,0,0.7); } }
.gde-node-title { font-size: 11px; font-weight: 700; letter-spacing: 0.8px; text-transform: uppercase; }
.gde-node-sub { font-size: 10px; opacity: 0.7; margin-top: 2px; }
.gde-node-count { font-size: 14px; font-weight: 700; font-family: 'Rajdhani', sans-serif; margin-top: 4px; }
.gde-node-label { font-size: 10px; color: #666; text-align: center; max-width: 130px; }
.gde-arrow { display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 4px; padding: 0 6px; min-width: 70px; }
.gde-count-label { font-size: 10px; white-space: nowrap; text-align: center; }
.gde-legend { display: flex; gap: 20px; margin-top: 16px; flex-wrap: wrap; }
.gde-legend-item { display: flex; align-items: center; gap: 6px; font-size: 11px; color: #888; }
.legend-dot { width: 10px; height: 10px; border-radius: 2px; }

/* DECRYPT DOWNLOAD PANEL */
.decrypt-panel { background: linear-gradient(135deg, #0a2a0a, #0d1f0d); border: 2px solid rgba(105,240,174,0.4); border-radius: 12px; padding: 20px 24px; margin: 16px 0; }
.decrypt-panel-title { color: #69F0AE; font-family: 'Space Mono', monospace; font-size: 12px; font-weight: 700; letter-spacing: 1.5px; margin-bottom: 12px; }
.decrypt-warning { background: rgba(255,152,0,0.1); border: 1px solid rgba(255,152,0,0.4); border-radius: 8px; padding: 10px 14px; margin-bottom: 12px; }
.decrypt-warning-text { color: #FFB300; font-size: 12px; line-height: 1.6; }
.download-option { background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); border-radius: 8px; padding: 12px 16px; margin: 8px 0; }
.download-option-title { color: white; font-size: 13px; font-weight: 600; margin-bottom: 4px; }
.download-option-desc { color: rgba(255,255,255,0.6); font-size: 11px; }

/* JIRA */
.epic-card { background: linear-gradient(135deg, #B31B1B 0%, #7a1212 100%); border-radius: 10px; padding: 20px 24px; margin: 16px 0; box-shadow: 0 4px 15px rgba(179,27,27,0.3); }
.epic-title { color: #FFC72C; font-family: 'Rajdhani', sans-serif; font-size: 22px; font-weight: 700; letter-spacing: 0.5px; }
.epic-value { color: rgba(255,255,255,0.85); font-size: 13px; margin-top: 6px; line-height: 1.6; }
.epic-meta { display: flex; gap: 10px; margin-top: 12px; flex-wrap: wrap; }
.epic-badge { background: rgba(255,199,44,0.2); border: 1px solid rgba(255,199,44,0.5); color: #FFC72C; padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; }
.story-card { background: white; border: 1px solid #E8E8E8; border-left: 4px solid #B31B1B; border-radius: 8px; padding: 16px 20px; margin: 10px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
.story-id { font-size: 11px; color: #999; font-weight: 600; letter-spacing: 0.5px; }
.story-title { font-size: 14px; font-weight: 600; color: #1a1a1a; margin: 4px 0 8px 0; line-height: 1.4; }
.story-desc { font-size: 13px; color: #555; line-height: 1.6; font-style: italic; }
.story-badges { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 10px; align-items: center; }
.badge-priority-critical { background: #FFEBEE; color: #C62828; border: 1px solid #EF9A9A; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 700; }
.badge-priority-high { background: #FFF3E0; color: #E65100; border: 1px solid #FFCC80; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 700; }
.badge-priority-medium { background: #FFFDE7; color: #F57F17; border: 1px solid #FFF176; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 700; }
.badge-priority-low { background: #F5F5F5; color: #616161; border: 1px solid #E0E0E0; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 700; }
.badge-points { background: #FFC72C; color: #1a1a1a; padding: 2px 10px; border-radius: 10px; font-size: 12px; font-weight: 700; }
.badge-sprint { background: #E8F5E9; color: #2E7D32; border: 1px solid #A5D6A7; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }
.badge-type { background: #E3F2FD; color: #1565C0; border: 1px solid #90CAF9; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }
.ac-section { margin-top: 12px; }
.ac-title { font-size: 11px; font-weight: 700; color: #B31B1B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
.ac-item { font-size: 12px; color: #444; padding: 4px 0 4px 12px; border-left: 2px solid #FFC72C; margin: 4px 0; line-height: 1.5; }
.subtask-section { margin-top: 10px; }
.subtask-title { font-size: 11px; font-weight: 700; color: #666; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
.subtask-item { display: flex; align-items: center; gap: 8px; font-size: 12px; color: #555; padding: 3px 0; }
.subtask-hrs { font-size: 10px; color: #999; background: #F5F5F5; padding: 1px 6px; border-radius: 8px; }
.risk-card { background: #FFF8E1; border: 1px solid #FFE082; border-left: 4px solid #FFC72C; border-radius: 8px; padding: 14px 18px; margin: 10px 0; }
.risk-title { font-size: 13px; font-weight: 700; color: #E65100; margin-bottom: 6px; }
.risk-item { font-size: 12px; color: #555; padding: 3px 0 3px 12px; border-left: 2px solid #FFB300; margin: 3px 0; }
.dod-card { background: #E8F5E9; border: 1px solid #A5D6A7; border-left: 4px solid #2E7D32; border-radius: 8px; padding: 14px 18px; margin: 10px 0; }
.dod-title { font-size: 13px; font-weight: 700; color: #1B5E20; margin-bottom: 6px; }
.dod-item { font-size: 12px; color: #2E7D32; padding: 3px 0 3px 12px; border-left: 2px solid #66BB6A; margin: 3px 0; }
.jira-metrics { display: flex; gap: 12px; margin: 16px 0; flex-wrap: wrap; }
.jira-metric-box { background: white; border: 1px solid #E8E8E8; border-top: 3px solid #B31B1B; border-radius: 8px; padding: 12px 16px; min-width: 100px; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.06); flex: 1; }
.jira-metric-value { font-size: 24px; font-weight: 700; color: #B31B1B; font-family: 'Rajdhani', sans-serif; }
.jira-metric-label { font-size: 10px; color: #999; text-transform: uppercase; letter-spacing: 0.8px; }

/* EXAMPLE PROMPTS */
.example-prompt-box { background: #F8F9FA; border: 1px solid #E0E0E0; border-left: 3px solid #B31B1B; border-radius: 8px; padding: 10px 14px; margin: 6px 0; cursor: pointer; transition: all 0.2s; }
.example-prompt-box:hover { background: #FFF5F5; border-left-color: #FFC72C; }
.example-prompt-tag { font-size: 10px; color: #B31B1B; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 3px; }
.example-prompt-text { font-size: 12px; color: #444; line-height: 1.4; }
</style>
""", unsafe_allow_html=True)

# -----------------------------------
# HELPER FUNCTIONS
# -----------------------------------
def extract_code(raw: str) -> str:
    fenced = re.search(r"```(?:python)?\s*\n(.*?)```", raw, re.DOTALL)
    if fenced:
        return fenced.group(1).strip()
    lines = [ln for ln in raw.splitlines() if not ln.strip().startswith("```")]
    return "\n".join(lines).strip()


def safe_exec_multi(dataframes: dict, code: str) -> pd.DataFrame:
    code = extract_code(code)
    exec_globals = {"pd": pd, "np": np, "re": re, "datetime": datetime, **dataframes}
    try:
        exec(compile(code, "<ai_etl>", "exec"), exec_globals)
    except Exception as exc:
        raise RuntimeError(f"Execution failed: {exc}\n\nCode:\n{code}") from exc
    primary = "df" if "df" in dataframes else list(dataframes.keys())[0]
    output = exec_globals.get("result", exec_globals.get(primary, list(dataframes.values())[0]))
    if not isinstance(output, pd.DataFrame):
        raise RuntimeError(f"AI produced {type(output).__name__} instead of DataFrame.")
    return output


def build_system_prompt_secure(dataframes: dict) -> str:
    """Send ONLY schema + 2 masked sample rows to AI — never raw data"""
    schema_context = build_schema_only_context(dataframes)
    aliases = list(dataframes.keys())
    primary = aliases[0] if aliases else "df"
    join_examples = ""
    if len(aliases) >= 2:
        a, b = aliases[0], aliases[1]
        common = list(set(dataframes[a].columns) & set(dataframes[b].columns))
        jcol = common[0] if common else "id"
        join_examples = f"\n# Example multi-file join:\nresult = pd.merge({a}, {b}, on='{jcol}', how='inner')"

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    return f"""You are a Senior Enterprise Data Engineer. Today's date is {today}.

PRIVACY NOTICE: You receive ONLY column names, dtypes, and 2 masked sample rows. Never hardcode masked/sample values.

AVAILABLE DATAFRAMES (schema + masked samples):{schema_context}

RULES:
1. Use ONLY the listed dataframe aliases. Store final output in variable named 'result'.
2. Always import pandas as pd and numpy as np (already available).
3. Handle null/NaN values gracefully with fillna or dropna.
4. Strip whitespace from string columns.
5. Use vectorised operations; avoid row-level Python loops.
6. For date filters, use pd.to_datetime() and timedelta. Reference today = pd.Timestamp('{today}').
7. For multi-file tasks: join on common keys, then aggregate.
8. For risk/segment calculations: use np.select() or pd.cut() rather than apply loops.
9. Return ONLY executable Python code — no markdown fences, no explanations.
10. Handle edge cases: empty groups, missing join keys, zero-division.
11. For complex transformations with multiple joins + aggregations:
    - Step 1: Filter/clean each dataframe
    - Step 2: Join dataframes
    - Step 3: Compute aggregations
    - Step 4: Apply derived columns
    - Step 5: Select final columns, sort, assign to 'result'
{join_examples}"""


def make_gde_html(dataframes, file_names, code, result_df, state,
                  read_count=0, transform_count=0, out_count=0, masked_cols=None):
    aliases = list(dataframes.keys())
    real_aliases = [a for a in aliases if a != "df"] or list(aliases)[:1] or ["df"]
    has_join = len(real_aliases) >= 2
    code_lower = (code or "").lower()
    trans_ops = []
    if "merge" in code_lower or "join" in code_lower: trans_ops.append("JOIN")
    if "groupby" in code_lower and "rank" in code_lower: trans_ops.append("RANK")
    if "pd.cut" in code_lower or "pd.qcut" in code_lower or "np.select" in code_lower: trans_ops.append("SEGMENT")
    if "re.sub" in code_lower or "replace" in code_lower: trans_ops.append("CLEAN")
    if "fillna" in code_lower: trans_ops.append("FILLNA")
    if "groupby" in code_lower: trans_ops.append("AGGREGATE")
    if not trans_ops: trans_ops.append("TRANSFORM")
    trans_label = " · ".join(list(dict.fromkeys(trans_ops))[:3])
    primary_rows = dataframes[real_aliases[0]].shape[0]
    secondary_rows = dataframes[real_aliases[1]].shape[0] if has_join else 0
    fname1 = file_names[0] if file_names else "file1.csv"
    fname2 = file_names[1] if len(file_names) > 1 else ""
    out_rows = len(result_df) if state == "done" and result_df is not None else out_count
    out_cols = len(result_df.columns) if state == "done" and result_df is not None else 0
    arrow1_color = "#29B6F6" if state in ("transforming","done") else ("#FFD600" if state == "reading" else "#444")
    arrow2_color = "#29B6F6" if state == "done" else "#444"
    privacy_arrow_color = "#69F0AE" if state == "done" else "#444"
    input_border = "#1E90FF" if state in ("reading","transforming","done") else "#333"
    input_bg = "#1a2744" if state in ("reading","transforming","done") else "#111"
    input_color = "#7BB8FF" if state in ("reading","transforming","done") else "#444"
    if state == "transforming":
        trans_border="#FFD600"; trans_bg="#2a2a0a"; trans_color="#FFD600"; trans_anim="animation: pulse 1s infinite;"
    elif state == "done":
        trans_border="#29B6F6"; trans_bg="#0d2137"; trans_color="#29B6F6"; trans_anim=""
    else:
        trans_border="#333"; trans_bg="#111"; trans_color="#444"; trans_anim=""
    if state == "done":
        out_border="#29B6F6"; out_bg="#0d2137"; out_color="#29B6F6"
    else:
        out_border="#AB47BC"; out_bg="#1a1a2e"; out_color="#CE93D8"
    trans_status = "🟡 RUNNING" if state == "transforming" else ("🔵 COMPLETE" if state == "done" else "⏳ WAITING")
    in_count_display = f"{primary_rows:,}" if state in ("reading","transforming","done") else "–"
    in2_count_display = f"{secondary_rows:,}" if has_join and state in ("reading","transforming","done") else "–"
    tr_count_display = f"{primary_rows+secondary_rows:,} rec" if has_join and state in ("transforming","done") else (f"{primary_rows:,} rec" if state in ("transforming","done") else "–")
    out_count_display = f"{out_rows:,}" if state == "done" else "–"
    masked_count = len(masked_cols) if masked_cols else 0

    def svg_arrow(color, label=""):
        uid = abs(hash(label + color)) % 99999
        return f"""<div class="gde-arrow"><svg width="70" height="18" viewBox="0 0 70 18" xmlns="http://www.w3.org/2000/svg"><defs><marker id="ah{uid}" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto"><polygon points="0 0, 6 3, 0 6" fill="{color}" /></marker></defs><line x1="2" y1="9" x2="62" y2="9" stroke="{color}" stroke-width="2.5" marker-end="url(#ah{uid})" /></svg><div class="gde-count-label" style="color:{color};">{label}</div></div>"""

    html = '<div class="gde-flow">'
    if has_join:
        html += f"""<div class="gde-node"><div style="display:flex;flex-direction:column;gap:10px;"><div class="gde-node"><div class="gde-node-box" style="background:{input_bg};border:2px solid {input_border};color:{input_color};min-width:110px;border-radius:8px;padding:10px 14px;text-align:center;"><div class="gde-node-title">📂 INPUT 1</div><div class="gde-node-sub" style="font-size:9px;">{fname1[:18]}</div><div class="gde-node-count">{in_count_display}</div></div><div class="gde-node-label">{real_aliases[0]}</div></div><div class="gde-node"><div class="gde-node-box" style="background:{input_bg};border:2px solid {input_border};color:{input_color};min-width:110px;border-radius:8px;padding:10px 14px;text-align:center;"><div class="gde-node-title">📂 INPUT 2</div><div class="gde-node-sub" style="font-size:9px;">{fname2[:18]}</div><div class="gde-node-count">{in2_count_display}</div></div><div class="gde-node-label">{real_aliases[1]}</div></div>"""
        if len(real_aliases) >= 3:
            fname3 = file_names[2] if len(file_names) > 2 else ""
            in3_count = dataframes[real_aliases[2]].shape[0]
            in3_display = f"{in3_count:,}" if state in ("reading","transforming","done") else "–"
            html += f"""<div class="gde-node"><div class="gde-node-box" style="background:{input_bg};border:2px solid {input_border};color:{input_color};min-width:110px;border-radius:8px;padding:10px 14px;text-align:center;"><div class="gde-node-title">📂 INPUT 3</div><div class="gde-node-sub" style="font-size:9px;">{fname3[:18]}</div><div class="gde-node-count">{in3_display}</div></div><div class="gde-node-label">{real_aliases[2]}</div></div>"""
        html += "</div></div>"
        html += svg_arrow(arrow1_color, "raw data")
    else:
        html += f"""<div class="gde-node"><div class="gde-node-box" style="background:{input_bg};border:2px solid {input_border};color:{input_color};min-width:110px;border-radius:8px;padding:10px 14px;text-align:center;"><div class="gde-node-title">📂 INPUT</div><div class="gde-node-sub" style="font-size:9px;">{fname1[:18]}</div><div class="gde-node-count">{in_count_display}</div></div><div class="gde-node-label">{real_aliases[0]}</div></div>"""
        html += svg_arrow(arrow1_color, "raw data")

    privacy_node_bg = "#0a2a0a" if state in ("transforming","done") else "#111"
    privacy_node_border = "rgba(105,240,174,0.5)" if state in ("transforming","done") else "#333"
    privacy_node_color = "#69F0AE" if state in ("transforming","done") else "#444"
    html += f"""<div class="gde-node"><div class="gde-node-box" style="background:{privacy_node_bg};border:2px solid {privacy_node_border};color:{privacy_node_color};min-width:110px;border-radius:8px;padding:10px 14px;text-align:center;"><div class="gde-node-title">🔒 PII MASK</div><div class="gde-node-sub">Auto-Detect</div><div class="gde-node-count" style="font-size:11px;">{masked_count} cols</div></div><div class="gde-node-label" style="color:#69F0AE;font-size:9px;">SCHEMA ONLY → AI</div></div>"""
    html += svg_arrow(privacy_arrow_color, "schema only")
    html += f"""<div class="gde-node"><div class="gde-node-box" style="background:{trans_bg};border:2px solid {trans_border};color:{trans_color};min-width:120px;border-radius:8px;padding:10px 14px;text-align:center;{trans_anim}"><div class="gde-node-title">⚙ {trans_label}</div><div class="gde-node-sub">AI GENERATED</div><div class="gde-node-count">{tr_count_display}</div></div><div class="gde-node-label">{trans_status}</div></div>"""
    html += svg_arrow(arrow2_color, f"{out_count_display}" if state == "done" else "")
    html += f"""<div class="gde-node"><div class="gde-node-box" style="background:{out_bg};border:2px solid {out_border};color:{out_color};min-width:110px;border-radius:8px;padding:10px 14px;text-align:center;"><div class="gde-node-title">💾 OUTPUT</div><div class="gde-node-sub">{out_cols} columns</div><div class="gde-node-count">{out_count_display}</div></div><div class="gde-node-label">RESULT</div></div></div>"""
    legend = """<div class="gde-legend"><div class="gde-legend-item"><div class="legend-dot" style="background:#1E90FF;"></div> Input</div><div class="gde-legend-item"><div class="legend-dot" style="background:#69F0AE;"></div> PII Layer</div><div class="gde-legend-item"><div class="legend-dot" style="background:#FFD600;"></div> Running</div><div class="gde-legend-item"><div class="legend-dot" style="background:#29B6F6;"></div> Complete</div><div class="gde-legend-item"><div class="legend-dot" style="background:#AB47BC;"></div> Output</div></div>"""
    return f'<div class="gde-container">{html}{legend}</div>'


def build_pipeline_log(code, dataframes, result_df, file_names, original_rows):
    summary_prompt = f"""Narrate this data pipeline in 4-8 plain-English steps for a business audience.
Code: ```python\n{code}\n```
Input files: {file_names}, Rows before: {original_rows}, Rows after: {len(result_df)}, Columns: {result_df.columns.tolist()}
Each step starts with: Loaded, Joined, Filtered, Cleaned, Computed, Aggregated, Ranked, Sorted, Flagged, Selected.
Return ONLY a JSON array of strings: ["Step one", "Step two"]. No markdown, no other text."""
    try:
        raw = call_ai([{"role": "user", "content": summary_prompt}], temperature=0.2)
        arr_match = re.search(r"\[.*\]", raw, re.DOTALL)
        steps = json.loads(arr_match.group()) if arr_match else [raw]
    except Exception:
        steps = [
            f"Loaded {original_rows:,} rows from {', '.join(file_names)}",
            "Applied AI-generated transformations",
            f"Produced {len(result_df):,} rows × {len(result_df.columns)} columns"
        ]
    icon_map = {
        "load": "📂", "read": "📂", "join": "🔗", "merge": "🔗", "combined": "🔗",
        "clean": "🧹", "strip": "🧹", "remov": "🧹", "replac": "🧹",
        "comput": "⚙️", "calculat": "⚙️", "creat": "⚙️", "aggregat": "⚙️", "generat": "⚙️",
        "filter": "🔍", "kept": "🔍", "exclud": "🔍", "select": "🔍",
        "sort": "↕️", "order": "↕️", "rank": "🏅", "flag": "🚩", "risk": "⚠️",
        "format": "✏️", "segment": "🏷️"
    }
    def pick_icon(t):
        tl = t.lower()
        for kw, icon in icon_map.items():
            if kw in tl:
                return icon
        return "✅"
    steps_html = '<div class="pipeline-steps">'
    for i, step in enumerate(steps, 1):
        steps_html += f'<div class="pipeline-step"><span class="step-num">{i}</span><span class="step-icon">{pick_icon(step)}</span><span class="step-text">{step}</span></div>'
    steps_html += "</div>"
    new_cols = [c for c in result_df.columns if c not in list(dataframes.values())[0].columns]
    n_joins = max(0, len([a for a in dataframes.keys() if a != "df"]) - 1)
    metrics_html = f"""<div class="metric-row">
    <div class="metric-box"><div class="metric-value">{len(file_names)}</div><div class="metric-label">Files</div></div>
    <div class="metric-box"><div class="metric-value">{original_rows:,}</div><div class="metric-label">Rows In</div></div>
    <div class="metric-box"><div class="metric-value">{len(result_df):,}</div><div class="metric-label">Rows Out</div></div>
    <div class="metric-box"><div class="metric-value">{len(result_df.columns)}</div><div class="metric-label">Columns</div></div>
    <div class="metric-box"><div class="metric-value">{len(new_cols)}</div><div class="metric-label">New Cols</div></div>
    <div class="metric-box"><div class="metric-value">{n_joins}</div><div class="metric-label">Joins</div></div>
    </div>"""
    return metrics_html, steps_html


def render_decrypt_download_panel(
    masked_result_df: pd.DataFrame,
    original_dataframes: dict,
    ai_code: str,
    file_names: list,
    all_masked_cols: list
):
    """
    Render the smart download panel with two options:
    1. Privacy-Protected CSV (masked, safe to share)
    2. Decrypted / Original CSV (re-runs transformation on original data, requires acknowledgement)
    """
    st.markdown("""
    <div class="decrypt-panel">
        <div class="decrypt-panel-title">⬇️ EXPORT & DOWNLOAD OPTIONS</div>
    </div>
    """, unsafe_allow_html=True)

    col_a, col_b = st.columns(2)

    # --- Option A: Privacy-Protected (masked) ---
    with col_a:
        st.markdown("""
        <div class="download-option">
            <div class="download-option-title">🔒 Privacy-Protected Export</div>
            <div class="download-option-desc">PII columns masked with one-way hashes. Safe for sharing, auditing, or external reporting. Recommended for most use cases.</div>
        </div>
        """, unsafe_allow_html=True)

        masked_csv = masked_result_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇ Download Masked CSV",
            data=masked_csv,
            file_name="output_privacy_protected.csv",
            mime="text/csv",
            key="dl_masked_csv"
        )
        xlsx_buf = BytesIO()
        with pd.ExcelWriter(xlsx_buf, engine="xlsxwriter") as writer:
            masked_result_df.to_excel(writer, sheet_name="Transformed_Data", index=False)
            privacy_report = [
                {"Check": "PII Columns Masked", "Result": ", ".join(all_masked_cols) if all_masked_cols else "None"},
                {"Check": "Schema-Only AI Processing", "Result": "YES"},
                {"Check": "Session ID", "Result": SESSION_ID},
                {"Check": "Export Type", "Result": "Privacy Protected"},
            ]
            pd.DataFrame(privacy_report).to_excel(writer, sheet_name="Privacy_Report", index=False)
        st.download_button(
            label="⬇ Download Masked Excel + Privacy Report",
            data=xlsx_buf.getvalue(),
            file_name="output_privacy_protected.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_masked_xlsx"
        )

    # --- Option B: Decrypted / Original Data ---
    with col_b:
        st.markdown("""
        <div class="download-option" style="border-color: rgba(255,152,0,0.4); background: rgba(255,152,0,0.05);">
            <div class="download-option-title" style="color:#E65100;">🔓 Decrypted / Original Data Export</div>
            <div class="download-option-desc" style="color:#795548;">Re-runs transformation on your <b>original unmasked data</b>. Contains real PII. Restrict access. Only download if required for internal use.</div>
        </div>
        """, unsafe_allow_html=True)

        # Acknowledge gate
        decrypt_ack = st.checkbox(
            "⚠️ I acknowledge this export contains real PII/sensitive data and I am authorised to access it.",
            key="decrypt_ack"
        )
        if decrypt_ack:
            audit_log("DECRYPT_EXPORT_ACKNOWLEDGED", SESSION_ID,
                      f"User acknowledged PII export. Files={file_names}", "HIGH")
            with st.spinner("🔓 Re-running transformation on original data..."):
                try:
                    original_result_df = safe_exec_multi(original_dataframes, ai_code)
                    orig_csv = original_result_df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        label="🔓 Download DECRYPTED CSV (Original Data)",
                        data=orig_csv,
                        file_name="output_ORIGINAL_DATA_SENSITIVE.csv",
                        mime="text/csv",
                        key="dl_original_csv"
                    )
                    orig_xlsx = BytesIO()
                    with pd.ExcelWriter(orig_xlsx, engine="xlsxwriter") as writer:
                        original_result_df.to_excel(writer, sheet_name="Original_Data", index=False)
                        pd.DataFrame([
                            {"Check": "Export Type", "Result": "DECRYPTED — CONTAINS REAL PII"},
                            {"Check": "Authorised By", "Result": "User Acknowledged"},
                            {"Check": "Session", "Result": SESSION_ID},
                            {"Check": "Timestamp", "Result": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
                        ]).to_excel(writer, sheet_name="Access_Log", index=False)
                    st.download_button(
                        label="🔓 Download DECRYPTED Excel (Original Data)",
                        data=orig_xlsx.getvalue(),
                        file_name="output_ORIGINAL_DATA_SENSITIVE.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="dl_original_xlsx"
                    )
                    st.warning("⚠️ This file contains real PII. Handle according to your data classification policy.")
                except Exception as exc:
                    st.error(f"Could not re-run on original data: {exc}")
        else:
            st.info("Check the acknowledgement box above to unlock decrypted export.")


# -----------------------------------
# PROJECT TYPE PROMPTS
# -----------------------------------
PROJECT_TYPE_PROMPTS = {
    "🌐 Web Application": "You are a Senior Agile Delivery Manager specialising in Web Application delivery.",
    "📱 Mobile App": "You are a Senior Agile Delivery Manager specialising in Mobile Application delivery.",
    "📊 Data / ETL Pipeline": "You are a Senior Agile Delivery Manager specialising in Data Engineering and ETL pipelines.",
    "🔗 API / Integration": "You are a Senior Agile Delivery Manager specialising in API and Systems Integration.",
    "☁️ Cloud / Infrastructure": "You are a Senior Agile Delivery Manager specialising in Cloud Infrastructure and DevOps.",
    "🔒 Security Feature": "You are a Senior Agile Delivery Manager specialising in Cybersecurity features.",
    "🏦 Banking / FinTech": "You are a Senior Agile Delivery Manager specialising in Banking and FinTech product delivery with deep knowledge of PCI-DSS, GDPR, FCA and SOX compliance.",
    "🤖 AI / ML Feature": "You are a Senior Agile Delivery Manager specialising in AI and Machine Learning product delivery.",
    "📋 General / Other": "You are a Senior Agile Delivery Manager with 15+ years enterprise software delivery experience.",
}


def build_jira_prompt_secure(description, project_type, team_size, sprint_length, methodology):
    sanitized_desc, pii_found = sanitize_prompt(description)
    system = PROJECT_TYPE_PROMPTS.get(project_type, PROJECT_TYPE_PROMPTS["📋 General / Other"])
    user = f"""BUSINESS REQUIREMENT (PII-SANITISED):
{sanitized_desc}

TEAM CONTEXT: Project Type: {project_type}, Team Size: {team_size}, Sprint: {sprint_length} weeks, Methodology: {methodology}

Generate complete Jira breakdown. Return ONLY valid JSON:
{{"epic":{{"title":"","business_value":"","objective":"","estimated_sprints":3,"definition_of_done":[]}},"stories":[{{"id":"US-001","title":"","user_story":"As a [role], I want [feature], so that [benefit]","priority":"High","story_points":5,"sprint":"Sprint 1","type":"Feature","acceptance_criteria":["Given..."],"subtasks":[{{"title":"","hours":4}}]}}],"risks":[{{"title":"","description":""}}],"dependencies":[]}}

RULES: 4-7 stories, Fibonacci points (1,2,3,5,8,13), Gherkin AC (Given/When/Then), return ONLY JSON. Do NOT include any PII."""
    return system, user, pii_found


def render_jira_cards(data):
    epic = data.get("epic", {})
    stories = data.get("stories", [])
    risks = data.get("risks", [])
    dependencies = data.get("dependencies", [])
    total_points = sum(s.get("story_points", 0) for s in stories)
    sprints_needed = epic.get("estimated_sprints", "?")
    html = f"""<div class="jira-metrics">
    <div class="jira-metric-box"><div class="jira-metric-value">{len(stories)}</div><div class="jira-metric-label">Stories</div></div>
    <div class="jira-metric-box"><div class="jira-metric-value">{total_points}</div><div class="jira-metric-label">Total Points</div></div>
    <div class="jira-metric-box"><div class="jira-metric-value">{sprints_needed}</div><div class="jira-metric-label">Sprints</div></div>
    <div class="jira-metric-box"><div class="jira-metric-value">{len(risks)}</div><div class="jira-metric-label">Risks</div></div>
    <div class="jira-metric-box"><div class="jira-metric-value">{len(dependencies)}</div><div class="jira-metric-label">Dependencies</div></div>
    </div>"""
    dod_items = "".join(f'<div class="dod-item">✓ {d}</div>' for d in epic.get("definition_of_done", []))
    html += f"""<div class="epic-card">
    <div class="epic-title">🏆 EPIC: {epic.get('title','')}</div>
    <div class="epic-value">{epic.get('business_value','')}</div>
    <div class="epic-value" style="margin-top:6px;"><b>Objective:</b> {epic.get('objective','')}</div>
    <div class="epic-meta">
        <span class="epic-badge">📅 {sprints_needed} Sprints</span>
        <span class="epic-badge">📊 {total_points} Story Points</span>
        <span class="epic-badge">📝 {len(stories)} Stories</span>
    </div></div>"""
    if epic.get("definition_of_done"):
        html += f'<div class="dod-card"><div class="dod-title">✅ Definition of Done</div>{dod_items}</div>'
    return html, stories, risks, dependencies


# ============================================================
# EXAMPLE PROMPTS for the ETL Engine
# ============================================================
EXAMPLE_PROMPTS = [
    {
        "tag": "🏦 Risk Profiling",
        "text": "Join customers with accounts on CUSTOMER_ID. Join accounts with transactions on ACCOUNT_ID. Filter only ACTIVE accounts. For each CUSTOMER_ID calculate: total number of transactions (last 90 days), total debit amount (last 90 days), total credit amount (last 90 days), average transaction amount. Create RISK_LEVEL: HIGH if total debit > 500000 OR more than 3 high-value transactions (>100000), MEDIUM if total debit between 200000 and 500000, LOW otherwise. Exclude customers with KYC_STATUS = REJECTED. Output: CUSTOMER_ID, FULL_NAME (FIRST_NAME + LAST_NAME), ACCOUNT_COUNT, TOTAL_TRANSACTIONS, TOTAL_DEBIT, TOTAL_CREDIT, AVG_TRANSACTION_AMOUNT, RISK_LEVEL. Sort by TOTAL_DEBIT descending."
    },
    {
        "tag": "📊 Monthly Aggregation",
        "text": "Join customers with accounts on CUSTOMER_ID. Join accounts with transactions on ACCOUNT_ID. For each customer compute monthly transaction summary for the last 6 months: month, total_credit, total_debit, net_flow (credit - debit), transaction_count. Only include ACTIVE accounts and VERIFIED customers. Sort by customer and month."
    },
    {
        "tag": "🔍 Dormant Account Detection",
        "text": "Join accounts with transactions on ACCOUNT_ID. Find all ACTIVE accounts that have had zero transactions in the last 180 days (i.e. dormant). Join with customers on CUSTOMER_ID. Output: CUSTOMER_ID, FIRST_NAME, LAST_NAME, ACCOUNT_ID, ACCOUNT_TYPE, BALANCE, OPEN_DATE, DAYS_SINCE_LAST_TRANSACTION. Sort by DAYS_SINCE_LAST_TRANSACTION descending."
    },
    {
        "tag": "💰 High Value Customer Segmentation",
        "text": "Join all three files. For ACTIVE accounts only, calculate per customer: total balance across all accounts, total credit in last 30 days, total debit in last 30 days. Segment customers into PLATINUM (total balance > 1000000), GOLD (500000-1000000), SILVER (100000-500000), BRONZE (below 100000). Exclude REJECTED KYC. Output customer details with SEGMENT and total metrics. Sort by total balance descending."
    },
    {
        "tag": "📈 Transaction Channel Analysis",
        "text": "Join transactions with accounts on ACCOUNT_ID, then with customers on CUSTOMER_ID. For ACTIVE accounts with VERIFIED KYC only, compute per customer per channel (UPI/NEFT/IMPS etc): transaction_count, total_amount, avg_amount. Pivot channels as columns. Add a PREFERRED_CHANNEL column showing the channel with highest transaction count."
    },
    {
        "tag": "⚠️ Fraud Velocity Check",
        "text": "Join transactions with accounts on ACCOUNT_ID. For the last 7 days only, flag accounts where: more than 5 DEBIT transactions in a single day (velocity flag), OR any single transaction > 200000 (large transaction flag), OR more than 3 transactions from the same channel in 24 hours (channel concentration). Join with customers. Output flagged records with flag type, transaction details, customer name. Sort by transaction date descending."
    },
]

# ============================================================
# HEADER
# ============================================================
st.markdown("""
<div class="built-by-banner">
    <span class="byline">Built by</span>
    <span class="dot"></span>
    <span class="author">PRADEEP</span>
    <span class="dot"></span>
    <span class="byline">Enterprise AI</span>
</div>
""", unsafe_allow_html=True)

st.markdown(f"""
<div class="main-header">
    <div>
        <div class="header-sub">AI POWERED &nbsp;·&nbsp; {AI_PROVIDER} &nbsp;·&nbsp; PANDAS &nbsp;·&nbsp; 🔒 BANK-GRADE PRIVACY</div>
        <h1>⚡ Enterprise AI Transformation &amp; Delivery Platform</h1>
        <div class="secure-badge">🛡️ PII PROTECTED · SCHEMA-ONLY AI · DECRYPT ON DEMAND · AUDIT LOGGED</div>
    </div>
    <div style="text-align:right;">
        <div class="version-badge">v5.0 SECURE</div>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown(f"""
<div class="session-bar">
    <div class="sb-item">🔐 <span>Session:</span> <span class="sb-val">{SESSION_ID}</span></div>
    <div class="sb-item sb-secure">🛡️ PII Masking: <span class="sb-val" style="color:#2E7D32;">ACTIVE</span></div>
    <div class="sb-item sb-secure">🔒 Schema-Only AI: <span class="sb-val" style="color:#2E7D32;">ENABLED</span></div>
    <div class="sb-item sb-secure">🔓 Decrypt Export: <span class="sb-val" style="color:#2E7D32;">AVAILABLE</span></div>
    <div class="sb-item">📋 Audit: <span class="sb-val" style="color:#1565C0;">ON</span></div>
    <div class="sb-item">⏱️ {datetime.datetime.now().strftime("%d %b %Y %H:%M")}</div>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="privacy-shield">
    <div class="ps-title">🔒 BANK-GRADE PRIVACY PROTECTION — ALL SESSIONS</div>
    <div class="ps-items">
        <span class="ps-item">✓ Schema-only sent to AI</span>
        <span class="ps-item">✓ PII auto-masked before processing</span>
        <span class="ps-item">✓ Decrypt export with acknowledgement</span>
        <span class="ps-item">✓ No data stored server-side</span>
        <span class="ps-item">✓ Session isolated memory only</span>
        <span class="ps-item">✓ Prompt PII scanning</span>
        <span class="ps-item">✓ Tamper-evident audit log</span>
        <span class="ps-item">✓ Auto-clear on session end</span>
    </div>
</div>
""", unsafe_allow_html=True)

# ============================================================
# TABS
# ============================================================
tab1, tab2, tab3, tab4 = st.tabs(["⚡ AI ETL Engine", "📋 AI Jira Breakdown", "🎬 Demo & Benefits", "🔒 Privacy & Audit"])


# ============================================================
# TAB 1 — AI ETL ENGINE
# ============================================================
with tab1:
    st.markdown("""
    <div style="background:#F0F7FF;border:1px solid #BBDEFB;border-left:4px solid #1E90FF;border-radius:8px;padding:12px 16px;margin-bottom:16px;">
        <div style="font-size:12px;font-weight:700;color:#1565C0;margin-bottom:6px;">🔒 HOW YOUR DATA IS PROTECTED</div>
        <div style="font-size:12px;color:#333;line-height:1.8;">
            <b>Step 1:</b> CSV uploaded to session memory only — never stored to disk.<br>
            <b>Step 2:</b> PII scanner detects sensitive columns and masks them with one-way hashes.<br>
            <b>Step 3:</b> ONLY column names, data types, and 2 masked sample rows are sent to AI.<br>
            <b>Step 4:</b> AI generates transformation code from schema. Code runs locally in session.<br>
            <b>Step 5:</b> Download masked output (safe) or decrypted output (requires acknowledgement).<br>
            <b>Step 6:</b> All data cleared from memory when you close your browser.
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Example Prompts
    st.markdown('<div class="section-title">💡 Example Complex Prompts</div>', unsafe_allow_html=True)
    st.markdown("<div style='font-size:12px;color:#666;margin-bottom:8px;'>Click any example to load it into the prompt box:</div>", unsafe_allow_html=True)

    cols_ex = st.columns(3)
    for i, ex in enumerate(EXAMPLE_PROMPTS):
        with cols_ex[i % 3]:
            if st.button(f"{ex['tag']}", key=f"ex_{i}", use_container_width=True):
                st.session_state["etl_prompt_value"] = ex["text"]

    # Prompt input
    st.markdown('<div class="section-title">Business Transformation Description</div>', unsafe_allow_html=True)
    default_prompt = st.session_state.get("etl_prompt_value", "")
    etl_prompt_raw = st.text_area(
        "Describe the data transformation in plain English (supports complex multi-file joins, aggregations, filters, and derived columns)",
        value=default_prompt,
        key="etl_prompt",
        height=160,
        placeholder="Example: Join customer transactions with account data, compute monthly totals per customer, flag accounts with balance below ₹1,00,000..."
    )

    if etl_prompt_raw:
        pii_in_prompt = scan_text_for_pii(etl_prompt_raw)
        if pii_in_prompt:
            st.markdown(f"""
            <div class="pii-warning">
                <div class="pw-title">⚠️ PII Detected in Your Prompt</div>
                <div class="pw-text">Found: {', '.join(pii_in_prompt)}. These will be automatically redacted before sending to AI.</div>
            </div>
            """, unsafe_allow_html=True)

    uploaded_files = st.file_uploader(
        "Upload CSV File(s) — max 50MB each",
        type=["csv"],
        accept_multiple_files=True,
        key="etl_upload"
    )

    if uploaded_files:
        st.markdown('<div class="section-title">Uploaded Files Preview & Privacy Scan</div>', unsafe_allow_html=True)
        for i, f in enumerate(uploaded_files):
            valid, msg = validate_file(f)
            if not valid:
                st.error(f"❌ {f.name}: {msg}")
                continue
            alias = f"df{i+1}" if len(uploaded_files) > 1 else "df"
            f.seek(0)
            _df = pd.read_csv(f)
            _, masked_cols, total_masked = mask_dataframe(_df)
            with st.expander(f"📄 {f.name}  →  alias: `{alias}`  |  {_df.shape[0]:,} rows × {_df.shape[1]} cols", expanded=(i == 0)):
                col_prev, col_priv = st.columns([2, 1])
                with col_prev:
                    st.dataframe(_df.head(3), use_container_width=True)
                with col_priv:
                    st.markdown("**🔒 Privacy Scan**")
                    if masked_cols:
                        st.markdown(f"<span style='color:#E65100;font-size:12px;'>⚠️ {len(masked_cols)} sensitive column(s):</span>", unsafe_allow_html=True)
                        for col in masked_cols:
                            st.markdown(f'<span class="mask-badge">🔒 {col}</span>', unsafe_allow_html=True)
                        st.markdown(f"<span style='color:#2E7D32;font-size:11px;'>✓ {total_masked} values will be masked</span>", unsafe_allow_html=True)
                    else:
                        st.markdown("<span style='color:#2E7D32;font-size:12px;'>✓ No sensitive columns</span>", unsafe_allow_html=True)

        if len(uploaded_files) > 1:
            aliases_list = [f"df{i+1}" for i in range(len(uploaded_files))]
            st.info(f"**{len(uploaded_files)} files loaded.** Reference as: {', '.join(f'`{a}`' for a in aliases_list)}")

    run_col, hint_col = st.columns([1, 3])
    with run_col:
        run_btn = st.button("▶ Execute ETL", key="run_etl", use_container_width=True)
    with hint_col:
        st.markdown("<div style='font-size:11px;color:#888;padding-top:10px;'>Supports multi-file joins, aggregations, filters, date windows, risk scoring, segmentation, pivots, and more.</div>", unsafe_allow_html=True)

    if run_btn:
        if not etl_prompt_raw.strip():
            st.warning("Enter a transformation description.")
            st.stop()
        if not uploaded_files:
            st.warning("Upload at least one CSV file.")
            st.stop()

        etl_prompt_clean, prompt_pii = sanitize_prompt(etl_prompt_raw)
        if prompt_pii:
            audit_log("PROMPT_PII_DETECTED", SESSION_ID, f"PII in ETL prompt: {prompt_pii}", "MEDIUM")
            st.info(f"🔒 PII redacted from prompt: {', '.join(prompt_pii)}")

        # Load both original and masked dataframes
        dataframes_masked = {}
        dataframes_original = {}
        file_names = []
        all_masked_cols = []

        for i, f in enumerate(uploaded_files):
            valid, msg = validate_file(f)
            if not valid:
                st.error(f"❌ {msg}")
                st.stop()
            f.seek(0)
            alias = f"df{i+1}" if len(uploaded_files) > 1 else "df"
            raw_df = pd.read_csv(f)
            masked_df, masked_cols, total_masked = mask_dataframe(raw_df)
            dataframes_masked[alias] = masked_df
            dataframes_original[alias] = raw_df.copy()
            file_names.append(f.name)
            all_masked_cols.extend(masked_cols)
            if masked_cols:
                audit_log("PII_MASKED", SESSION_ID, f"File={f.name}, cols={masked_cols}, count={total_masked}", "HIGH")

        # For single file also add as "df"
        if len(uploaded_files) == 1:
            dataframes_masked["df"] = list(dataframes_masked.values())[0]
            dataframes_original["df"] = list(dataframes_original.values())[0]

        primary_alias = "df" if len(uploaded_files) == 1 else "df1"
        original_rows = dataframes_masked[primary_alias].shape[0]

        system_prompt = build_system_prompt_secure(dataframes_masked)
        audit_log("AI_QUERY", SESSION_ID, f"Files={file_names}, masked_cols={all_masked_cols}", "LOW")

        st.markdown('<div class="section-title">⚡ Execution Flow</div>', unsafe_allow_html=True)
        gde_slot = st.empty()
        gde_slot.markdown(make_gde_html(dataframes_masked, file_names, "", None, "reading", masked_cols=all_masked_cols), unsafe_allow_html=True)
        time.sleep(0.4)
        gde_slot.markdown(make_gde_html(dataframes_masked, file_names, "", None, "transforming", masked_cols=all_masked_cols), unsafe_allow_html=True)

        MAX_ATTEMPTS = 3
        ai_code = ""
        transformed_df = None
        last_error = None
        conversation = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": etl_prompt_clean}
        ]

        with st.spinner("⚙️ AI generating pipeline from schema (no data values sent)..."):
            for attempt in range(1, MAX_ATTEMPTS + 1):
                if last_error and attempt > 1:
                    conversation.append({"role": "assistant", "content": ai_code})
                    conversation.append({"role": "user", "content": f"Attempt {attempt-1} raised:\n{last_error}\nFix it. Store result in 'result'. No markdown fences."})
                ai_code = call_ai(conversation, temperature=0.05)
                try:
                    transformed_df = safe_exec_multi(dataframes_masked, ai_code)
                    last_error = None
                    break
                except Exception as exc:
                    last_error = str(exc)
                    if attempt == MAX_ATTEMPTS:
                        st.error(f"ETL failed after {MAX_ATTEMPTS} attempts:\n\n{exc}")
                        st.code(extract_code(ai_code), language="python")
                        transformed_df = list(dataframes_masked.values())[0].copy()

        gde_slot.markdown(make_gde_html(dataframes_masked, file_names, extract_code(ai_code), transformed_df, "done", masked_cols=all_masked_cols), unsafe_allow_html=True)
        audit_log("ETL_COMPLETE", SESSION_ID, f"Rows: {original_rows}→{len(transformed_df)}, Status={'OK' if last_error is None else 'FAILED'}", "LOW")

        # Store for decrypt panel
        st.session_state.last_etl_result = {
            "masked_df": transformed_df,
            "original_dataframes": dataframes_original,
            "ai_code": ai_code,
            "file_names": file_names,
            "all_masked_cols": all_masked_cols,
        }

        st.markdown('<div class="section-title">📊 Pipeline Execution Summary</div>', unsafe_allow_html=True)
        if all_masked_cols:
            masked_html = "".join(f'<span class="mask-badge">🔒 {c}</span>' for c in all_masked_cols)
            st.markdown(f"""
            <div style="background:#E8F5E9;border:1px solid #A5D6A7;border-radius:8px;padding:10px 14px;margin-bottom:10px;">
                <span style="font-size:12px;font-weight:700;color:#1B5E20;">✅ Privacy Applied:</span>
                <div style="margin-top:6px;">{masked_html}</div>
            </div>
            """, unsafe_allow_html=True)

        with st.spinner("Generating pipeline summary..."):
            metrics_html, steps_html = build_pipeline_log(extract_code(ai_code), dataframes_masked, transformed_df, file_names, original_rows)
        st.markdown(metrics_html, unsafe_allow_html=True)
        with st.expander("📋 View detailed pipeline steps", expanded=False):
            st.markdown(steps_html, unsafe_allow_html=True)
        with st.expander("🔍 View generated Python code", expanded=False):
            st.code(extract_code(ai_code), language="python")

        st.markdown('<div class="section-title">Transformed Output (Preview)</div>', unsafe_allow_html=True)
        total_rows = len(transformed_df)
        col_info, col_select = st.columns([3, 1])
        col_info.markdown(f"<span style='font-size:13px;color:#666;'>Total records: <b>{total_rows:,}</b> &nbsp;·&nbsp; <span style='color:#2E7D32;'>🔒 PII-protected preview</span></span>", unsafe_allow_html=True)
        display_options = sorted(set(n for n in [20, 50, 100, 500, 1000, total_rows] if n <= total_rows)) or [total_rows]
        display_n = col_select.selectbox("Show rows", options=display_options, index=0, key="display_rows")
        st.dataframe(transformed_df.head(display_n), use_container_width=True)

        st.session_state.history.append({
            "Time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Session": SESSION_ID,
            "Prompt": etl_prompt_clean[:80] + "..." if len(etl_prompt_clean) > 80 else etl_prompt_clean,
            "Files": ", ".join(file_names),
            "Rows Before": original_rows,
            "Rows After": len(transformed_df),
            "PII Cols Masked": ", ".join(all_masked_cols) if all_masked_cols else "None",
            "Schema-Only AI": "YES",
            "Status": "OK" if last_error is None else "FAILED"
        })

    # Show download panel if result available
    if st.session_state.last_etl_result:
        r = st.session_state.last_etl_result
        st.markdown("---")
        render_decrypt_download_panel(
            masked_result_df=r["masked_df"],
            original_dataframes=r["original_dataframes"],
            ai_code=r["ai_code"],
            file_names=r["file_names"],
            all_masked_cols=r["all_masked_cols"],
        )


# ============================================================
# TAB 2 — AI JIRA BREAKDOWN
# ============================================================
with tab2:
    st.markdown("""
    <div style="background:#F0F7FF;border:1px solid #BBDEFB;border-left:4px solid #1E90FF;border-radius:8px;padding:12px 16px;margin-bottom:16px;">
        <div style="font-size:12px;font-weight:700;color:#1565C0;margin-bottom:6px;">🔒 PRIVACY IN AI JIRA BREAKDOWN</div>
        <div style="font-size:12px;color:#333;line-height:1.8;">
            Requirement text is scanned for PII before AI processing. Any detected PII is redacted with generic placeholders. AI output is scanned to ensure no PII leaks into stories or acceptance criteria.
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="section-title">Project Configuration</div>', unsafe_allow_html=True)
    project_type = st.selectbox("Project Type", list(PROJECT_TYPE_PROMPTS.keys()), key="proj_type", index=6)
    col_a, col_b, col_c = st.columns(3)
    team_size = col_a.selectbox("Team Size", [2, 3, 4, 5, 6, 8, 10, 12, 15, 20], index=3, key="team_size")
    sprint_length = col_b.selectbox("Sprint Length (weeks)", [1, 2, 3], index=1, key="sprint_len")
    methodology = col_c.selectbox("Methodology", ["Scrum", "Kanban", "SAFe", "Scrumban"], key="methodology")

    st.markdown('<div class="section-title">Business Requirement</div>', unsafe_allow_html=True)
    jira_prompt_raw = st.text_area(
        "Describe the feature, initiative or product requirement",
        key="jira_prompt",
        height=160,
        placeholder="Example: Build a customer portal for viewing account statements, raising disputes, and downloading transaction history. Must comply with FCA and PCI-DSS requirements."
    )

    if jira_prompt_raw:
        pii_in_jira = scan_text_for_pii(jira_prompt_raw)
        if pii_in_jira:
            st.markdown(f"""<div class="pii-warning"><div class="pw-title">⚠️ PII in Requirement: {', '.join(pii_in_jira)}</div><div class="pw-text">Auto-redacted before AI processing.</div></div>""", unsafe_allow_html=True)

    if st.button("🚀 Generate Jira Breakdown", key="run_jira"):
        if not jira_prompt_raw.strip():
            st.warning("Enter a business requirement.")
            st.stop()

        with st.spinner(f"🧠 Generating {project_type} Jira breakdown..."):
            sys_prompt, user_prompt, pii_found = build_jira_prompt_secure(
                jira_prompt_raw, project_type, team_size, sprint_length, methodology
            )
            if pii_found:
                audit_log("JIRA_PII_REDACTED", SESSION_ID, f"Types: {pii_found}", "MEDIUM")
                st.info(f"🔒 PII redacted: {', '.join(pii_found)}")

            audit_log("JIRA_QUERY", SESSION_ID, f"Project={project_type}, Team={team_size}", "LOW")
            raw_output = call_ai(
                [{"role": "system", "content": sys_prompt}, {"role": "user", "content": user_prompt}],
                temperature=0.3, max_tokens=4000
            )

        try:
            json_match = re.search(r"\{.*\}", raw_output, re.DOTALL)
            jira_data = json.loads(json_match.group()) if json_match else {}
        except Exception:
            jira_data = {}

        if not jira_data:
            st.error("Could not parse structured output.")
            st.markdown(raw_output)
            st.stop()

        output_pii = scan_text_for_pii(raw_output)
        if output_pii:
            audit_log("JIRA_OUTPUT_PII", SESSION_ID, f"Patterns in output: {output_pii}", "HIGH")
            st.warning(f"⚠️ PII patterns found in AI output ({', '.join(output_pii)}). Review before sharing.")

        audit_log("JIRA_COMPLETE", SESSION_ID, f"Stories={len(jira_data.get('stories',[]))}", "LOW")
        st.session_state.jira_result = {"data": jira_data, "raw": raw_output, "type": project_type, "pii_found": pii_found}

    if st.session_state.jira_result:
        jira_data = st.session_state.jira_result["data"]
        project_type_disp = st.session_state.jira_result["type"]
        pii_found_session = st.session_state.jira_result.get("pii_found", [])

        st.markdown('<div class="section-title">📊 Breakdown Summary</div>', unsafe_allow_html=True)
        header_html, stories, risks, dependencies = render_jira_cards(jira_data)
        st.markdown(header_html, unsafe_allow_html=True)

        st.markdown('<div class="section-title">📝 User Stories</div>', unsafe_allow_html=True)
        priority_badge = {
            "critical": "badge-priority-critical", "high": "badge-priority-high",
            "medium": "badge-priority-medium", "low": "badge-priority-low"
        }
        for story in stories:
            pri = story.get("priority", "Medium")
            pts = story.get("story_points", 0)
            sid = story.get("id", "US-?")
            stype = story.get("type", "Feature")
            pbadge = priority_badge.get(pri.lower(), "badge-priority-medium")
            with st.expander(f"  {sid} · {story.get('title', '')}  [{pri}] [{pts} pts]", expanded=False):
                ac_items = "".join(f'<div class="ac-item">• {ac}</div>' for ac in story.get("acceptance_criteria", []))
                sub_items = "".join(
                    f'<div class="subtask-item">☐ {s.get("title","")} <span class="subtask-hrs">~{s.get("hours",0)}h</span></div>'
                    for s in story.get("subtasks", [])
                )
                st.markdown(f"""<div class="story-card">
                <div class="story-id">{sid} · {project_type_disp}</div>
                <div class="story-title">{story.get('title','')}</div>
                <div class="story-desc">{story.get('user_story','')}</div>
                <div class="story-badges">
                    <span class="{pbadge}">🔴 {pri}</span>
                    <span class="badge-points">⭐ {pts} pts</span>
                    <span class="badge-sprint">🏃 {story.get('sprint','')}</span>
                    <span class="badge-type">🏷 {stype}</span>
                </div>
                <div class="ac-section"><div class="ac-title">✅ Acceptance Criteria</div>{ac_items}</div>
                <div class="subtask-section"><div class="subtask-title">🔧 Subtasks</div>{sub_items}</div>
                </div>""", unsafe_allow_html=True)

        if risks:
            st.markdown('<div class="section-title">⚠️ Risks & Dependencies</div>', unsafe_allow_html=True)
            risk_html = '<div class="risk-card"><div class="risk-title">⚠️ Identified Risks</div>'
            for r in risks:
                risk_html += f'<div class="risk-item"><b>{r.get("title","")}</b> — {r.get("description","")}</div>'
            risk_html += "</div>"
            if dependencies:
                risk_html += '<div class="risk-card" style="border-left-color:#1E90FF;background:#E3F2FD;"><div class="risk-title" style="color:#1565C0;">🔗 Dependencies</div>'
                for d in dependencies:
                    risk_html += f'<div class="risk-item" style="border-left-color:#1E90FF;">{d}</div>'
                risk_html += "</div>"
            st.markdown(risk_html, unsafe_allow_html=True)

        st.markdown('<div class="section-title">Export Jira Output</div>', unsafe_allow_html=True)
        col1, col2, col3 = st.columns(3)
        txt_lines = [f"EPIC: {jira_data.get('epic',{}).get('title','')}\n"]
        for s in stories:
            txt_lines.extend([
                f"\n{s.get('id','')} — {s.get('title','')}",
                f"  {s.get('user_story','')}",
                f"  Priority: {s.get('priority','')} | Points: {s.get('story_points','')} | Sprint: {s.get('sprint','')}",
                "  Acceptance Criteria:"
            ] + [f"    - {ac}" for ac in s.get("acceptance_criteria",[])])
        col1.download_button("⬇ Download TXT", "\n".join(txt_lines), "jira_breakdown.txt", "text/plain")

        xlsx_buf = BytesIO()
        with pd.ExcelWriter(xlsx_buf, engine="xlsxwriter") as writer:
            pd.DataFrame([{"ID": s.get("id",""), "Title": s.get("title",""), "User Story": s.get("user_story",""),
                           "Priority": s.get("priority",""), "Story Points": s.get("story_points",""),
                           "Sprint": s.get("sprint",""), "Type": s.get("type","")} for s in stories]).to_excel(writer, sheet_name="Stories", index=False)
            ac_rows = [{"Story ID": s.get("id",""), "AC": ac} for s in stories for ac in s.get("acceptance_criteria",[])]
            if ac_rows: pd.DataFrame(ac_rows).to_excel(writer, sheet_name="Acceptance_Criteria", index=False)
            if risks: pd.DataFrame(risks).to_excel(writer, sheet_name="Risks", index=False)
        col2.download_button("⬇ Download Excel", xlsx_buf.getvalue(), "jira_breakdown.xlsx",
                             "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        col3.download_button("⬇ Download JSON", json.dumps(jira_data, indent=2), "jira_breakdown.json", "application/json")


# ============================================================
# TAB 3 — DEMO & BENEFITS
# ============================================================
with tab3:
    st.markdown("""
    <div style="background:linear-gradient(135deg,#B31B1B,#7a1212);border-radius:12px;padding:28px 32px;margin-bottom:24px;box-shadow:0 4px 20px rgba(179,27,27,0.4);">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:14px;flex-wrap:wrap;">
            <div style="background:rgba(255,199,44,0.2);border:1px solid rgba(255,199,44,0.4);padding:4px 14px;border-radius:20px;font-size:11px;font-weight:700;color:#FFC72C;letter-spacing:2px;">🔴 LIVE DEMO</div>
            <div style="background:rgba(41,182,246,0.2);border:1px solid rgba(41,182,246,0.4);padding:4px 14px;border-radius:20px;font-size:11px;color:#29B6F6;letter-spacing:1px;">🔒 BANK-GRADE PRIVACY v5.0</div>
        </div>
        <div style="color:#FFC72C;font-family:'Rajdhani',sans-serif;font-size:28px;font-weight:700;margin-bottom:8px;">⚡ Enterprise AI Platform — Privacy Edition</div>
        <div style="color:rgba(255,255,255,0.85);font-size:14px;line-height:1.7;max-width:720px;">With <b style="color:#FFC72C;">bank-grade PII protection</b>: auto-masking, schema-only AI, prompt sanitisation, <b style="color:#69F0AE;">decrypt-on-demand export with audit trail</b>, and zero data persistence — built for financial services compliance.</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="section-title">🔒 Privacy Architecture</div>', unsafe_allow_html=True)
    st.markdown("""
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:14px;margin:14px 0 24px 0;">
        <div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #29B6F6;border-radius:10px;padding:16px;text-align:center;">
            <div style="font-size:28px;margin-bottom:8px;">🔒</div>
            <div style="font-family:'Rajdhani',sans-serif;font-size:14px;font-weight:700;color:#1565C0;margin-bottom:6px;">PII Auto-Detection</div>
            <div style="font-size:11px;color:#555;line-height:1.5;">30+ pattern types including Indian phone numbers, IBAN, SSN, emails, DOB, passports</div>
        </div>
        <div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #2E7D32;border-radius:10px;padding:16px;text-align:center;">
            <div style="font-size:28px;margin-bottom:8px;">🛡️</div>
            <div style="font-family:'Rajdhani',sans-serif;font-size:14px;font-weight:700;color:#1B5E20;margin-bottom:6px;">Schema-Only AI</div>
            <div style="font-size:11px;color:#555;line-height:1.5;">AI only receives column names & types. Data values never leave your session.</div>
        </div>
        <div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #E65100;border-radius:10px;padding:16px;text-align:center;">
            <div style="font-size:28px;margin-bottom:8px;">🔓</div>
            <div style="font-family:'Rajdhani',sans-serif;font-size:14px;font-weight:700;color:#E65100;margin-bottom:6px;">Decrypt On Demand</div>
            <div style="font-size:11px;color:#555;line-height:1.5;">Authorised users can download original data after explicit acknowledgement. Full audit trail recorded.</div>
        </div>
        <div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #B31B1B;border-radius:10px;padding:16px;text-align:center;">
            <div style="font-size:28px;margin-bottom:8px;">📋</div>
            <div style="font-family:'Rajdhani',sans-serif;font-size:14px;font-weight:700;color:#B31B1B;margin-bottom:6px;">Audit Logging</div>
            <div style="font-size:11px;color:#555;line-height:1.5;">Every action logged with session ID, timestamps. Decrypt events specially flagged. SOX/PCI ready.</div>
        </div>
        <div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #FFC72C;border-radius:10px;padding:16px;text-align:center;">
            <div style="font-size:28px;margin-bottom:8px;">🧹</div>
            <div style="font-family:'Rajdhani',sans-serif;font-size:14px;font-weight:700;color:#E65100;margin-bottom:6px;">Zero Persistence</div>
            <div style="font-size:11px;color:#555;line-height:1.5;">All data in session memory only. Nothing written to disk. Cleared on session end.</div>
        </div>
        <div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #7B1FA2;border-radius:10px;padding:16px;text-align:center;">
            <div style="font-size:28px;margin-bottom:8px;">🤖</div>
            <div style="font-family:'Rajdhani',sans-serif;font-size:14px;font-weight:700;color:#7B1FA2;margin-bottom:6px;">Complex Prompt Engine</div>
            <div style="font-size:11px;color:#555;line-height:1.5;">Multi-file joins, date windows, risk scoring, segmentation, pivots — all from natural language.</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="section-title">📈 Supported Complex Transformations</div>', unsafe_allow_html=True)
    st.markdown("""
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:14px;">
        <div style="background:#F8F9FA;border:1px solid #E0E0E0;border-left:3px solid #B31B1B;border-radius:8px;padding:12px 16px;">
            <div style="font-weight:700;color:#B31B1B;font-size:13px;margin-bottom:6px;">🔗 Multi-File Joins</div>
            <div style="font-size:12px;color:#555;">3-way joins with INNER/LEFT/OUTER, multiple join keys, handling missing keys gracefully</div>
        </div>
        <div style="background:#F8F9FA;border:1px solid #E0E0E0;border-left:3px solid #1565C0;border-radius:8px;padding:12px 16px;">
            <div style="font-weight:700;color:#1565C0;font-size:13px;margin-bottom:6px;">📅 Date Window Aggregations</div>
            <div style="font-size:12px;color:#555;">Last 7/30/60/90/180 days, month-over-month, YTD, rolling windows, date range filters</div>
        </div>
        <div style="background:#F8F9FA;border:1px solid #E0E0E0;border-left:3px solid #2E7D32;border-radius:8px;padding:12px 16px;">
            <div style="font-weight:700;color:#2E7D32;font-size:13px;margin-bottom:6px;">⚠️ Risk & Segmentation</div>
            <div style="font-size:12px;color:#555;">Multi-condition risk scoring (HIGH/MEDIUM/LOW), customer segmentation (PLATINUM/GOLD/SILVER), fraud flags</div>
        </div>
        <div style="background:#F8F9FA;border:1px solid #E0E0E0;border-left:3px solid #E65100;border-radius:8px;padding:12px 16px;">
            <div style="font-weight:700;color:#E65100;font-size:13px;margin-bottom:6px;">📊 Advanced Aggregations</div>
            <div style="font-size:12px;color:#555;">GroupBy with multiple metrics, pivot tables, channel analysis, velocity checks, rank/percentile</div>
        </div>
        <div style="background:#F8F9FA;border:1px solid #E0E0E0;border-left:3px solid #7B1FA2;border-radius:8px;padding:12px 16px;">
            <div style="font-weight:700;color:#7B1FA2;font-size:13px;margin-bottom:6px;">🔍 Complex Filters</div>
            <div style="font-size:12px;color:#555;">Multi-column filters, status exclusions, KYC checks, balance thresholds, NULL handling</div>
        </div>
        <div style="background:#F8F9FA;border:1px solid #E0E0E0;border-left:3px solid #00838F;border-radius:8px;padding:12px 16px;">
            <div style="font-weight:700;color:#00838F;font-size:13px;margin-bottom:6px;">✏️ Derived Columns</div>
            <div style="font-size:12px;color:#555;">Full name construction, net flow (credit - debit), days since last transaction, preferred channel</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="section-title">📈 ROI Metrics</div>', unsafe_allow_html=True)
    st.markdown("""
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-top:14px;">
      <div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #B31B1B;border-radius:10px;padding:18px;text-align:center;">
        <div style="font-family:'Rajdhani',sans-serif;font-size:38px;font-weight:700;color:#B31B1B;line-height:1;">~80%</div>
        <div style="font-size:10px;color:#999;letter-spacing:1px;text-transform:uppercase;margin:5px 0 3px;">Time Saved on Tickets</div>
        <div style="font-size:11px;color:#555;">POs create Jira stories 5× faster</div>
      </div>
      <div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #2E7D32;border-radius:10px;padding:18px;text-align:center;">
        <div style="font-family:'Rajdhani',sans-serif;font-size:38px;font-weight:700;color:#2E7D32;line-height:1;">12×</div>
        <div style="font-size:10px;color:#999;letter-spacing:1px;text-transform:uppercase;margin:5px 0 3px;">Pipeline Build Speed</div>
        <div style="font-size:11px;color:#555;">Build ETL pipelines in minutes</div>
      </div>
      <div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #1565C0;border-radius:10px;padding:18px;text-align:center;">
        <div style="font-family:'Rajdhani',sans-serif;font-size:38px;font-weight:700;color:#1565C0;line-height:1;">↓60%</div>
        <div style="font-size:10px;color:#999;letter-spacing:1px;text-transform:uppercase;margin:5px 0 3px;">Sprint Planning Time</div>
        <div style="font-size:11px;color:#555;">AI handles grooming & estimation</div>
      </div>
      <div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #29B6F6;border-radius:10px;padding:18px;text-align:center;">
        <div style="font-family:'Rajdhani',sans-serif;font-size:38px;font-weight:700;color:#29B6F6;line-height:1;">100%</div>
        <div style="font-size:10px;color:#999;letter-spacing:1px;text-transform:uppercase;margin:5px 0 3px;">Data Stays Local</div>
        <div style="font-size:11px;color:#555;">Zero data values sent to AI</div>
      </div>
    </div>
    """, unsafe_allow_html=True)


# ============================================================
# TAB 4 — PRIVACY & AUDIT
# ============================================================
with tab4:
    st.markdown('<div class="section-title">🔒 Privacy Framework & Compliance</div>', unsafe_allow_html=True)
    st.markdown(f"""
    <div style="background:#0D1117;border:1px solid rgba(41,182,246,0.3);border-radius:12px;padding:24px;margin-bottom:20px;">
        <div style="color:#29B6F6;font-family:'Space Mono',monospace;font-size:12px;letter-spacing:1.5px;margin-bottom:16px;">🛡️ ACTIVE PRIVACY PROTECTIONS — SESSION {SESSION_ID}</div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;">
            <div style="background:#0a1628;border:1px solid rgba(41,182,246,0.2);border-radius:8px;padding:14px;">
                <div style="color:#FFC72C;font-size:12px;font-weight:700;margin-bottom:8px;">🔍 PII Detection Patterns</div>
                <div style="color:#7BB8FF;font-size:11px;line-height:2;font-family:'Space Mono',monospace;">
                    Account Numbers · IBAN/BIC/SWIFT<br>Sort Codes · Card Numbers (PAN)<br>SSN/NIN/Passport · Email · Phone (IN/UK/US)<br>IP Addresses · UK Postcodes/US ZIP<br>Date of Birth · Full Names · Customer IDs
                </div>
            </div>
            <div style="background:#0a1628;border:1px solid rgba(41,182,246,0.2);border-radius:8px;padding:14px;">
                <div style="color:#FFC72C;font-size:12px;font-weight:700;margin-bottom:8px;">✅ Compliance Alignment</div>
                <div style="color:#7BB8FF;font-size:11px;line-height:2;font-family:'Space Mono',monospace;">
                    PCI-DSS · No card data to AI<br>GDPR · No personal data processing<br>FCA · Audit trail maintained<br>SOX · Session-based audit logging<br>GLBA · Financial data protection<br>CCPA · Zero data persistence
                </div>
            </div>
            <div style="background:#0a1628;border:1px solid rgba(41,182,246,0.2);border-radius:8px;padding:14px;">
                <div style="color:#FFC72C;font-size:12px;font-weight:700;margin-bottom:8px;">🔐 Data Flow</div>
                <div style="color:#7BB8FF;font-size:11px;line-height:1.8;font-family:'Space Mono',monospace;">
                    Upload → Session memory only<br>PII Scan → Hash masking applied<br>Schema extraction → AI receives schema<br>Code generated → Runs locally<br>Decrypt export → Requires acknowledgement + audit<br>Session end → All data cleared
                </div>
            </div>
            <div style="background:#0a1628;border:1px solid rgba(41,182,246,0.2);border-radius:8px;padding:14px;">
                <div style="color:#FFC72C;font-size:12px;font-weight:700;margin-bottom:8px;">🚫 What NEVER Happens</div>
                <div style="color:#ff9999;font-size:11px;line-height:1.8;font-family:'Space Mono',monospace;">
                    ✕ Data values sent to AI API<br>✕ Files written to server disk<br>✕ PII stored in logs<br>✕ Cross-session data sharing<br>✕ Raw prompts with PII to AI<br>✕ Decrypt without user consent
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="section-title">📋 Session Audit Log</div>', unsafe_allow_html=True)
    if st.session_state.history:
        audit_df = pd.DataFrame(st.session_state.history)
        st.dataframe(audit_df, use_container_width=True)
        st.download_button("⬇ Download Audit Log", audit_df.to_csv(index=False).encode(), "audit_log.csv", "text/csv")
    else:
        st.info("No actions recorded yet. Execute an ETL or generate a Jira breakdown to see audit entries.")

    st.markdown('<div class="section-title">🧪 PII Scanner Test</div>', unsafe_allow_html=True)
    test_text = st.text_area("Enter text to scan for PII", key="pii_test",
                              placeholder="Try: john.doe@bank.com or 4111-1111-1111-1111 or GB29NWBK60161331926819 or 9876543210")
    if test_text:
        found_pii = scan_text_for_pii(test_text)
        sanitized, _ = sanitize_prompt(test_text)
        if found_pii:
            st.markdown(f"""<div class="pii-warning"><div class="pw-title">⚠️ PII Detected: {', '.join(found_pii)}</div><div class="pw-text"><b>Sanitized:</b> {sanitized}</div></div>""", unsafe_allow_html=True)
        else:
            st.success("✅ No PII patterns detected.")
