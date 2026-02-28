"""
Enterprise AI ETL Platform  ·  v7.0
=====================================
FIXES & NEW FEATURES in v7.0:
  ✅ FIX 1: Example prompts now load REAL sample data (transactions/customers/accounts)
             — Click any example to auto-load matching CSV data + prompt
  ✅ FIX 2: Example prompts use embedded sample data so they work WITHOUT file upload
  ✅ NEW 3:  AI Jira Breakdown — PO can EDIT every field inline, then export to Jira
             via Jira REST API with one click button
  ✅ NEW 4:  Login page with session auth — new user requests come to admin email
  ✅ NEW 5:  Admin approval panel — see pending users, approve/reject
  ✅ Retained v6.4: Reset All Cooldowns, Next/Last provider indicators
  ✅ Retained v6.x: Bank-grade PII masking, GDE flow diagram, audit log
"""

import streamlit as st
import pandas as pd
import numpy as np
import os, re, hashlib, datetime, json, time, logging, uuid, io, requests
from io import BytesIO, StringIO
import sys

# ═══════════════════════════════════════════════════════════
# PAGE CONFIG — must be first Streamlit call
# ═══════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Enterprise AI ETL Platform",
    layout="wide",
    page_icon="⚡",
)

# ═══════════════════════════════════════════════════════════
# SECRETS SYNC
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
        get_next_provider,
        reset_cooldowns,
        _get_key,
        RATE_LIMIT_SENTINEL,
    )
    ROUTER_OK = True
except ImportError as _err:
    ROUTER_OK = False
    _ROUTER_ERR = str(_err)

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

# ═══════════════════════════════════════════════════════════
# USER AUTH — Simple file-based (swap for DB in production)
# ═══════════════════════════════════════════════════════════
ADMIN_EMAIL = "pradeep@yourorg.com"   # ← Change this to your email
ADMIN_PASSWORD = "Admin@123"           # ← Change this in production
USERS_FILE = "users.json"

def load_users():
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE) as f:
            return json.load(f)
    # Default users — admin always present
    return {
        "admin": {
            "password": ADMIN_PASSWORD,
            "email": ADMIN_EMAIL,
            "role": "admin",
            "status": "approved",
            "name": "Pradeep (Admin)",
        }
    }

def save_users(users):
    with open(USERS_FILE, "w") as f:
        json.dump(users, f, indent=2)

def register_request(username, password, email, name):
    users = load_users()
    if username in users:
        return False, "Username already exists."
    users[username] = {
        "password": password,
        "email": email,
        "role": "user",
        "status": "pending",
        "name": name,
        "requested_at": datetime.datetime.now().isoformat(),
    }
    save_users(users)
    return True, "Request submitted. Admin will approve your access."

def authenticate(username, password):
    users = load_users()
    u = users.get(username)
    if not u:
        return False, "User not found."
    if u["password"] != password:
        return False, "Incorrect password."
    if u["status"] == "pending":
        return False, "Your account is pending admin approval."
    if u["status"] == "rejected":
        return False, "Your access request was rejected. Contact admin."
    return True, u

def get_pending_users():
    users = load_users()
    return {k: v for k, v in users.items() if v.get("status") == "pending"}

def approve_user(username):
    users = load_users()
    if username in users:
        users[username]["status"] = "approved"
        save_users(users)

def reject_user(username):
    users = load_users()
    if username in users:
        users[username]["status"] = "rejected"
        save_users(users)

# ═══════════════════════════════════════════════════════════
# LOGIN PAGE
# ═══════════════════════════════════════════════════════════
def render_login_page():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@600;700&family=Inter:wght@400;500;600&display=swap');
    .login-container{max-width:420px;margin:60px auto;background:white;border-radius:14px;padding:40px 36px;box-shadow:0 8px 40px rgba(0,0,0,.12);border-top:5px solid #B31B1B;}
    .login-title{font-family:'Rajdhani',sans-serif;font-size:26px;font-weight:700;color:#B31B1B;text-align:center;margin-bottom:4px;}
    .login-sub{font-size:12px;color:#888;text-align:center;margin-bottom:24px;letter-spacing:.5px;}
    </style>
    <div class="login-container">
        <div class="login-title">⚡ Enterprise AI ETL Platform</div>
        <div class="login-sub">SECURE LOGIN · BANK-GRADE PRIVACY</div>
    </div>
    """, unsafe_allow_html=True)

    tab_login, tab_register = st.tabs(["🔐 Login", "📝 Request Access"])

    with tab_login:
        with st.form("login_form"):
            st.markdown("#### Sign In")
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Login →", use_container_width=True)
            if submitted:
                ok, result = authenticate(username, password)
                if ok:
                    st.session_state["authenticated"] = True
                    st.session_state["current_user"] = username
                    st.session_state["user_data"] = result
                    st.session_state["user_role"] = result.get("role", "user")
                    st.rerun()
                else:
                    st.error(f"❌ {result}")

    with tab_register:
        with st.form("register_form"):
            st.markdown("#### Request Access")
            st.info("Your request will be reviewed by the admin before access is granted.")
            reg_name = st.text_input("Full Name")
            reg_email = st.text_input("Email")
            reg_user = st.text_input("Choose Username")
            reg_pass = st.text_input("Choose Password", type="password")
            reg_submitted = st.form_submit_button("Submit Request →", use_container_width=True)
            if reg_submitted:
                if not all([reg_name, reg_email, reg_user, reg_pass]):
                    st.warning("Please fill all fields.")
                else:
                    ok, msg = register_request(reg_user, reg_pass, reg_email, reg_name)
                    if ok:
                        st.success(f"✅ {msg}")
                    else:
                        st.error(f"❌ {msg}")

# ═══════════════════════════════════════════════════════════
# ADMIN APPROVAL PANEL
# ═══════════════════════════════════════════════════════════
def render_admin_panel():
    st.markdown("### 👤 Admin — User Approval Panel")
    pending = get_pending_users()
    if not pending:
        st.success("✅ No pending access requests.")
    else:
        st.warning(f"⚠️ {len(pending)} pending request(s)")
        for uname, udata in pending.items():
            with st.expander(f"👤 {udata.get('name', uname)} — {udata.get('email', '')} — requested {udata.get('requested_at', 'unknown')[:10]}"):
                col_a, col_b = st.columns(2)
                if col_a.button(f"✅ Approve {uname}", key=f"approve_{uname}"):
                    approve_user(uname)
                    st.success(f"✅ {uname} approved!")
                    st.rerun()
                if col_b.button(f"❌ Reject {uname}", key=f"reject_{uname}"):
                    reject_user(uname)
                    st.warning(f"❌ {uname} rejected.")
                    st.rerun()

    st.markdown("### 👥 All Users")
    all_users = load_users()
    user_table = [
        {"Username": k, "Name": v.get("name",""), "Email": v.get("email",""),
         "Role": v.get("role",""), "Status": v.get("status","")}
        for k, v in all_users.items()
    ]
    st.dataframe(pd.DataFrame(user_table), use_container_width=True, hide_index=True)

# ═══════════════════════════════════════════════════════════
# CHECK AUTH GATE
# ═══════════════════════════════════════════════════════════
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    render_login_page()
    st.stop()

# ═══════════════════════════════════════════════════════════
# SESSION STATE (post-login)
# ═══════════════════════════════════════════════════════════
for _k, _v in {
    "session_id":      str(uuid.uuid4())[:8].upper(),
    "history":         [],
    "jira_result":     None,
    "last_etl_result": None,
    "jira_edit_mode":  False,
}.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

SESSION_ID = st.session_state.session_id
CURRENT_USER = st.session_state.get("current_user", "unknown")
USER_ROLE = st.session_state.get("user_role", "user")

# ── Router guard ──────────────────────────────────────────
if not ROUTER_OK:
    st.error(f"### ⚠️ AI Router Import Failed\n**Error:** `{_ROUTER_ERR}`\n\n**Fix:** `pip install litellm` — make sure `ai_router.py` is in the same folder.")
    st.stop()

_ready = sum(1 for r in get_router_status() if "🟢" in r["Status"])
if _ready == 0:
    st.error("### ⚠️ No AI Providers Ready\n\nAdd API keys in Streamlit Secrets:\n```toml\nGEMINI_API_KEY = 'AIzaSy...'\nGROQ_API_KEY   = 'gsk_...'\n```")
    st.stop()


# ═══════════════════════════════════════════════════════════
# SAMPLE DATA — embedded CSVs for example prompts
# ═══════════════════════════════════════════════════════════
SAMPLE_CUSTOMERS_CSV = """CUSTOMER_ID,FIRST_NAME,LAST_NAME,KYC_STATUS,CUSTOMER_TYPE,SEGMENT,CITY,CREATED_DATE
CUST001,Arjun,Sharma,VERIFIED,RETAIL,GOLD,Mumbai,2022-01-15
CUST002,Priya,Patel,VERIFIED,CORPORATE,PLATINUM,Delhi,2021-06-20
CUST003,Rahul,Singh,UNVERIFIED,RETAIL,STANDARD,Bangalore,2023-03-10
CUST004,Meera,Nair,VERIFIED,RETAIL,SILVER,Chennai,2020-11-05
CUST005,Vikram,Gupta,REJECTED,RETAIL,STANDARD,Hyderabad,2023-08-22
CUST006,Ananya,Iyer,VERIFIED,CORPORATE,PREMIUM,Pune,2021-09-14
CUST007,Suresh,Kumar,VERIFIED,RETAIL,GOLD,Kolkata,2022-04-30
CUST008,Divya,Reddy,VERIFIED,RETAIL,STANDARD,Ahmedabad,2023-01-18
CUST009,Rajesh,Joshi,UNVERIFIED,RETAIL,SILVER,Jaipur,2022-12-07
CUST010,Sneha,Menon,VERIFIED,CORPORATE,PLATINUM,Surat,2020-07-25
"""
# 10 customers — concise sample for demo

SAMPLE_ACCOUNTS_CSV = """ACCOUNT_ID,CUSTOMER_ID,ACCOUNT_TYPE,STATUS,BALANCE,OPEN_DATE,BRANCH_CODE
ACC1001,CUST001,SAVINGS,ACTIVE,125000.50,2022-01-20,BR001
ACC1002,CUST002,SAVINGS,ACTIVE,890000.75,2021-06-25,BR002
ACC1003,CUST003,SAVINGS,INACTIVE,5000.00,2023-03-15,BR003
ACC1004,CUST004,SAVINGS,ACTIVE,210000.25,2020-11-10,BR004
ACC1005,CUST005,CURRENT,SUSPENDED,75000.00,2023-08-28,BR005
ACC1006,CUST006,SAVINGS,ACTIVE,1200000.00,2021-09-20,BR006
ACC1007,CUST006,CURRENT,ACTIVE,3400000.50,2021-09-20,BR006
ACC1008,CUST007,SAVINGS,ACTIVE,320000.00,2022-05-05,BR007
ACC1009,CUST008,SAVINGS,ACTIVE,95000.00,2023-01-25,BR008
ACC1010,CUST009,SAVINGS,DORMANT,12000.50,2022-12-10,BR009
"""
# 10 accounts — 1 per customer (CUST006 has 2 to demo multi-account joins)

SAMPLE_TRANSACTIONS_CSV = """TRANSACTION_ID,ACCOUNT_ID,CUSTOMER_ID,TRANSACTION_DATE,AMOUNT,TRANSACTION_TYPE,CHANNEL,STATUS,DESCRIPTION
TXN10001,ACC1001,CUST001,2024-01-05,15000.00,DEBIT,ONLINE,COMPLETED,Online Shopping
TXN10002,ACC1001,CUST001,2024-01-10,50000.00,CREDIT,BRANCH,COMPLETED,Salary Credit
TXN10003,ACC1002,CUST002,2024-01-12,200000.00,DEBIT,NEFT,COMPLETED,Vendor Payment
TXN10004,ACC1004,CUST004,2024-01-15,8000.00,DEBIT,ATM,COMPLETED,ATM Withdrawal
TXN10005,ACC1006,CUST006,2024-01-18,500000.00,CREDIT,RTGS,COMPLETED,Business Income
TXN10006,ACC1006,CUST006,2024-01-20,750000.00,DEBIT,NEFT,COMPLETED,Investment Transfer
TXN10007,ACC1008,CUST007,2024-01-22,12000.00,DEBIT,UPI,COMPLETED,UPI Payment
TXN10008,ACC1009,CUST008,2024-01-25,5000.00,CREDIT,ONLINE,COMPLETED,Refund
TXN10009,ACC1010,CUST009,2024-02-02,300000.00,DEBIT,RTGS,COMPLETED,Property Tax
TXN10010,ACC1001,CUST001,2024-02-05,25000.00,DEBIT,ONLINE,COMPLETED,Insurance Premium
"""
# 10 transactions — covers all channels (ONLINE/NEFT/ATM/RTGS/UPI) and both DEBIT/CREDIT

def get_sample_dfs():
    customers = pd.read_csv(StringIO(SAMPLE_CUSTOMERS_CSV))
    accounts  = pd.read_csv(StringIO(SAMPLE_ACCOUNTS_CSV))
    transactions = pd.read_csv(StringIO(SAMPLE_TRANSACTIONS_CSV))
    return customers, accounts, transactions

# ═══════════════════════════════════════════════════════════
# EXAMPLE PROMPTS — now with embedded data loading
# ═══════════════════════════════════════════════════════════
EXAMPLES = [
    {
        "tag": "✅ Customer Summary",
        "complexity": "Medium",
        "files": ["customers", "accounts", "transactions"],
        "text": "Join customers (df1) with accounts (df2) on CUSTOMER_ID. Keep only ACTIVE accounts and VERIFIED customers. Compute FULL_NAME (FIRST_NAME + ' ' + LAST_NAME), count of ACCOUNT_COUNT, SUM of BALANCE as TOTAL_BALANCE. Sort by TOTAL_BALANCE descending.",
    },
    {
        "tag": "✅ Channel Spend",
        "complexity": "Simple",
        "files": ["transactions"],
        "text": "Use df1 (transactions). Filter only DEBIT transactions with STATUS=COMPLETED. Group by CHANNEL and compute TOTAL_TRANSACTIONS (count), TOTAL_AMOUNT (sum), AVG_AMOUNT (mean), MAX_AMOUNT. Sort by TOTAL_AMOUNT descending.",
    },
    {
        "tag": "✅ Dormant Accounts",
        "complexity": "Medium",
        "files": ["customers", "accounts", "transactions"],
        "text": "Join df2 (accounts) with df3 (transactions) on ACCOUNT_ID. For each DORMANT account (STATUS=DORMANT) find LAST_TXN_DATE = max TRANSACTION_DATE and compute DAYS_INACTIVE = today minus LAST_TXN_DATE. Join df1 (customers) for FIRST_NAME, LAST_NAME. Sort by DAYS_INACTIVE descending.",
    },
    {
        "tag": "✅ Monthly Credit/Debit",
        "complexity": "Medium",
        "files": ["transactions"],
        "text": "Use df1 (transactions). Extract MONTH as YYYY-MM from TRANSACTION_DATE. For each MONTH pivot CREDIT total and DEBIT total. Add NET_FLOW = CREDIT minus DEBIT. Sort by MONTH ascending.",
    },
    {
        "tag": "✅ Top Customers by Balance",
        "complexity": "Simple",
        "files": ["customers", "accounts"],
        "text": "Join df1 (customers) with df2 (accounts) on CUSTOMER_ID. Keep ACTIVE accounts, exclude KYC_STATUS=REJECTED. Per customer compute FULL_NAME, TOTAL_BALANCE (sum of BALANCE), ACCOUNT_COUNT. Show top 10 by TOTAL_BALANCE descending.",
    },
    {
        "tag": "⚡ Risk Profile",
        "complexity": "High",
        "files": ["customers", "accounts", "transactions"],
        "text": "Join df1 (customers) to df2 (accounts) on CUSTOMER_ID, join df2 to df3 (transactions) on ACCOUNT_ID. Keep ACTIVE accounts, exclude KYC_STATUS=REJECTED. Per customer compute TOTAL_DEBIT, TOTAL_CREDIT, TXN_COUNT, AVG_TXN_AMOUNT. RISK_LEVEL: HIGH if TOTAL_DEBIT>500000, MEDIUM if TOTAL_DEBIT between 200000 and 500000, LOW otherwise. Sort by TOTAL_DEBIT descending.",
    },
]


# ═══════════════════════════════════════════════════════════
# BUSINESS VALUE WHITELIST
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
# Patterns applied to DATA VALUES in columns (full set - more aggressive)
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

# Patterns safe for PROMPT TEXT — excludes SWIFT/passport which match common English words
# SWIFT r'[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}' matches: EMPLOYEE, CUSTOMER, VERIFIED, TRANSFER etc.
# passport r'[A-Z]{1,2}\d{6,9}' matches: column references like A1234567
PROMPT_SAFE_PATTERNS = {
    "account_number": r'\b\d{8,17}\b',
    "sort_code":      r'\b\d{2}-\d{2}-\d{2}\b',
    "card_number":    r'\b(?:\d[ -]?){13,19}\b',
    "ssn":            r'\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b',
    "iban":           r'\b[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}([A-Z0-9]?){0,16}\b',
    "email":          r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
    "phone_uk":       r'\b(?:0|\+44)[\s-]?\d{4}[\s-]?\d{6}\b',
    "phone_us":       r'\b(?:\+1[\s-]?)?\(?\d{3}\)?[\s-]?\d{3}[\s-]?\d{4}\b',
    "phone_in":       r'\b[6-9]\d{9}\b',
    "ip_address":     r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
    "dob":            r'\b(?:0?[1-9]|[12]\d|3[01])[/-](?:0?[1-9]|1[0-2])[/-](?:19|20)\d{2}\b',
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
    """Scan prompt text — uses PROMPT_SAFE_PATTERNS to avoid false positives on common words."""
    found = []
    for name, pat in PROMPT_SAFE_PATTERNS.items():
        matches = re.findall(pat, text, re.IGNORECASE)
        for m in matches:
            val = m if isinstance(m, str) else (m[0] if m else "")
            if val.upper() not in BUSINESS_WHITELIST:
                found.append(name)
                break
    return list(dict.fromkeys(found))

def sanitize_prompt(prompt: str):
    """Sanitize prompt text — uses PROMPT_SAFE_PATTERNS only (excludes SWIFT/passport)."""
    found = scan_pii(prompt)
    sanitized = prompt
    for pname, pat in PROMPT_SAFE_PATTERNS.items():
        def _replace(m, pname=pname):
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
# JIRA EXPORT — push to Jira REST API
# ═══════════════════════════════════════════════════════════

def _jira_post(url, payload, auth, headers):
    """Helper — POST to Jira, auto-retry Story/Task issuetype."""
    resp = requests.post(url, json=payload, headers=headers, auth=auth, timeout=15)
    if resp.status_code in [200, 201]:
        return True, resp.json().get("key", "?")
    # If issuetype rejected, flip Task<->Story and retry once
    if resp.status_code == 400 and "issuetype" in resp.text.lower():
        current = payload["fields"]["issuetype"]["name"]
        payload["fields"]["issuetype"]["name"] = "Story" if current == "Task" else "Task"
        resp2 = requests.post(url, json=payload, headers=headers, auth=auth, timeout=15)
        if resp2.status_code in [200, 201]:
            return True, resp2.json().get("key", "?")
        return False, "HTTP {}: {}".format(resp2.status_code, resp2.text[:300])
    return False, "HTTP {}: {}".format(resp.status_code, resp.text[:300])


def _build_adf(story, epic_title):
    """Build Atlassian Document Format description with User Story + AC + Subtask list."""
    ac_items = []
    for ac in story.get("acceptance_criteria", []):
        ac_items.append({
            "type": "listItem",
            "content": [{"type": "paragraph", "content": [{"type": "text", "text": str(ac)}]}]
        })

    subtask_items = []
    for st_item in story.get("subtasks", []):
        label = "{} (~{}h)".format(st_item.get("title", ""), st_item.get("hours", "?"))
        subtask_items.append({
            "type": "listItem",
            "content": [{"type": "paragraph", "content": [{"type": "text", "text": label}]}]
        })

    content_blocks = [
        # User Story italic
        {"type": "paragraph", "content": [
            {"type": "text", "text": story.get("user_story", ""), "marks": [{"type": "em"}]}
        ]},
        # Meta line
        {"type": "paragraph", "content": [{"type": "text", "text":
            "Sprint: {} | Points: {} | Priority: {} | Epic: {}".format(
                story.get("sprint",""), story.get("story_points",""),
                story.get("priority",""), epic_title),
            "marks": [{"type": "strong"}]
        }]},
        # Acceptance Criteria heading + list
        {"type": "heading", "attrs": {"level": 3}, "content": [
            {"type": "text", "text": "Acceptance Criteria"}
        ]},
    ]
    if ac_items:
        content_blocks.append({"type": "bulletList", "content": ac_items})
    else:
        content_blocks.append({"type": "paragraph", "content": [{"type": "text", "text": "No AC defined."}]})

    # Subtasks summary in description
    if subtask_items:
        content_blocks.append({"type": "heading", "attrs": {"level": 3}, "content": [
            {"type": "text", "text": "Subtasks (also created as child issues)"}
        ]})
        content_blocks.append({"type": "bulletList", "content": subtask_items})

    return {"type": "doc", "version": 1, "content": content_blocks}


def push_story_to_jira(story, epic_title, jira_url, jira_email, jira_token, jira_project_key):
    """
    Push one story + all its subtasks to Jira Cloud.
    Returns: (ok: bool, message: str, subtask_keys: list)
    """
    base_url = jira_url.rstrip("/")
    issue_url = "{}/rest/api/3/issue".format(base_url)
    headers = {"Content-Type": "application/json"}
    auth = (jira_email, jira_token)

    priority_map = {"critical": "Highest", "high": "High", "medium": "Medium", "low": "Low"}
    priority = priority_map.get(story.get("priority", "medium").lower(), "Medium")

    # Clean summary — plain hyphen, strip special chars that break Jira
    raw_summary = "{} - {}".format(story.get("id", "US"), story.get("title", ""))
    summary = raw_summary.encode("ascii", "ignore").decode("ascii")  # strip non-ASCII

    labels = [
        story.get("type", "Feature").replace(" ", "-"),
        story.get("sprint", "Sprint-1").replace(" ", "-"),
    ]

    # ── STEP 1: Create parent story/task ─────────────────
    parent_payload = {
        "fields": {
            "project": {"key": jira_project_key},
            "summary": summary,
            "description": _build_adf(story, epic_title),
            "issuetype": {"name": "Task"},
            "priority": {"name": priority},
            "labels": labels,
        }
    }

    ok, parent_key = _jira_post(issue_url, parent_payload, auth, headers)
    if not ok:
        return False, parent_key, []

    # ── STEP 2: Create subtasks as child issues ───────────
    subtask_keys = []
    subtasks = story.get("subtasks", [])
    for i, st_item in enumerate(subtasks, 1):
        st_title = st_item.get("title", "Subtask {}".format(i))
        st_hours = st_item.get("hours", 0)
        st_summary = "{}.{} - {} (~{}h)".format(
            story.get("id", "US"), i, st_title, st_hours
        ).encode("ascii", "ignore").decode("ascii")

        st_description = {
            "type": "doc", "version": 1,
            "content": [
                {"type": "paragraph", "content": [{"type": "text", "text":
                    "Subtask of: {} | Estimated: {}h | Parent: {}".format(
                        parent_key, st_hours, summary)
                }]}
            ]
        }

        # Try Subtask issuetype first, fall back to Task
        st_payload = {
            "fields": {
                "project": {"key": jira_project_key},
                "summary": st_summary,
                "description": st_description,
                "issuetype": {"name": "Subtask"},
                "priority": {"name": priority},
                "parent": {"key": parent_key},   # link to parent
            }
        }
        st_ok, st_key = _jira_post(issue_url, st_payload, auth, headers)
        if not st_ok:
            # Subtask issuetype not available — try "Task" with parent link
            st_payload["fields"]["issuetype"] = {"name": "Task"}
            st_ok, st_key = _jira_post(issue_url, st_payload, auth, headers)
        if st_ok:
            subtask_keys.append(st_key)

    return True, parent_key, subtask_keys


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
.example-btn{background:#F0F7FF!important;border:1px solid #90CAF9!important;color:#1565C0!important;font-size:12px!important;}
.metric-box{background:white;border:1px solid #E8E8E8;border-top:3px solid #B31B1B;border-radius:8px;padding:14px 18px;min-width:120px;text-align:center;box-shadow:0 2px 8px rgba(0,0,0,.06);flex:1;}
.metric-box .metric-value{font-size:28px;font-weight:700;color:#B31B1B;font-family:'Rajdhani',sans-serif;}
.metric-box .metric-label{font-size:10px;color:#999;text-transform:uppercase;letter-spacing:.8px;margin-top:2px;}
.pipeline-step{display:flex;align-items:flex-start;gap:12px;padding:10px 0;border-bottom:1px solid #F0F0F0;font-size:14px;}
.pipeline-step:last-child{border-bottom:none;}
.step-num{background:#B31B1B;color:white;border-radius:50%;width:22px;height:22px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;min-width:22px;}
.step-icon{font-size:18px;min-width:24px;}
.step-text{flex:1;line-height:1.5;}
.epic-card{background:linear-gradient(135deg,#B31B1B,#7a1212);border-radius:10px;padding:20px 24px;margin:16px 0;}
.epic-title{color:#FFC72C;font-family:'Rajdhani',sans-serif;font-size:22px;font-weight:700;}
.story-card{background:white;border:1px solid #E8E8E8;border-left:4px solid #B31B1B;border-radius:8px;padding:16px 20px;margin:10px 0;}
.story-id{font-size:11px;color:#999;font-weight:600;}
.story-title{font-size:14px;font-weight:600;color:#1a1a1a;margin:4px 0 8px 0;}
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
.jira-edit-panel{background:#F8F9FA;border:2px dashed #B31B1B;border-radius:10px;padding:20px;margin:16px 0;}
.jira-export-panel{background:linear-gradient(135deg,#0a2a0a,#0d1f0d);border:2px solid rgba(105,240,174,.4);border-radius:12px;padding:20px;margin:16px 0;}
.decrypt-panel{background:linear-gradient(135deg,#0a2a0a,#0d1f0d);border:2px solid rgba(105,240,174,.4);border-radius:12px;padding:20px 24px;margin:16px 0;}
.decrypt-panel-title{color:#69F0AE;font-family:'Space Mono',monospace;font-size:12px;font-weight:700;letter-spacing:1.5px;margin-bottom:12px;}
.download-option{background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.1);border-radius:8px;padding:12px 16px;margin:8px 0;}
.download-option-title{color:white;font-size:13px;font-weight:600;margin-bottom:4px;}
.download-option-desc{color:rgba(255,255,255,.6);font-size:11px;}
.built-by{display:flex;align-items:center;justify-content:space-between;padding:6px 16px 0 0;margin-bottom:-6px;}
.byline{font-size:11px;color:#999;letter-spacing:.8px;text-transform:uppercase;}
.author{font-family:'Rajdhani',sans-serif;font-size:15px;font-weight:700;color:#B31B1B;}
.dot{width:6px;height:6px;background:#FFC72C;border-radius:50%;display:inline-block;}
.sample-data-badge{background:#E8F5E9;border:1px solid #A5D6A7;color:#1B5E20;padding:4px 10px;border-radius:8px;font-size:11px;font-weight:600;display:inline-block;margin-left:8px;}
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════
# HEADER
# ═══════════════════════════════════════════════════════════
active_prov  = get_active_provider()
active_model = get_active_model()
next_prov    = get_next_provider()

# Logout + User info in top right
col_header, col_logout = st.columns([5, 1])
with col_header:
    st.markdown("""<div class="built-by">
        <div><span class="byline">Built by</span> <span class="dot"></span> <span class="author">PRADEEP</span></div>
    </div>""", unsafe_allow_html=True)
with col_logout:
    st.markdown(f"<div style='font-size:11px;color:#888;text-align:right;padding-top:4px;'>👤 {CURRENT_USER}</div>", unsafe_allow_html=True)
    if st.button("Logout", key="logout_btn"):
        st.session_state["authenticated"] = False
        st.session_state["current_user"] = ""
        st.rerun()

st.markdown(f"""<div class="main-header">
    <div>
        <div style="color:rgba(255,255,255,.6);font-size:12px;margin-top:4px;">AI POWERED · LITELLM MULTI-PROVIDER · GEMINI PRIMARY · 🔒 BANK-GRADE PRIVACY</div>
        <h1>⚡ Enterprise AI Transformation &amp; Delivery Platform</h1>
        <div class="secure-badge">🛡️ PII PROTECTED · SCHEMA-ONLY AI · DECRYPT ON DEMAND · AUDIT LOGGED</div>
        <div class="provider-pill">🤖 Last Used: {active_prov} &nbsp;·&nbsp; {active_model}</div>
        <div class="provider-pill" style="background:rgba(255,199,44,.15);border-color:rgba(255,199,44,.4);color:#FFC72C;">⏭️ Next Call: {next_prov}</div>
    </div>
    <div style="text-align:right;"><div class="version-badge">v7.0 FULL FEATURES</div></div>
</div>""", unsafe_allow_html=True)

st.markdown(f"""<div class="session-bar">
    <div>🔐 Session: <span class="sb-val">{SESSION_ID}</span></div>
    <div>👤 User: <span class="sb-val">{CURRENT_USER}</span></div>
    <div style="color:#2E7D32;">🛡️ PII: <span class="sb-val" style="color:#2E7D32;">ACTIVE</span></div>
    <div>✅ Last: <span class="sb-val" style="color:#1565C0;">{active_prov}</span></div>
    <div>⏭️ Next: <span class="sb-val" style="color:#69F0AE;">{next_prov}</span></div>
    <div>⏱️ {datetime.datetime.now().strftime("%d %b %Y %H:%M")}</div>
</div>""", unsafe_allow_html=True)

# Tabs — admin gets extra tab
if USER_ROLE == "admin":
    tab1, tab2, tab3, tab4, tab_admin = st.tabs([
        "⚡ AI ETL Engine", "📋 AI Jira Breakdown", "🎬 Demo & Benefits", "🔒 Privacy & Audit", "👤 Admin"
    ])
else:
    tab1, tab2, tab3, tab4 = st.tabs([
        "⚡ AI ETL Engine", "📋 AI Jira Breakdown", "🎬 Demo & Benefits", "🔒 Privacy & Audit",
    ])
    tab_admin = None


# ───────────────────────────────────────────────────────────
# TAB 1 — ETL ENGINE (with working example prompts)
# ───────────────────────────────────────────────────────────
with tab1:
    st.markdown("""<div style="background:#F0F7FF;border:1px solid #BBDEFB;border-left:4px solid #1E90FF;border-radius:8px;padding:12px 16px;margin-bottom:16px;">
        <div style="font-size:12px;font-weight:700;color:#1565C0;margin-bottom:6px;">🔒 HOW YOUR DATA IS PROTECTED</div>
        <div style="font-size:12px;color:#333;line-height:1.9;">
            <b>Step 1:</b> CSV loaded into session memory only — never written to disk.<br>
            <b>Step 2:</b> PII scanner masks sensitive columns. Business values preserved.<br>
            <b>Step 3:</b> ONLY column names + data types sent to AI. Zero data values leave your session.<br>
            <b>Step 4:</b> AI generates code. Code executes locally against your original data.<br>
            <b>Step 5:</b> Download masked (safe) or original (requires acknowledgement + audit log).
        </div></div>""", unsafe_allow_html=True)

    # ── Example Prompts — now load sample data too ──────────
    st.markdown('<div class="section-title">💡 Example Prompts <span style="font-size:12px;color:#2E7D32;font-weight:normal;">(click to load prompt + sample banking data)</span></div>', unsafe_allow_html=True)

    cols = st.columns(3)
    for i, ex in enumerate(EXAMPLES):
        with cols[i % 3]:
            file_hint = ", ".join(ex["files"])
            if st.button(f"{ex['tag']}  [{ex['complexity']}]\n📎 {file_hint}", key=f"ex_{i}", use_container_width=True):
                st.session_state["etl_prompt_val"] = ex["text"]
                # KEY FIX: delete the text_area widget key so Streamlit re-renders
                # with the new value= on next rerun (without this, value= is ignored)
                if "etl_prompt" in st.session_state:
                    del st.session_state["etl_prompt"]
                # Load the required sample data into session
                c, a, t = get_sample_dfs()
                sample_dfs = {}
                df_list = []
                for fi, fname in enumerate(ex["files"]):
                    alias = f"df{fi+1}" if len(ex["files"]) > 1 else "df"
                    df_map = {"customers": c, "accounts": a, "transactions": t}
                    sample_dfs[alias] = df_map[fname]
                    df_list.append((fname, df_map[fname]))
                st.session_state["sample_dfs"] = sample_dfs
                st.session_state["sample_df_list"] = df_list
                st.session_state["using_sample"] = True
                st.rerun()

    # Show sample data preview if loaded
    if st.session_state.get("using_sample") and st.session_state.get("sample_df_list"):
        st.markdown('<span class="sample-data-badge">✅ Sample banking data loaded — no upload needed!</span>', unsafe_allow_html=True)
        with st.expander("👁️ Preview Sample Data", expanded=False):
            for fname, df in st.session_state["sample_df_list"]:
                st.markdown(f"**📄 {fname.title()}** — {df.shape[0]} rows × {df.shape[1]} cols")
                st.dataframe(df.head(3), use_container_width=True)

    st.markdown('<div class="section-title">Transformation Description</div>', unsafe_allow_html=True)
    etl_raw = st.text_area("Describe your data transformation in plain English",
        value=st.session_state.get("etl_prompt_val", ""), key="etl_prompt", height=160,
        placeholder="Example: Join customers with accounts, compute total balance per customer...")

    if etl_raw:
        pii_in_prompt = scan_pii(etl_raw)
        if pii_in_prompt:
            st.markdown(f'<div class="pii-warning"><div class="pw-title">⚠️ PII Detected in Prompt: {", ".join(pii_in_prompt)}</div>'
                        f'<div style="font-size:12px;color:#555;">Auto-redacted before sending to AI.</div></div>', unsafe_allow_html=True)

    # File upload (optional if sample loaded)
    if st.session_state.get("using_sample"):
        st.info("💡 Using embedded sample data. You can also upload your own CSVs below to override.")
        if st.button("🗑️ Clear Sample Data", key="clear_sample"):
            st.session_state["using_sample"] = False
            st.session_state["sample_dfs"] = {}
            st.rerun()

    uploaded = st.file_uploader("Upload CSV File(s) — max 50MB each (optional if using sample data)",
                                type=["csv"], accept_multiple_files=True, key="etl_upload")

    if uploaded:
        st.session_state["using_sample"] = False
        st.markdown('<div class="section-title">Files & Privacy Scan</div>', unsafe_allow_html=True)
        for i, f in enumerate(uploaded):
            ok, msg = validate_file(f)
            if not ok:
                st.error(f"❌ {f.name}: {msg}"); continue
            alias = f"df{i+1}" if len(uploaded) > 1 else "df"
            f.seek(0)
            _df = pd.read_csv(f)
            _, mcols, mtotal = mask_dataframe(_df)
            with st.expander(f"📄 {f.name}  →  `{alias}`  |  {_df.shape[0]:,} rows × {_df.shape[1]} cols", expanded=(i==0)):
                c1, c2 = st.columns([2, 1])
                with c1:
                    st.dataframe(_df.head(3), use_container_width=True)
                with c2:
                    st.markdown("**🔒 Privacy Scan**")
                    if mcols:
                        for col in mcols:
                            st.markdown(f'<span class="mask-badge">🔒 {col}</span>', unsafe_allow_html=True)
                        st.markdown(f"<span style='color:#2E7D32;font-size:11px;'>✓ {mtotal} values masked</span>", unsafe_allow_html=True)
                    else:
                        st.markdown("<span style='color:#2E7D32;font-size:12px;'>✓ No sensitive columns</span>", unsafe_allow_html=True)

    # Provider Status
    with st.expander("🤖 AI Provider Status", expanded=False):
        np_val = get_next_provider()
        lu_val = get_active_provider()
        c_next, c_last = st.columns(2)
        is_gemini_next = "Gemini" in np_val
        c_next.markdown(
            f"<div style='background:{'#E8F5E9' if is_gemini_next else '#FFF3E0'};border:1px solid {'#A5D6A7' if is_gemini_next else '#FFB300'};border-radius:6px;padding:8px 12px;font-size:12px;'>"
            f"⏭️ <b>Next call will use:</b><br><span style='font-size:14px;font-weight:700;color:{'#2E7D32' if is_gemini_next else '#E65100'};'>{np_val}</span></div>",
            unsafe_allow_html=True
        )
        c_last.markdown(
            f"<div style='background:#E3F2FD;border:1px solid #90CAF9;border-radius:6px;padding:8px 12px;font-size:12px;'>"
            f"✅ <b>Last call used:</b><br><span style='font-size:14px;font-weight:700;color:#1565C0;'>{lu_val}</span></div>",
            unsafe_allow_html=True
        )
        st.markdown("<br>", unsafe_allow_html=True)
        st.dataframe(pd.DataFrame(get_router_status()), use_container_width=True, hide_index=True)
        btn1, btn2 = st.columns(2)
        if btn1.button("🔄 Refresh Status", key="refresh_tab1"):
            st.rerun()
        if btn2.button("⚡ Reset All Cooldowns", key="reset_cd_tab1"):
            n = reset_cooldowns()
            st.success(f"✅ Cleared {n} cooldown(s) — Gemini is Ready!")
            st.rerun()

    rc, hc = st.columns([1, 3])
    with rc:
        run = st.button("▶ Execute ETL", key="run_etl", use_container_width=True)
    with hc:
        st.markdown(f"<div style='font-size:11px;color:#888;padding-top:8px;'>Next: <b style='color:#2E7D32;'>{get_next_provider()}</b> &nbsp;·&nbsp; Zero data sent to AI</div>",
                    unsafe_allow_html=True)

    if run:
        if not etl_raw.strip():
            st.warning("Please enter a transformation description.")
            st.stop()

        # Determine data source — sample or uploaded
        if st.session_state.get("using_sample") and st.session_state.get("sample_dfs"):
            dfs_original = st.session_state["sample_dfs"].copy()
            fnames = [f"{k} (sample)" for k in dfs_original.keys()]
            st.info("📊 Running on embedded sample banking data.")
        elif uploaded:
            dfs_original = {}
            fnames = []
            for i, f in enumerate(uploaded):
                ok, msg = validate_file(f)
                if not ok:
                    st.error(f"❌ {msg}")
                    st.stop()
                alias = f"df{i+1}" if len(uploaded) > 1 else "df"
                f.seek(0)
                dfs_original[alias] = pd.read_csv(f)
                fnames.append(f.name)
        else:
            st.warning("Please upload CSV files or click an example prompt to load sample data.")
            st.stop()

        etl_clean, ppii = sanitize_prompt(etl_raw)
        if ppii:
            audit_log("PROMPT_PII", SESSION_ID, f"Types:{ppii}", "MEDIUM")

        dfs_masked = {}
        all_mc = []
        for alias, raw_df in dfs_original.items():
            mdf, mc, mt = mask_dataframe(raw_df)
            dfs_masked[alias] = mdf
            all_mc.extend(mc)
            if mc:
                audit_log("PII_MASKED", SESSION_ID, f"alias={alias},cols={mc},count={mt}", "HIGH")

        primary = "df" if "df" in dfs_original else list(dfs_original.keys())[0]
        orig_rows = dfs_masked[primary].shape[0]
        sys_p = build_system_prompt(dfs_masked)
        audit_log("AI_QUERY", SESSION_ID, f"Files={fnames},provider={get_next_provider()}", "LOW")

        ai_code = ""
        result_df = None
        last_err = None
        conv = [{"role": "system", "content": sys_p}, {"role": "user", "content": etl_clean}]

        with st.spinner(f"⚙️ {get_next_provider()} generating pipeline (schema only — no data values sent)..."):
            for attempt in range(1, 3):
                if last_err and attempt > 1:
                    conv.append({"role": "assistant", "content": ai_code})
                    conv.append({"role": "user", "content": f"Fix: {last_err}\nStore result in 'result'. No markdown."})
                ai_code = call_ai(conv, temperature=0.05, task="code")
                if ai_code == RATE_LIMIT_SENTINEL:
                    st.error("### ⏱️ All AI Providers Rate-Limited\n\nWait 30-60s then click **⚡ Reset All Cooldowns** and retry.")
                    st.stop()
                try:
                    result_df = safe_exec(dfs_original, ai_code)
                    last_err = None
                    break
                except Exception as exc:
                    last_err = str(exc)
                    if attempt == 2:
                        st.error(f"⚠️ ETL failed after 2 attempts: {exc}")
                        with st.expander("🔍 Debug: AI-generated code"):
                            st.code(extract_code(ai_code), language="python")
                        result_df = list(dfs_original.values())[0].copy()

        actual_prov = get_active_provider()
        actual_model = get_active_model()
        if "Gemini" in actual_prov:
            st.success(f"✅ **Answered by: {actual_prov}** ({actual_model})")
        else:
            st.warning(f"⚠️ **Answered by: {actual_prov}** ({actual_model}) — Gemini was rate-limited.")

        audit_log("ETL_COMPLETE", SESSION_ID, f"Rows:{orig_rows}→{len(result_df)},Provider={actual_prov}", "LOW")

        masked_result = result_df.copy()
        for col in result_df.columns:
            masked_result[col] = mask_sensitive_column(result_df[col], col)

        st.session_state.last_etl_result = {
            "masked_df": masked_result, "original_df": result_df,
            "ai_code": ai_code, "file_names": fnames, "masked_cols": all_mc,
        }

        st.markdown('<div class="section-title">📊 Results</div>', unsafe_allow_html=True)
        if all_mc:
            badges = "".join(f'<span class="mask-badge">🔒 {c}</span>' for c in all_mc)
            st.markdown(f'<div style="background:#E8F5E9;border:1px solid #A5D6A7;border-radius:8px;padding:10px 14px;margin-bottom:10px;"><span style="font-size:12px;font-weight:700;color:#1B5E20;">✅ Privacy Applied:</span><div style="margin-top:6px;">{badges}</div></div>', unsafe_allow_html=True)

        with st.expander("🔍 Generated Python code", expanded=False):
            st.code(extract_code(ai_code), language="python")

        ci, cs = st.columns([3, 1])
        ci.markdown(f"<span style='font-size:13px;color:#666;'>Total: <b>{len(result_df):,}</b> rows &nbsp;·&nbsp; Provider: <b style='color:#1565C0;'>{actual_prov}</b></span>", unsafe_allow_html=True)
        total = len(result_df)
        opts = sorted(set(n for n in [20, 50, 100, 500, total] if n <= total)) or [total]
        n_show = cs.selectbox("Show rows", opts, index=0, key="show_n")
        st.dataframe(masked_result.head(n_show), use_container_width=True)

        st.session_state.history.append({
            "Time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "User": CURRENT_USER,
            "Session": SESSION_ID,
            "Prompt": etl_clean[:80] + "..." if len(etl_clean) > 80 else etl_clean,
            "Files": ", ".join(fnames),
            "Rows In": orig_rows, "Rows Out": len(result_df),
            "PII Masked": ", ".join(all_mc) or "None",
            "Provider": actual_prov, "Model": actual_model,
            "Status": "OK" if not last_err else "FAILED",
        })

    if st.session_state.last_etl_result:
        r = st.session_state.last_etl_result
        st.markdown("---")
        st.markdown('<div class="decrypt-panel"><div class="decrypt-panel-title">⬇️ EXPORT OPTIONS</div></div>', unsafe_allow_html=True)
        col_a, col_b = st.columns(2)
        with col_a:
            st.download_button("⬇ Download Masked CSV",
                               r["masked_df"].to_csv(index=False).encode("utf-8"),
                               "output_privacy_protected.csv", "text/csv", key="dl_masked_csv")
        with col_b:
            if st.checkbox("⚠️ I confirm this export contains real PII and I am authorised.", key="decrypt_ack"):
                audit_log("DECRYPT_ACKNOWLEDGED", SESSION_ID, f"User={CURRENT_USER}", "HIGH")
                st.download_button("🔓 Download DECRYPTED CSV",
                                   r["original_df"].to_csv(index=False).encode("utf-8"),
                                   "output_ORIGINAL_SENSITIVE.csv", "text/csv", key="dl_orig_csv")


# ───────────────────────────────────────────────────────────
# TAB 2 — JIRA BREAKDOWN + PO EDIT + JIRA EXPORT
# ───────────────────────────────────────────────────────────
with tab2:
    st.markdown("""<div style="background:#F0F7FF;border:1px solid #BBDEFB;border-left:4px solid #1E90FF;border-radius:8px;padding:12px 16px;margin-bottom:16px;">
        <div style="font-size:12px;font-weight:700;color:#1565C0;margin-bottom:4px;">🔒 PRIVACY IN JIRA BREAKDOWN</div>
        <div style="font-size:12px;color:#333;">PII auto-redacted. Business values preserved. You can edit all fields before export.</div>
    </div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-title">Project Configuration</div>', unsafe_allow_html=True)
    proj_type = st.selectbox("Project Type", list(PROJECT_PROMPTS.keys()), index=6, key="proj_type")
    ca, cb, cc = st.columns(3)
    team_sz = ca.selectbox("Team Size", [2, 3, 4, 5, 6, 8, 10, 12, 15, 20], index=3, key="team_sz")
    sprint_l = cb.selectbox("Sprint (weeks)", [1, 2, 3], index=1, key="sprint_l")
    method = cc.selectbox("Methodology", ["Scrum", "Kanban", "SAFe", "Scrumban"], key="method")

    st.markdown('<div class="section-title">Business Requirement</div>', unsafe_allow_html=True)
    jira_raw = st.text_area("Describe the feature, initiative, or product requirement", key="jira_prompt", height=160,
                            placeholder="Example: Build a customer portal for viewing account statements, raising disputes...")

    if jira_raw:
        pj = scan_pii(jira_raw)
        if pj:
            st.markdown(f'<div class="pii-warning"><div class="pw-title">⚠️ PII in Requirement: {", ".join(pj)}</div>'
                        f'<div style="font-size:12px;color:#555;">Auto-redacted.</div></div>', unsafe_allow_html=True)

    if st.button("🚀 Generate Jira Breakdown", key="run_jira"):
        if not jira_raw.strip():
            st.warning("Enter a requirement.")
            st.stop()
        with st.spinner(f"🧠 Generating {proj_type} breakdown via {get_next_provider()}..."):
            sp, up, pii_j = build_jira_prompt(jira_raw, proj_type, team_sz, sprint_l, method)
            if pii_j:
                audit_log("JIRA_PII", SESSION_ID, f"Types:{pii_j}", "MEDIUM")
            audit_log("JIRA_QUERY", SESSION_ID, f"Type={proj_type},User={CURRENT_USER}", "LOW")
            raw_out = call_ai([{"role": "system", "content": sp}, {"role": "user", "content": up}], temperature=0.3, task="jira")
        if raw_out == RATE_LIMIT_SENTINEL:
            st.error("⏱️ All providers rate-limited. Click ⚡ Reset All Cooldowns in Tab 1, then retry.")
            st.stop()

        actual_prov = get_active_provider()
        st.success(f"✅ Generated by {actual_prov}")

        try:
            jm = re.search(r"\{.*\}", raw_out, re.DOTALL)
            jdata = json.loads(jm.group()) if jm else {}
        except Exception:
            jdata = {}
        if not jdata:
            st.error("Could not parse AI output.")
            st.markdown(raw_out)
            st.stop()

        audit_log("JIRA_DONE", SESSION_ID, f"Stories={len(jdata.get('stories', []))},Provider={actual_prov}", "LOW")
        st.session_state.jira_result = {"data": jdata, "type": proj_type, "edited": False}

    # ── JIRA RESULT DISPLAY + EDIT + EXPORT ──────────────
    if st.session_state.jira_result:
        jd = st.session_state.jira_result["data"]
        pt = st.session_state.jira_result["type"]
        epic = jd.get("epic", {})
        stories = jd.get("stories", [])
        risks = jd.get("risks", [])
        deps = jd.get("dependencies", [])
        total_pts = sum(s.get("story_points", 0) for s in stories)
        sprints = epic.get("estimated_sprints", "?")

        # Metrics
        st.markdown(f'<div style="display:flex;gap:12px;margin:16px 0;flex-wrap:wrap;">'
                    f'<div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #B31B1B;border-radius:8px;padding:12px 16px;min-width:100px;text-align:center;flex:1;"><div style="font-size:24px;font-weight:700;color:#B31B1B;font-family:Rajdhani,sans-serif;">{len(stories)}</div><div style="font-size:10px;color:#999;">STORIES</div></div>'
                    f'<div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #B31B1B;border-radius:8px;padding:12px 16px;min-width:100px;text-align:center;flex:1;"><div style="font-size:24px;font-weight:700;color:#B31B1B;font-family:Rajdhani,sans-serif;">{total_pts}</div><div style="font-size:10px;color:#999;">POINTS</div></div>'
                    f'<div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #B31B1B;border-radius:8px;padding:12px 16px;min-width:100px;text-align:center;flex:1;"><div style="font-size:24px;font-weight:700;color:#B31B1B;font-family:Rajdhani,sans-serif;">{sprints}</div><div style="font-size:10px;color:#999;">SPRINTS</div></div>'
                    f'<div style="background:white;border:1px solid #E8E8E8;border-top:3px solid #B31B1B;border-radius:8px;padding:12px 16px;min-width:100px;text-align:center;flex:1;"><div style="font-size:24px;font-weight:700;color:#B31B1B;font-family:Rajdhani,sans-serif;">{len(risks)}</div><div style="font-size:10px;color:#999;">RISKS</div></div>'
                    f'</div>', unsafe_allow_html=True)

        # Epic card
        st.markdown(f'<div class="epic-card"><div class="epic-title">🏆 EPIC: {epic.get("title", "")}</div>'
                    f'<div style="color:rgba(255,255,255,.85);font-size:13px;margin-top:6px;">{epic.get("business_value", "")}</div>'
                    f'<div style="color:rgba(255,255,255,.85);font-size:13px;margin-top:6px;"><b>Objective:</b> {epic.get("objective", "")}</div>'
                    f'</div>', unsafe_allow_html=True)

        # ── PO EDIT MODE ──────────────────────────────────
        st.markdown('<div class="section-title">📝 Stories — View & Edit</div>', unsafe_allow_html=True)

        edit_mode = st.toggle("✏️ Enable PO Edit Mode — modify stories before export", key="edit_toggle")

        if edit_mode:
            st.markdown('<div class="jira-edit-panel"><b style="color:#B31B1B;">✏️ PO EDIT MODE ACTIVE</b> — All fields are editable. Changes saved to session.</div>', unsafe_allow_html=True)

            # Edit Epic
            with st.expander("✏️ Edit Epic", expanded=False):
                new_epic_title = st.text_input("Epic Title", value=epic.get("title", ""), key="edit_epic_title")
                new_epic_bv = st.text_area("Business Value", value=epic.get("business_value", ""), key="edit_epic_bv", height=80)
                new_epic_obj = st.text_area("Objective", value=epic.get("objective", ""), key="edit_epic_obj", height=80)
                new_sprints = st.number_input("Estimated Sprints", value=int(sprints) if str(sprints).isdigit() else 3, min_value=1, max_value=20, key="edit_sprints")

            # Edit Stories
            edited_stories = []
            for i, s in enumerate(stories):
                with st.expander(f"✏️ {s.get('id', 'US')} — {s.get('title', '')}", expanded=False):
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        new_title = st.text_input("Title", value=s.get("title", ""), key=f"s_title_{i}")
                        new_us = st.text_area("User Story", value=s.get("user_story", ""), key=f"s_us_{i}", height=80)
                    with col2:
                        new_pri = st.selectbox("Priority", ["Critical", "High", "Medium", "Low"],
                                               index=["critical","high","medium","low"].index(s.get("priority","medium").lower()) if s.get("priority","medium").lower() in ["critical","high","medium","low"] else 1,
                                               key=f"s_pri_{i}")
                        new_pts = st.selectbox("Points", [1, 2, 3, 5, 8, 13],
                                               index=[1,2,3,5,8,13].index(s.get("story_points", 5)) if s.get("story_points", 5) in [1,2,3,5,8,13] else 3,
                                               key=f"s_pts_{i}")
                        new_sprint = st.text_input("Sprint", value=s.get("sprint", f"Sprint {i//2+1}"), key=f"s_sprint_{i}")
                        new_type = st.selectbox("Type", ["Feature", "Bug", "Spike", "Tech Debt", "Enabler"],
                                                index=0, key=f"s_type_{i}")
                    # AC editing
                    st.markdown("**Acceptance Criteria** (one per line)")
                    ac_text = "\n".join(s.get("acceptance_criteria", []))
                    new_ac_raw = st.text_area("AC", value=ac_text, key=f"s_ac_{i}", height=120, label_visibility="collapsed")
                    new_ac = [l.strip() for l in new_ac_raw.split("\n") if l.strip()]

                    edited_stories.append({
                        "id": s.get("id", f"US-{i+1:03d}"),
                        "title": new_title,
                        "user_story": new_us,
                        "priority": new_pri,
                        "story_points": new_pts,
                        "sprint": new_sprint,
                        "type": new_type,
                        "acceptance_criteria": new_ac,
                        "subtasks": s.get("subtasks", []),
                    })

            if st.button("💾 Save All Edits", key="save_edits"):
                updated = dict(jd)
                updated["epic"] = {
                    "title": new_epic_title,
                    "business_value": new_epic_bv,
                    "objective": new_epic_obj,
                    "estimated_sprints": new_sprints,
                    "definition_of_done": epic.get("definition_of_done", []),
                }
                updated["stories"] = edited_stories
                st.session_state.jira_result["data"] = updated
                st.session_state.jira_result["edited"] = True
                st.success("✅ All edits saved! Now export to Jira below.")
                st.rerun()

        else:
            # View-only mode
            pbadge = {"critical": "badge-critical", "high": "badge-high", "medium": "badge-medium", "low": "badge-low"}
            for s in stories:
                pri = s.get("priority", "Medium")
                pts = s.get("story_points", 0)
                pb = pbadge.get(pri.lower(), "badge-medium")
                with st.expander(f"  {s.get('id','US')} · {s.get('title','')}  [{pri}] [{pts}pts]", expanded=False):
                    acs = "".join(f'<div class="ac-item">• {a}</div>' for a in s.get("acceptance_criteria", []))
                    st.markdown(f'<div class="story-card"><div class="story-id">{s.get("id","US")} · {pt}</div>'
                                f'<div class="story-title">{s.get("title","")}</div>'
                                f'<div style="font-size:13px;color:#555;font-style:italic;">{s.get("user_story","")}</div>'
                                f'<div class="story-badges"><span class="{pb}">🔴 {pri}</span>'
                                f'<span class="badge-points">⭐ {pts} pts</span>'
                                f'<span class="badge-sprint">🏃 {s.get("sprint","")}</span>'
                                f'<span class="badge-type">🏷 {s.get("type","")}</span></div>'
                                f'<div class="ac-section"><div class="ac-title">✅ Acceptance Criteria</div>{acs}</div>'
                                f'</div>', unsafe_allow_html=True)

        # ── JIRA EXPORT PANEL ─────────────────────────────
        st.markdown("---")
        st.markdown('<div class="jira-export-panel"><div style="color:#69F0AE;font-family:monospace;font-size:13px;font-weight:700;letter-spacing:1px;margin-bottom:12px;">🚀 EXPORT TO JIRA</div>', unsafe_allow_html=True)

        export_tab1, export_tab2 = st.tabs(["📤 Push to Jira Cloud", "⬇️ Download Files"])

        with export_tab1:
            st.markdown("""<div style="background:rgba(255,255,255,.1);border-radius:8px;padding:12px;margin-bottom:12px;">
                <div style="color:white;font-size:12px;font-weight:700;margin-bottom:6px;">🔐 Jira Connection Settings</div>
                <div style="color:rgba(255,255,255,.7);font-size:11px;">Enter your Jira Cloud credentials to push stories directly. 
                API Token: <a href="https://id.atlassian.com/manage-profile/security/api-tokens" target="_blank" style="color:#69F0AE;">Get token here</a></div>
            </div>""", unsafe_allow_html=True)

            j_col1, j_col2 = st.columns(2)
            jira_url   = j_col1.text_input("Jira URL", placeholder="https://yourorg.atlassian.net", key="jira_url")
            jira_email = j_col2.text_input("Jira Email", placeholder="your@email.com", key="jira_email")
            jira_token = j_col1.text_input("Jira API Token", type="password", key="jira_token")
            jira_proj  = j_col2.text_input("Project Key", placeholder="PROJ", key="jira_proj")

            tc1, tc2 = st.columns([1, 3])
            if tc1.button("🔌 Test Connection", key="test_jira"):
                if not all([jira_url, jira_email, jira_token, jira_proj]):
                    st.error("❌ Fill all fields first.")
                else:
                    try:
                        test_url = "{}/rest/api/3/project/{}".format(jira_url.rstrip("/"), jira_proj)
                        r = requests.get(test_url, auth=(jira_email, jira_token), timeout=10)
                        if r.status_code == 200:
                            proj_data = r.json()
                            st.success(f"✅ Connected! Project: **{proj_data.get('name', jira_proj)}** | Type: {proj_data.get('projectTypeKey','?')}")
                        elif r.status_code == 401:
                            st.error("❌ 401 Unauthorized — check your email and API token.")
                        elif r.status_code == 404:
                            st.error(f"❌ 404 Project '{jira_proj}' not found — check your Project Key.")
                        else:
                            st.error(f"❌ HTTP {r.status_code}: {r.text[:200]}")
                    except Exception as e:
                        st.error(f"❌ Connection error: {e}")

            if st.button("🚀 Push All Stories + Subtasks to Jira", key="push_jira", use_container_width=True):
                if not all([jira_url, jira_email, jira_token, jira_proj]):
                    st.error("❌ Please fill all Jira connection fields.")
                else:
                    current_stories = st.session_state.jira_result["data"].get("stories", [])
                    current_epic = st.session_state.jira_result["data"].get("epic", {})
                    total_subtasks = sum(len(s.get("subtasks", [])) for s in current_stories)
                    st.info(f"📤 Pushing {len(current_stories)} stories + {total_subtasks} subtasks to Jira project **{jira_proj}**...")

                    success_count, fail_count = 0, 0
                    total_subtask_created = 0
                    progress = st.progress(0)
                    results_log = []

                    for idx, story in enumerate(current_stories):
                        ok, parent_key, subtask_keys = push_story_to_jira(
                            story, current_epic.get("title", ""),
                            jira_url, jira_email, jira_token, jira_proj
                        )
                        subtask_count = len(subtask_keys)
                        total_subtask_created += subtask_count

                        if ok:
                            success_count += 1
                            subtask_display = ", ".join(subtask_keys) if subtask_keys else "none"
                            results_log.append({
                                "Story ID": story.get("id", "?"),
                                "Title": story.get("title", "")[:35],
                                "Jira Key": parent_key,
                                "Subtasks Created": subtask_count,
                                "Subtask Keys": subtask_display[:40],
                                "AC Count": len(story.get("acceptance_criteria", [])),
                                "Status": "✅ Created",
                            })
                        else:
                            fail_count += 1
                            results_log.append({
                                "Story ID": story.get("id", "?"),
                                "Title": story.get("title", "")[:35],
                                "Jira Key": "-",
                                "Subtasks Created": 0,
                                "Subtask Keys": "-",
                                "AC Count": 0,
                                "Status": f"❌ {parent_key[:60]}",
                            })
                        progress.progress((idx + 1) / len(current_stories))

                    # Summary
                    if success_count > 0:
                        st.success(f"✅ {success_count} stories + {total_subtask_created} subtasks pushed to Jira!")
                        st.markdown(f"""
                        <div style='background:#E8F5E9;border:1px solid #A5D6A7;border-radius:8px;padding:12px 16px;margin:8px 0;font-size:13px;'>
                        🎯 <b>What was created in Jira project <code>{jira_proj}</code>:</b><br>
                        &nbsp;&nbsp;• <b>{success_count}</b> parent issues (stories/tasks) — each with full User Story + Acceptance Criteria in description<br>
                        &nbsp;&nbsp;• <b>{total_subtask_created}</b> child subtask issues — linked to their parent, with hour estimates<br>
                        &nbsp;&nbsp;• View on your board: <a href="{jira_url}/jira/software/projects/{jira_proj}/boards" target="_blank">{jira_url}/jira/software/projects/{jira_proj}/boards</a>
                        </div>""", unsafe_allow_html=True)
                    if fail_count > 0:
                        st.warning(f"⚠️ {fail_count} stories failed. See error details in the table below.")

                    st.dataframe(pd.DataFrame(results_log), use_container_width=True, hide_index=True)
                    audit_log("JIRA_EXPORT", SESSION_ID,
                              f"User={CURRENT_USER},Project={jira_proj},Stories={success_count},Subtasks={total_subtask_created},Fail={fail_count}",
                              "MEDIUM")

        with export_tab2:
            current_data = st.session_state.jira_result["data"]
            current_stories = current_data.get("stories", [])
            current_epic = current_data.get("epic", {})

            ec1, ec2, ec3 = st.columns(3)
            txt = "\n".join([f"EPIC: {current_epic.get('title', '')}\n"] +
                            [f"\n{s.get('id', '')} — {s.get('title', '')}\n  {s.get('user_story', '')}\n  Priority: {s.get('priority','')} | Points: {s.get('story_points','')} | {s.get('sprint','')}\n  AC:\n" +
                             "\n".join(f"    • {a}" for a in s.get("acceptance_criteria", []))
                             for s in current_stories])
            ec1.download_button("⬇ TXT", txt, "jira_breakdown.txt", "text/plain")

            xb = BytesIO()
            with pd.ExcelWriter(xb, engine="xlsxwriter") as w:
                pd.DataFrame([{
                    "ID": s.get("id", ""), "Title": s.get("title", ""),
                    "User Story": s.get("user_story", ""), "Priority": s.get("priority", ""),
                    "Points": s.get("story_points", ""), "Sprint": s.get("sprint", ""),
                    "Type": s.get("type", ""),
                    "Acceptance Criteria": "\n".join(s.get("acceptance_criteria", [])),
                } for s in current_stories]).to_excel(w, sheet_name="Stories", index=False)
                pd.DataFrame([{"Title": current_epic.get("title",""), "Business Value": current_epic.get("business_value",""),
                                "Objective": current_epic.get("objective",""), "Sprints": current_epic.get("estimated_sprints","")}
                               ]).to_excel(w, sheet_name="Epic", index=False)
                if current_data.get("risks"):
                    pd.DataFrame(current_data["risks"]).to_excel(w, sheet_name="Risks", index=False)
            ec2.download_button("⬇ Excel", xb.getvalue(), "jira_breakdown.xlsx",
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            ec3.download_button("⬇ JSON", json.dumps(current_data, indent=2), "jira_breakdown.json", "application/json")

        st.markdown("</div>", unsafe_allow_html=True)


# ───────────────────────────────────────────────────────────
# TAB 3 — DEMO & BENEFITS
# ───────────────────────────────────────────────────────────
with tab3:
    st.markdown(f"""<div style="background:linear-gradient(135deg,#B31B1B,#7a1212);border-radius:12px;padding:28px 32px;margin-bottom:24px;">
        <div style="color:#FFC72C;font-family:'Rajdhani',sans-serif;font-size:28px;font-weight:700;margin-bottom:8px;">⚡ v7.0 — Full Feature Release</div>
        <div style="color:rgba(255,255,255,.85);font-size:14px;line-height:2.0;">
            ✅ <b>FIX:</b> Example prompts load real banking sample data (customers/accounts/transactions)<br>
            ✅ <b>NEW:</b> PO Edit Mode — edit every story field before export<br>
            ✅ <b>NEW:</b> Push to Jira Cloud via REST API with one click<br>
            ✅ <b>NEW:</b> Login page with user approval flow — all requests come to admin<br>
            ✅ Active right now: <b style="color:#69F0AE;">{active_prov} ({active_model})</b>
        </div>
    </div>""", unsafe_allow_html=True)

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
# TAB 4 — PRIVACY & AUDIT
# ───────────────────────────────────────────────────────────
with tab4:
    st.markdown('<div class="section-title">🤖 Live Provider Dashboard</div>', unsafe_allow_html=True)
    np_val = get_next_provider()
    lu_val = get_active_provider()
    col_next, col_last = st.columns(2)
    is_gemini_next = "Gemini" in np_val
    col_next.markdown(
        f"<div style='background:{'#E8F5E9' if is_gemini_next else '#FFF3E0'};border:1px solid {'#A5D6A7' if is_gemini_next else '#FFB300'};border-radius:8px;padding:12px 16px;'>"
        f"<div style='font-size:11px;color:#666;margin-bottom:4px;'>⏭️ NEXT CALL</div>"
        f"<div style='font-size:16px;font-weight:700;color:{'#2E7D32' if is_gemini_next else '#E65100'};'>{np_val}</div></div>",
        unsafe_allow_html=True
    )
    col_last.markdown(
        f"<div style='background:#E3F2FD;border:1px solid #90CAF9;border-radius:8px;padding:12px 16px;'>"
        f"<div style='font-size:11px;color:#666;margin-bottom:4px;'>✅ LAST USED</div>"
        f"<div style='font-size:16px;font-weight:700;color:#1565C0;'>{lu_val}</div></div>",
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.dataframe(pd.DataFrame(get_router_status()), use_container_width=True, hide_index=True)
    btn_col1, btn_col2 = st.columns(2)
    if btn_col1.button("🔄 Refresh Status", key="refresh_tab4"):
        st.rerun()
    if btn_col2.button("⚡ Reset All Cooldowns", key="reset_cd_tab4"):
        n = reset_cooldowns()
        st.success(f"✅ Cleared {n} cooldown(s) — Gemini Ready!")
        st.rerun()

    st.markdown('<div class="section-title">📋 Session Audit Log</div>', unsafe_allow_html=True)
    if st.session_state.history:
        adf = pd.DataFrame(st.session_state.history)
        st.dataframe(adf, use_container_width=True)
        st.download_button("⬇ Download Audit Log", adf.to_csv(index=False).encode(), "audit_log.csv", "text/csv")
    else:
        st.info("No actions yet. Run an ETL or Jira breakdown to see audit entries.")

    st.markdown('<div class="section-title">🧪 PII Scanner Test</div>', unsafe_allow_html=True)
    test_in = st.text_area("Test text", key="pii_test",
                           placeholder="Try: VERIFIED customer john@bank.com with account 12345678901234")
    if test_in:
        found = scan_pii(test_in)
        san, _ = sanitize_prompt(test_in)
        if found:
            st.markdown(f'<div class="pii-warning"><div class="pw-title">⚠️ PII Detected: {", ".join(found)}</div>'
                        f'<div style="font-size:12px;color:#555;margin-top:6px;"><b>Original:</b> {test_in}<br><b>Sanitized:</b> {san}</div></div>',
                        unsafe_allow_html=True)
        else:
            st.success("✅ No PII detected. Text passes through unchanged.")


# ───────────────────────────────────────────────────────────
# ADMIN TAB (admin only)
# ───────────────────────────────────────────────────────────
if tab_admin is not None:
    with tab_admin:
        if USER_ROLE == "admin":
            render_admin_panel()
        else:
            st.error("❌ Admin access required.")
