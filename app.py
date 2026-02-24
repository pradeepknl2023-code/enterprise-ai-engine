import streamlit as st
from huggingface_hub import InferenceClient
import pandas as pd
import os
import io
import json

# =====================================================
# PAGE CONFIG
# =====================================================
st.set_page_config(page_title="ReqIntel AI", layout="wide")

# =====================================================
# HEADER
# =====================================================
st.markdown("""
<style>
.page-header {
    width:100%;
    background-color:#E41B17;
    color:#FFD700;
    padding:25px;
    font-size:28px;
    font-weight:700;
}
.sub-header {
    font-size:14px;
    margin-bottom:20px;
}
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="page-header">ReqIntel AI – Hybrid ETL Platform</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">AI Rule Extraction + Deterministic Pandas Transformation Engine</div>', unsafe_allow_html=True)

# =====================================================
# HUGGING FACE SETUP
# =====================================================
HF_TOKEN = os.getenv("HF_TOKEN")

if not HF_TOKEN:
    st.error("HF_TOKEN missing. Add it in Streamlit Cloud → Settings → Secrets.")
    st.stop()

client = InferenceClient(token=HF_TOKEN)

# =====================================================
# INPUT SECTION
# =====================================================
st.markdown("### Business Transformation Description")
business_logic = st.text_area("Describe transformation rules (e.g., flag high amounts above 10000)", height=150)

uploaded_file = st.file_uploader("Upload CSV File", type=["csv"])

# =====================================================
# DATA PROFILING
# =====================================================
def profile_dataframe(df):
    profile = {
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "row_count": len(df)
    }
    return profile

# =====================================================
# DETERMINISTIC CLEANING
# =====================================================
def deterministic_cleaning(df):
    audit = []

    before = len(df)
    df = df.drop_duplicates()
    audit.append(f"Removed {before - len(df)} duplicate rows")

    # Normalize string columns
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()

    # Normalize date columns automatically
    for col in df.columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
            audit.append(f"Standardized date format for {col}")

    return df, audit

# =====================================================
# AI RULE EXTRACTION (STRUCTURED JSON)
# =====================================================
def ai_extract_rules(description, columns):

    prompt = f"""
You are a Senior Data Architect.

Given dataset columns:
{columns}

Extract transformation rules from this description:

{description}

Return ONLY valid JSON:

{{
  "amount_column": "column name if applicable or null",
  "risk_threshold": number or null,
  "high_label": "string",
  "normal_label": "string"
}}

Do not include explanation. JSON only.
"""

    try:
        response = client.chat.completions.create(
            model="Qwen/Qwen2.5-7B-Instruct",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400,
            temperature=0.2
        )

        content = response.choices[0].message.content
        return json.loads(content)

    except:
        return None

# =====================================================
# APPLY AI RULES DETERMINISTICALLY
# =====================================================
def apply_ai_rules(df, rules, audit):

    if not rules:
        audit.append("No AI rules extracted")
        return df, audit

    amount_col = rules.get("amount_column")
    threshold = rules.get("risk_threshold")

    if amount_col and threshold and amount_col in df.columns:

        high_label = rules.get("high_label", "HIGH")
        normal_label = rules.get("normal_label", "NORMAL")

        df["Risk_Flag"] = df[amount_col].apply(
            lambda x: high_label if pd.to_numeric(x, errors="coerce") > threshold else normal_label
        )

        audit.append(f"Risk_Flag created using {amount_col} > {threshold}")

    else:
        audit.append("Risk rule not applied (missing column or threshold)")

    return df, audit

# =====================================================
# MAIN PIPELINE
# =====================================================
if st.button("🚀 Run Hybrid AI + Pandas ETL"):

    if not uploaded_file:
        st.warning("Please upload a CSV file.")
        st.stop()

    df = pd.read_csv(uploaded_file)

    st.subheader("Original Data Preview")
    st.dataframe(df.head())

    # Profile dataset
    profile = profile_dataframe(df)

    st.subheader("Dataset Profile")
    st.json(profile)

    # Deterministic Cleaning
    df_cleaned, audit_log = deterministic_cleaning(df)

    # AI Rule Extraction
    rules = ai_extract_rules(business_logic, profile["columns"]) if business_logic else None

    # Apply AI Rules Deterministically
    df_final, audit_log = apply_ai_rules(df_cleaned, rules, audit_log)

    st.subheader("Final Transformed Data")
    st.dataframe(df_final)

    # CSV Download (Guaranteed Clean Format)
    buffer = io.StringIO()
    df_final.to_csv(buffer, index=False)

    st.download_button(
        label="Download Cleaned CSV",
        data=buffer.getvalue(),
        file_name="cleaned_output.csv",
        mime="text/csv"
    )

    st.subheader("Audit Log")
    for entry in audit_log:
        st.write("•", entry)

    st.subheader("AI Extracted Rules")
    st.json(rules if rules else {"message": "No structured rules extracted"})
