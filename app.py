import streamlit as st
from huggingface_hub import InferenceClient
import pandas as pd
import os
import io

# =====================================================
# PAGE CONFIG
# =====================================================
st.set_page_config(page_title="ReqIntel AI", layout="wide")

# =====================================================
# ENTERPRISE HEADER
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
st.markdown('<div class="sub-header">Enterprise AI + Deterministic Pandas Transformation Engine</div>', unsafe_allow_html=True)

# =====================================================
# HF SETUP
# =====================================================
HF_TOKEN = os.getenv("HF_TOKEN")

if not HF_TOKEN:
    st.error("HF_TOKEN missing. Add in Streamlit Secrets.")
    st.stop()

client = InferenceClient(token=HF_TOKEN)

# =====================================================
# INPUT SECTION
# =====================================================
st.markdown("### Business Transformation Description")
business_logic = st.text_area("Describe transformation rules", height=150)

uploaded_file = st.file_uploader("Upload CSV File", type=["csv"])

# =====================================================
# HYBRID ETL FUNCTION
# =====================================================
def deterministic_etl(df):
    audit_log = []

    # Remove duplicates
    before = len(df)
    df = df.drop_duplicates()
    audit_log.append(f"Removed {before - len(df)} duplicate records")

    # Remove negative amounts if column exists
    if "Amount" in df.columns:
        before = len(df)
        df = df[df["Amount"] >= 0]
        audit_log.append(f"Removed {before - len(df)} negative amount records")

    # Standardize currency
    if "Currency" in df.columns:
        df["Currency"] = df["Currency"].str.upper()
        audit_log.append("Standardized currency to uppercase")

    # Normalize dates
    for col in df.columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
            audit_log.append(f"Standardized date format for {col}")

    return df, audit_log

# =====================================================
# AI DERIVED LOGIC
# =====================================================
def ai_generate_metadata(description):
    prompt = f"""
You are a Senior Data Architect.

Based on this transformation description:
{description}

Generate:
1. Derived column logic (if any)
2. Risk scoring rules
3. Data quality rules
4. Audit summary

Keep it structured and concise.
"""
    response = client.chat.completions.create(
        model="Qwen/Qwen2.5-7B-Instruct",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=800,
        temperature=0.2
    )

    return response.choices[0].message.content

# =====================================================
# RUN HYBRID PIPELINE
# =====================================================
if st.button("🚀 Run Hybrid AI + Pandas ETL"):

    if not uploaded_file:
        st.warning("Please upload a CSV file.")
        st.stop()

    df = pd.read_csv(uploaded_file)

    st.subheader("Original Data Preview")
    st.dataframe(df.head())

    # Deterministic ETL
    df_cleaned, audit = deterministic_etl(df)

    # AI Layer
    ai_metadata = ai_generate_metadata(business_logic) if business_logic else "No business logic provided."

    st.subheader("Transformed Data")
    st.dataframe(df_cleaned)

    # Download
    csv_buffer = io.StringIO()
    df_cleaned.to_csv(csv_buffer, index=False)

    st.download_button(
        label="Download Cleaned CSV",
        data=csv_buffer.getvalue(),
        file_name="cleaned_output.csv",
        mime="text/csv"
    )

    st.subheader("AI Transformation Insights")
    st.text_area("AI Metadata Output", ai_metadata, height=250)

    st.subheader("Audit Log")
    for log in audit:
        st.write("•", log)
