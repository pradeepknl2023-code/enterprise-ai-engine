import streamlit as st
import pandas as pd
import os
import re
import datetime
import numpy as np
from groq import Groq
from io import BytesIO

# -----------------------------------
# PAGE CONFIG
# -----------------------------------
st.set_page_config(page_title="Enterprise AI Platform", layout="wide")

# -----------------------------------
# ENTERPRISE THEME (UNCHANGED)
# -----------------------------------
st.markdown("""
<style>
.main-header {
    background-color: #B31B1B;
    padding: 20px;
    border-radius: 8px;
    margin-bottom: 20px;
}
.main-header h1 {
    color: #FFC72C;
    margin: 0;
}
.section-title {
    color: #B31B1B;
    font-weight: 600;
    font-size: 20px;
    margin-top: 20px;
}
.stButton>button {
    background-color: #B31B1B;
    color: white;
    font-weight: bold;
}
.stButton>button:hover {
    background-color: #8E1414;
    color: #FFC72C;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
<h1>Enterprise AI Transformation & Delivery Platform</h1>
</div>
""", unsafe_allow_html=True)

# -----------------------------------
# GROQ SETUP
# -----------------------------------
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

# -----------------------------------
# SESSION STATE
# -----------------------------------
if "history" not in st.session_state:
    st.session_state.history = []

# ===================================
# RULE-BASED ENGINE
# ===================================

def apply_rule_based(df, prompt):
    text = prompt.lower()

    # ---------------- FILTER > < >= <= = ----------------
    filter_match = re.search(r"(\w+)\s*(>=|<=|>|<|=)\s*([\w\.]+)", text)
    if filter_match:
        col, op, val = filter_match.groups()
        col = col.strip()
        if col.capitalize() in df.columns:
            if val.replace('.', '', 1).isdigit():
                val = float(val)
            if op == ">":
                return df[df[col.capitalize()] > val]
            if op == "<":
                return df[df[col.capitalize()] < val]
            if op == ">=":
                return df[df[col.capitalize()] >= val]
            if op == "<=":
                return df[df[col.capitalize()] <= val]
            if op == "=":
                return df[df[col.capitalize()].astype(str).str.lower().str.strip() == str(val).lower()]

    # ---------------- BETWEEN ----------------
    between_match = re.search(r"(\w+)\s+between\s+(\d+)\s+and\s+(\d+)", text)
    if between_match:
        col, low, high = between_match.groups()
        col = col.capitalize()
        if col in df.columns:
            return df[(df[col] >= float(low)) & (df[col] <= float(high))]

    # ---------------- AGGREGATION ----------------
    if "average" in text or "avg" in text:
        match = re.search(r"average\s+(\w+)\s+by\s+(\w+)", text)
        if match:
            metric, group = match.groups()
            return df.groupby(group.capitalize())[metric.capitalize()].mean().reset_index()

    if "sum" in text or "total" in text:
        match = re.search(r"(sum|total)\s+(\w+)\s+by\s+(\w+)", text)
        if match:
            _, metric, group = match.groups()
            return df.groupby(group.capitalize())[metric.capitalize()].sum().reset_index()

    if "count" in text:
        match = re.search(r"count\s+(\w+)\s+by\s+(\w+)", text)
        if match:
            metric, group = match.groups()
            return df.groupby(group.capitalize())[metric.capitalize()].count().reset_index()

    # ---------------- NEW COLUMN RULE ----------------
    if "create" in text and "column" in text:
        if "salary" in text and "high" in text:
            df["Salary_Level"] = np.where(df["Salary"] >= 85000, "High",
                                  np.where(df["Salary"] >= 75000, "Medium", "Low"))
            return df

    return None  # fallback to AI

# ===================================
# SAFE AI EXECUTION
# ===================================

def safe_exec(df, code):
    code = re.sub(r"```.*?```", "", code, flags=re.DOTALL)
    python_lines = []
    for line in code.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if re.match(r"^(df|pd|np|import|from|\w+.*=)", line):
            python_lines.append(line)
    cleaned_code = "\n".join(python_lines)
    if not cleaned_code:
        return df
    local_env = {"df": df.copy(), "pd": pd, "np": np}
    try:
        exec(cleaned_code, {}, local_env)
    except Exception:
        return df
    return local_env["df"]

# ===================================
# TABS
# ===================================

tab1, tab2 = st.tabs(["AI ETL Engine", "AI Jira Breakdown"])

# ===================================
# ETL TAB
# ===================================

with tab1:

    st.markdown('<div class="section-title">Business Description</div>', unsafe_allow_html=True)
    etl_prompt = st.text_area("Describe data transformation", height=140)
    uploaded_file = st.file_uploader("Upload CSV File", type=["csv"])

    if st.button("Execute ETL"):

        if not etl_prompt or not uploaded_file:
            st.warning("Provide prompt and CSV.")
            st.stop()

        df = pd.read_csv(uploaded_file)
        original_rows = len(df)

        # 1️⃣ Try rule-based first
        transformed_df = apply_rule_based(df.copy(), etl_prompt)

        # 2️⃣ If not handled → AI fallback
        ai_code = ""
        if transformed_df is None:
            system_prompt = f"""
Return ONLY pandas executable code modifying df.
No explanation.
Columns available: {df.columns.tolist()}
"""
            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": etl_prompt}
                ],
                temperature=0.1
            )
            ai_code = response.choices[0].message.content
            transformed_df = safe_exec(df.copy(), ai_code)

        st.subheader("Generated Code (If AI Used)")
        st.code(ai_code if ai_code else "Rule-based engine executed")

        st.subheader("Transformed Output")
        st.dataframe(transformed_df, use_container_width=True)

        st.session_state.history.append({
            "Time": datetime.datetime.now(),
            "Prompt": etl_prompt,
            "Rows Before": original_rows,
            "Rows After": len(transformed_df)
        })

        st.markdown('<div class="section-title">Export Results</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        csv_data = transformed_df.to_csv(index=False).encode("utf-8")
        col1.download_button("Download CSV", csv_data, "etl_output.csv", "text/csv")

        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            transformed_df.to_excel(writer, sheet_name="Transformed_Data", index=False)
            pd.DataFrame(st.session_state.history).to_excel(writer, sheet_name="Audit_Log", index=False)

        col2.download_button("Download Excel", output.getvalue(), "etl_output.xlsx",
                             "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ===================================
# JIRA TAB (UNCHANGED)
# ===================================

with tab2:
    st.markdown('<div class="section-title">Business Description</div>', unsafe_allow_html=True)
    jira_prompt = st.text_area("Describe feature or initiative", height=140)

    if st.button("Generate Jira Breakdown"):

        jira_system_prompt = """
You are a Senior Agile Delivery Manager.
Generate:
- 1 Epic
- Multiple User Stories
- Acceptance Criteria
- Subtasks
Return structured professional format.
"""
        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": jira_system_prompt},
                {"role": "user", "content": jira_prompt}
            ],
            temperature=0.3
        )

        st.subheader("Jira Breakdown")
        st.markdown(response.choices[0].message.content)

# ===================================
# HISTORY
# ===================================

st.markdown("---")
st.markdown('<div class="section-title">Transformation History</div>', unsafe_allow_html=True)

if st.session_state.history:
    st.dataframe(pd.DataFrame(st.session_state.history))
else:
    st.info("No transformations executed yet.")
