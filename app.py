import streamlit as st
import pandas as pd
import os
import datetime
import json
import re
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
if not GROQ_API_KEY:
    st.error("Set GROQ_API_KEY in environment.")
    st.stop()

client = Groq(api_key=GROQ_API_KEY)

# -----------------------------------
# SESSION STATE
# -----------------------------------
if "history" not in st.session_state:
    st.session_state.history = []

# -----------------------------------
# SAFE JSON PARSER
# -----------------------------------
def extract_json(text):
    """
    Extracts first JSON block from model response and fixes common issues.
    """
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None

    json_str = match.group(0)

    # Fix common issues
    json_str = json_str.replace("'", '"')
    json_str = re.sub(r",\s*}", "}", json_str)
    json_str = re.sub(r",\s*]", "]", json_str)

    try:
        return json.loads(json_str)
    except:
        return None

# -----------------------------------
# CONDITION ENGINE
# -----------------------------------
def apply_conditions(df, condition):

    if "logic" in condition:
        logic = condition["logic"]
        conditions = condition["conditions"]
        masks = [apply_conditions(df, c) for c in conditions]

        if logic == "AND":
            mask = masks[0]
            for m in masks[1:]:
                mask &= m
            return mask
        elif logic == "OR":
            mask = masks[0]
            for m in masks[1:]:
                mask |= m
            return mask

    col = condition.get("column")
    op = condition.get("operator")
    val = condition.get("value")

    if col not in df.columns:
        return pd.Series([True] * len(df))

    # Handle NULL checks
    if op == "is_null":
        return df[col].isna() | (df[col] == "")
    if op == "not_null":
        return df[col].notna() & (df[col] != "")

    # Numeric
    if pd.api.types.is_numeric_dtype(df[col]):
        val = float(val)
        if op == ">": return df[col] > val
        if op == "<": return df[col] < val
        if op == ">=": return df[col] >= val
        if op == "<=": return df[col] <= val
        if op == "==": return df[col] == val
        if op == "!=": return df[col] != val

    # String
    series = df[col].astype(str).str.strip().str.lower()
    val = str(val).strip().lower()
    if op == "==": return series == val
    if op == "!=": return series != val

    return pd.Series([True] * len(df))

# -----------------------------------
# TRANSFORMATION ENGINE
# -----------------------------------
def apply_transformations(df, instructions):

    result_df = df.copy()

    if "filter" in instructions:
        mask = apply_conditions(result_df, instructions["filter"])
        result_df = result_df[mask]

    return result_df

# -----------------------------------
# TABS
# -----------------------------------
tab1, tab2 = st.tabs(["AI ETL Engine", "AI Jira Breakdown"])

# ===================================
# ETL TAB
# ===================================
with tab1:

    st.markdown('<div class="section-title">Business Description</div>', unsafe_allow_html=True)
    etl_prompt = st.text_area("Describe data transformation", height=140)
    uploaded_file = st.file_uploader("Upload CSV File", type=["csv"])

    if st.button("Execute ETL"):

        if not etl_prompt.strip() or not uploaded_file:
            st.warning("Provide prompt and CSV.")
            st.stop()

        df = pd.read_csv(uploaded_file)
        original_rows = len(df)

        system_prompt = f"""
Return ONLY valid JSON.
Supported structure:

{{
  "filter": {{
      "logic": "AND",
      "conditions": [
          {{"column": "Department", "operator": "==", "value": "Finance"}},
          {{"column": "Email", "operator": "not_null"}}
      ]
  }}
}}

Operators:
>, <, >=, <=, ==, !=, is_null, not_null

Columns:
{df.columns.tolist()}
"""

        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": etl_prompt}
            ],
            temperature=0.1
        )

        raw_output = response.choices[0].message.content
        instructions = extract_json(raw_output)

        if instructions is None:
            st.error("AI returned invalid JSON.")
            st.code(raw_output)
            st.stop()

        result_df = apply_transformations(df, instructions)

        st.subheader("AI Instructions")
        st.json(instructions)

        st.subheader("Transformed Output")
        st.dataframe(result_df, use_container_width=True)

        # Audit log
        st.session_state.history.append({
            "Time": datetime.datetime.now(),
            "Prompt": etl_prompt,
            "Rows Before": original_rows,
            "Rows After": len(result_df)
        })

        # Export
        st.markdown('<div class="section-title">Export Results</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)

        col1.download_button(
            "Download CSV",
            result_df.to_csv(index=False).encode("utf-8"),
            "etl_output.csv",
            "text/csv"
        )

        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            result_df.to_excel(writer, sheet_name="Transformed_Data", index=False)
            pd.DataFrame(st.session_state.history).to_excel(writer, sheet_name="Audit_Log", index=False)

        col2.download_button(
            "Download Excel",
            output.getvalue(),
            "etl_output.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

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

        jira_output = response.choices[0].message.content

        st.subheader("Jira Breakdown")
        st.markdown(jira_output)

# ===================================
# HISTORY
# ===================================
st.markdown("---")
st.markdown('<div class="section-title">Transformation History</div>', unsafe_allow_html=True)

if st.session_state.history:
    st.dataframe(pd.DataFrame(st.session_state.history))
else:
    st.info("No transformations executed yet.")
