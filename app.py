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
# JSON EXTRACTOR
# -----------------------------------
def extract_json(text):
    try:
        match = re.search(r"\{[\s\S]*\}", text)
        if not match:
            return None
        return json.loads(match.group(0))
    except:
        return None

# -----------------------------------
# COLUMN MATCH
# -----------------------------------
def match_column(df, name):
    for col in df.columns:
        if col.lower() == str(name).lower():
            return col
    return None

# -----------------------------------
# FILTER ENGINE
# -----------------------------------
def apply_conditions(df, condition):

    if "logic" in condition:
        logic = condition.get("logic", "AND").upper()
        conditions = condition.get("conditions", [])

        if not conditions:
            return pd.Series([True] * len(df))

        masks = [apply_conditions(df, c) for c in conditions]
        mask = masks[0]

        for m in masks[1:]:
            if logic == "AND":
                mask &= m
            elif logic == "OR":
                mask |= m

        return mask

    col = match_column(df, condition.get("column"))
    op = condition.get("operator")
    val = condition.get("value")

    if not col:
        return pd.Series([True] * len(df))

    if op in [">", "<", ">=", "<=", "=", "==", "!="]:
        if pd.api.types.is_numeric_dtype(df[col]):
            val = float(val)
            if op == ">": return df[col] > val
            if op == "<": return df[col] < val
            if op == ">=": return df[col] >= val
            if op == "<=": return df[col] <= val
            if op in ["=", "=="]: return df[col] == val
            if op == "!=": return df[col] != val
        else:
            series = df[col].astype(str).str.lower().str.strip()
            val = str(val).lower().strip()
            if op in ["=", "=="]: return series == val
            if op == "!=": return series != val

    if op == "between":
        low = float(val[0])
        high = float(val[1])
        return (df[col] >= low) & (df[col] <= high)

    return pd.Series([True] * len(df))

# -----------------------------------
# DERIVED COLUMN ENGINE
# -----------------------------------
def apply_new_columns(df, instructions):

    for new_col in instructions.get("new_columns", []):
        col_name = new_col.get("name")
        rules = new_col.get("rules", [])

        df[col_name] = ""

        for rule in rules:
            condition = rule.get("condition")
            value = rule.get("value")

            if condition == "otherwise":
                df[col_name] = df[col_name].replace("", value)
            else:
                mask = apply_conditions(df, condition)
                df.loc[mask, col_name] = value

    return df

# -----------------------------------
# AGGREGATION ENGINE
# -----------------------------------
def apply_aggregation(df, agg):

    group_col = match_column(df, agg.get("group_by"))
    agg_col = match_column(df, agg.get("column"))
    func = str(agg.get("function", "")).lower()

    if not group_col or not agg_col:
        return df

    if func in ["avg", "average", "mean"]:
        return df.groupby(group_col)[agg_col].mean().reset_index()

    if func in ["sum", "total"]:
        return df.groupby(group_col)[agg_col].sum().reset_index()

    if func == "count":
        return df.groupby(group_col)[agg_col].count().reset_index()

    return df

# -----------------------------------
# MAIN TRANSFORMATION ENGINE
# -----------------------------------
def apply_transformations(df, instructions):

    result = df.copy()

    if "filter" in instructions:
        mask = apply_conditions(result, instructions["filter"])
        result = result[mask]

    if "new_columns" in instructions:
        result = apply_new_columns(result, instructions)

    if "aggregation" in instructions:
        result = apply_aggregation(result, instructions["aggregation"])

    return result

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

Structure:

{{
  "filter": {{
    "logic": "AND",
    "conditions": []
  }},
  "new_columns": [
    {{
      "name": "",
      "rules": [
        {{
          "condition": {{"column":"Salary","operator":">=","value":80000}},
          "value":"High"
        }},
        {{
          "condition": "otherwise",
          "value":"Low"
        }}
      ]
    }}
  ],
  "aggregation": {{
    "group_by": "",
    "column": "",
    "function": ""
  }}
}}

Operators:
>, <, >=, <=, =, !=, between

Available Columns:
{df.columns.tolist()}
"""

        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": etl_prompt}
            ],
            temperature=0.0
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

        st.session_state.history.append({
            "Time": datetime.datetime.now(),
            "Prompt": etl_prompt,
            "Rows Before": original_rows,
            "Rows After": len(result_df)
        })

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
