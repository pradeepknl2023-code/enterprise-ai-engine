import streamlit as st
import pandas as pd
import os
import datetime
import json
import re
from groq import Groq

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
# SAFE JSON EXTRACTOR
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
# VALIDATOR
# -----------------------------------
def validate_instructions(instructions):
    if not isinstance(instructions, dict):
        return False, "Instructions must be JSON object."

    if "filter" in instructions:
        if not isinstance(instructions["filter"], dict):
            return False, "Invalid filter structure."

    if "aggregation" in instructions:
        agg = instructions["aggregation"]
        if not isinstance(agg, dict):
            return False, "Aggregation must be object."
        if "group_by" not in agg or "column" not in agg:
            return False, "Aggregation missing required fields."

    return True, None

# -----------------------------------
# COLUMN MATCHER
# -----------------------------------
def match_column(df, name):
    for col in df.columns:
        if col.lower() == str(name).lower():
            return col
    return None

# -----------------------------------
# CONDITION ENGINE
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

    if not col or not op:
        return pd.Series([True] * len(df))

    if op == "is_null":
        return df[col].isna() | (df[col] == "")
    if op == "not_null":
        return df[col].notna() & (df[col] != "")

    if pd.api.types.is_numeric_dtype(df[col]):
        try:
            val = float(val)
        except:
            return pd.Series([True] * len(df))

        if op == ">": return df[col] > val
        if op == "<": return df[col] < val
        if op == ">=": return df[col] >= val
        if op == "<=": return df[col] <= val
        if op in ["=", "=="]: return df[col] == val
        if op == "!=": return df[col] != val

    series = df[col].astype(str).str.lower().str.strip()
    val = str(val).lower().strip()

    if op in ["=", "=="]: return series == val
    if op == "!=": return series != val

    return pd.Series([True] * len(df))

# -----------------------------------
# TRANSFORMATION ENGINE
# -----------------------------------
def apply_transformations(df, instructions):

    result_df = df.copy()

    # FILTER
    if "filter" in instructions:
        mask = apply_conditions(result_df, instructions["filter"])
        result_df = result_df[mask]

    # AGGREGATION
    if "aggregation" in instructions:
        agg = instructions["aggregation"]
        group_col = match_column(result_df, agg.get("group_by"))
        agg_col = match_column(result_df, agg.get("column"))
        func = str(agg.get("function", "")).lower()

        if group_col and agg_col:

            if func in ["avg", "average", "mean"]:
                result_df = (
                    result_df.groupby(group_col)[agg_col]
                    .mean()
                    .reset_index()
                    .rename(columns={agg_col: f"Average_{agg_col}"})
                )

            elif func in ["sum", "total"]:
                result_df = (
                    result_df.groupby(group_col)[agg_col]
                    .sum()
                    .reset_index()
                    .rename(columns={agg_col: f"Total_{agg_col}"})
                )

            elif func == "count":
                result_df = (
                    result_df.groupby(group_col)[agg_col]
                    .count()
                    .reset_index()
                    .rename(columns={agg_col: f"Count_{agg_col}"})
                )

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

Structure:
{{
  "filter": {{
    "logic": "AND",
    "conditions": []
  }},
  "aggregation": {{
    "group_by": "",
    "column": "",
    "function": ""
  }}
}}

Operators:
>, <, >=, <=, =, !=, is_null, not_null

Available Columns:
{df.columns.tolist()}
"""

        try:
            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": etl_prompt}
                ],
                temperature=0.0
            )

            if not response or not response.choices:
                st.error("AI returned no response.")
                st.stop()

            raw_output = response.choices[0].message.content

        except Exception as e:
            st.error(f"AI call failed: {str(e)}")
            st.stop()

        instructions = extract_json(raw_output)

        if instructions is None:
            st.error("AI returned invalid JSON.")
            st.code(raw_output)
            st.stop()

        is_valid, error = validate_instructions(instructions)
        if not is_valid:
            st.error(f"Invalid instruction schema: {error}")
            st.json(instructions)
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
# JIRA TAB (UNCHANGED LOGIC)
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

        try:
            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": jira_system_prompt},
                    {"role": "user", "content": jira_prompt}
                ],
                temperature=0.3
            )

            if not response or not response.choices:
                st.error("AI returned no response.")
                st.stop()

            jira_output = response.choices[0].message.content
            st.subheader("Jira Breakdown")
            st.markdown(jira_output)

        except Exception as e:
            st.error(f"AI call failed: {str(e)}")

# ===================================
# HISTORY
# ===================================
st.markdown("---")
st.markdown('<div class="section-title">Transformation History</div>', unsafe_allow_html=True)

if st.session_state.history:
    st.dataframe(pd.DataFrame(st.session_state.history))
else:
    st.info("No transformations executed yet.")
