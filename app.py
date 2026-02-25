import streamlit as st
import pandas as pd
import os
import datetime
import json
import re
from groq import Groq

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------
st.set_page_config(page_title="Enterprise AI Platform", layout="wide")

# --------------------------------------------------
# ENTERPRISE THEME (UNCHANGED)
# --------------------------------------------------
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

# --------------------------------------------------
# GROQ CLIENT
# --------------------------------------------------
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    st.error("Set GROQ_API_KEY in environment.")
    st.stop()

client = Groq(api_key=GROQ_API_KEY)

# --------------------------------------------------
# SESSION STATE
# --------------------------------------------------
if "history" not in st.session_state:
    st.session_state.history = []

# --------------------------------------------------
# SAFE JSON PARSER (AUTO REPAIR)
# --------------------------------------------------
def safe_json_parse(text):
    try:
        match = re.search(r"\{[\s\S]*\}", text)
        if not match:
            return None
        content = match.group(0)
        content = content.replace("\n", "")
        return json.loads(content)
    except:
        return None

# --------------------------------------------------
# COLUMN MATCH (CASE INSENSITIVE)
# --------------------------------------------------
def match_column(df, name):
    if not name:
        return None
    for col in df.columns:
        if col.lower() == str(name).lower():
            return col
    return None

# --------------------------------------------------
# CONDITION ENGINE (RECURSIVE)
# --------------------------------------------------
def apply_condition(df, cond):

    if cond is None:
        return pd.Series([True] * len(df))

    if "logic" in cond:
        logic = cond.get("logic", "AND").upper()
        conditions = cond.get("conditions", [])
        masks = [apply_condition(df, c) for c in conditions]
        if not masks:
            return pd.Series([True] * len(df))
        mask = masks[0]
        for m in masks[1:]:
            mask = mask & m if logic == "AND" else mask | m
        return mask

    col = match_column(df, cond.get("column"))
    op = cond.get("operator")
    val = cond.get("value")

    if not col:
        return pd.Series([True] * len(df))

    series = df[col]

    if op in [">", "<", ">=", "<=", "=", "==", "!="]:
        if pd.api.types.is_numeric_dtype(series):
            val = float(val)
        else:
            series = series.astype(str).str.lower().str.strip()
            val = str(val).lower().strip()

        if op == ">": return series > val
        if op == "<": return series < val
        if op == ">=": return series >= val
        if op == "<=": return series <= val
        if op in ["=", "=="]: return series == val
        if op == "!=": return series != val

    if op == "between":
        low, high = val
        return (series >= float(low)) & (series <= float(high))

    return pd.Series([True] * len(df))

# --------------------------------------------------
# DERIVED COLUMN ENGINE
# --------------------------------------------------
def apply_new_columns(df, instructions):
    for new_col in instructions:
        name = new_col.get("name")
        rules = new_col.get("rules", [])
        if not name:
            continue
        df[name] = ""
        for rule in rules:
            condition = rule.get("condition")
            value = rule.get("value")
            if condition == "otherwise":
                df[name] = df[name].replace("", value)
            else:
                mask = apply_condition(df, condition)
                df.loc[mask, name] = value
    return df

# --------------------------------------------------
# AGGREGATION ENGINE
# --------------------------------------------------
def apply_aggregation(df, agg):
    if not agg:
        return df

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

# --------------------------------------------------
# TRANSFORMATION ORCHESTRATOR
# --------------------------------------------------
def transform(df, instructions):

    result = df.copy()

    # FILTER
    if instructions.get("filter"):
        mask = apply_condition(result, instructions["filter"])
        result = result[mask]

    # NEW COLUMNS
    if instructions.get("new_columns"):
        result = apply_new_columns(result, instructions["new_columns"])

    # AGGREGATION
    if instructions.get("aggregation"):
        result = apply_aggregation(result, instructions["aggregation"])

    return result

# --------------------------------------------------
# TABS
# --------------------------------------------------
tab1, tab2 = st.tabs(["AI ETL Engine", "AI Jira Breakdown"])

# ==================================================
# ETL TAB
# ==================================================
with tab1:

    st.markdown('<div class="section-title">Business Description</div>', unsafe_allow_html=True)
    prompt = st.text_area("Describe transformation", height=150)
    uploaded_file = st.file_uploader("Upload CSV", type=["csv"])

    if st.button("Execute ETL"):

        if not prompt.strip() or not uploaded_file:
            st.warning("Provide prompt and CSV.")
            st.stop()

        df = pd.read_csv(uploaded_file)
        original_rows = len(df)

        system_prompt = f"""
You are a strict JSON transformation engine.

Return ONLY valid JSON.

Structure MUST be:

{{
 "filter": null,
 "new_columns": [],
 "aggregation": null
}}

Rules:
- Always include all 3 keys.
- If unused → null or [].
- Allowed operators: >, <, >=, <=, =, !=, between
- between must use array [low, high]
- No explanation text.

Available Columns:
{df.columns.tolist()}
"""

        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )

        raw = response.choices[0].message.content
        instructions = safe_json_parse(raw)

        if not instructions:
            st.error("AI returned invalid JSON.")
            st.code(raw)
            st.stop()

        result_df = transform(df, instructions)

        st.subheader("AI Instructions")
        st.json(instructions)

        st.subheader("Transformed Output")
        st.dataframe(result_df, use_container_width=True)

        st.session_state.history.append({
            "Time": datetime.datetime.now(),
            "Prompt": prompt,
            "Rows Before": original_rows,
            "Rows After": len(result_df)
        })

# ==================================================
# JIRA TAB (UNCHANGED)
# ==================================================
with tab2:

    st.markdown('<div class="section-title">Business Description</div>', unsafe_allow_html=True)
    jira_prompt = st.text_area("Describe feature or initiative", height=150)

    if st.button("Generate Jira Breakdown"):

        jira_system = """
You are a Senior Agile Delivery Manager.

Generate:
- 1 Epic
- Multiple User Stories
- Acceptance Criteria
- Subtasks

Professional structured format only.
"""

        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": jira_system},
                {"role": "user", "content": jira_prompt}
            ],
            temperature=0.3
        )

        st.markdown(response.choices[0].message.content)

# ==================================================
# HISTORY
# ==================================================
st.markdown("---")
st.markdown('<div class="section-title">Transformation History</div>', unsafe_allow_html=True)

if st.session_state.history:
    st.dataframe(pd.DataFrame(st.session_state.history))
else:
    st.info("No transformations executed yet.")
