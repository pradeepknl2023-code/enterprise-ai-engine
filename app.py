import streamlit as st
import pandas as pd
import os
import datetime
import json
import sqlite3
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
# CONDITION ENGINE (AND / OR SUPPORT)
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

    col = condition["column"]
    op = condition["operator"]
    val = condition["value"]

    if col not in df.columns:
        return pd.Series([True] * len(df))

    if pd.api.types.is_numeric_dtype(df[col]):
        val = float(val)
        if op == ">": return df[col] > val
        if op == "<": return df[col] < val
        if op == ">=": return df[col] >= val
        if op == "<=": return df[col] <= val
        if op == "==": return df[col] == val
        if op == "!=": return df[col] != val
    else:
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

    # Apply filters
    if "filter" in instructions:
        mask = apply_conditions(result_df, instructions["filter"])
        result_df = result_df[mask]

    # Drop nulls
    for col in instructions.get("dropna", []):
        if col in result_df.columns:
            result_df = result_df[result_df[col].notna() & (result_df[col] != "")]

    # Derived columns
    for d in instructions.get("derived_columns", []):
        base = d["base_column"]
        new_col = d["new_column"]
        threshold = float(d["threshold"])
        if base in result_df.columns:
            result_df[new_col] = result_df[base].apply(
                lambda x: "High" if x >= threshold else "Low"
            )

    # Column selection
    select_cols = instructions.get("select_columns", [])
    if select_cols:
        valid = [c for c in select_cols if c in result_df.columns]
        result_df = result_df[valid]

    # Aggregation
    if "aggregation" in instructions:
        agg = instructions["aggregation"]
        group_col = agg["group_by"]
        agg_col = agg["column"]
        func = agg["function"].lower()

        if group_col in result_df.columns and agg_col in result_df.columns:
            if func in ["avg", "mean"]:
                result_df = result_df.groupby(group_col)[agg_col].mean().reset_index()
            elif func == "sum":
                result_df = result_df.groupby(group_col)[agg_col].sum().reset_index()
            elif func == "count":
                result_df = result_df.groupby(group_col)[agg_col].count().reset_index()

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

    sql_mode = st.toggle("Enable SQL Backend Mode")

    if st.button("Execute ETL"):

        if not etl_prompt.strip() or not uploaded_file:
            st.warning("Provide prompt and CSV.")
            st.stop()

        df = pd.read_csv(uploaded_file)
        original_rows = len(df)

        if sql_mode:
            conn = sqlite3.connect(":memory:")
            df.to_sql("data", conn, index=False, if_exists="replace")
            try:
                result_df = pd.read_sql_query(etl_prompt, conn)
            except Exception as e:
                st.error(f"SQL Error: {e}")
                st.stop()
        else:

            system_prompt = f"""
Return ONLY valid JSON.
Supported keys:
filter (nested AND/OR),
dropna,
derived_columns,
select_columns,
aggregation.

Columns available:
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

            try:
                instructions = json.loads(response.choices[0].message.content)
            except:
                st.error("Invalid JSON from AI.")
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
            "Rows After": len(result_df),
            "Mode": "SQL" if sql_mode else "AI Engine"
        })

        # Export
        st.markdown('<div class="section-title">Export Results</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)

        col1.download_button("Download CSV",
                             result_df.to_csv(index=False).encode("utf-8"),
                             "etl_output.csv",
                             "text/csv")

        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            result_df.to_excel(writer, sheet_name="Transformed_Data", index=False)
            pd.DataFrame(st.session_state.history).to_excel(writer, sheet_name="Audit_Log", index=False)

        col2.download_button("Download Excel",
                             output.getvalue(),
                             "etl_output.xlsx",
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
