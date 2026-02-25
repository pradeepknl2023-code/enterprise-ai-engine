import streamlit as st
import pandas as pd
import numpy as np
import json
import os
from groq import Groq

# -----------------------------------
# PAGE CONFIG
# -----------------------------------
st.set_page_config(page_title="Enterprise AI Platform", layout="wide")

st.markdown("""
<div style='background-color:#b31b1b;padding:15px;border-radius:5px'>
<h1 style='color:gold;text-align:center;'>Enterprise AI Smart Data Assistant</h1>
</div>
""", unsafe_allow_html=True)

# -----------------------------------
# GROQ CLIENT
# -----------------------------------
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
client = Groq(api_key=GROQ_API_KEY)

# -----------------------------------
# STRICT SYSTEM PROMPT
# -----------------------------------
SYSTEM_PROMPT = """
You are a Smart ETL Intent Parser.

Return ONLY valid JSON.
No explanation.
No markdown.
No extra text.

Schema:

{
  "operation_type": null,
  "filters": [],
  "new_columns": [],
  "group_by": [],
  "aggregation": null,
  "ranking": null,
  "sort": null,
  "limit": null
}

Filter format:
{
  "column": "",
  "operator": "=", ">", "<", ">=", "<=", "!=",
  "value": "",
  "logic": "AND" or "OR"
}

New column format:
{
  "column_name": "",
  "conditions": [
      {
        "column": "",
        "operator": "",
        "value": "",
        "output": ""
      }
  ],
  "default": ""
}

Aggregation:
{
  "column": "",
  "operator": "avg | sum | min | max | count"
}

Ranking:
{
  "partition_by": "",
  "order_by": "",
  "order": "asc | desc",
  "top_n": number
}
"""

# -----------------------------------
# SAFE JSON RETRY
# -----------------------------------
def get_clean_json(user_prompt):
    for _ in range(2):
        response = client.chat.completions.create(
            model="llama3-70b-8192",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0
        )
        raw = response.choices[0].message.content.strip()
        try:
            return json.loads(raw)
        except:
            user_prompt = "Return valid JSON only.\n" + user_prompt
    raise ValueError("Invalid JSON from AI.")

# -----------------------------------
# FILTER ENGINE (Dynamic AND/OR)
# -----------------------------------
def apply_filters(df, filters):
    if not filters:
        return df

    mask = pd.Series([True] * len(df))

    for f in filters:
        col = f["column"]
        op = f["operator"]
        val = f["value"]
        logic = f.get("logic", "AND")

        if col not in df.columns:
            continue

        series = df[col]
        series_num = pd.to_numeric(series, errors="coerce")

        if op == "=":
            cond = series == val
        elif op == "!=":
            cond = series != val
        elif op == ">":
            cond = series_num > float(val)
        elif op == "<":
            cond = series_num < float(val)
        elif op == ">=":
            cond = series_num >= float(val)
        elif op == "<=":
            cond = series_num <= float(val)
        else:
            continue

        if logic == "AND":
            mask &= cond
        else:
            mask |= cond

    return df[mask]

# -----------------------------------
# NEW COLUMN ENGINE
# -----------------------------------
def apply_new_columns(df, new_columns):
    for col_def in new_columns:
        name = col_def["column_name"]
        df[name] = col_def.get("default", None)

        for cond in col_def["conditions"]:
            col = cond["column"]
            op = cond["operator"]
            val = cond["value"]
            output = cond["output"]

            if col not in df.columns:
                continue

            series = df[col]
            series_num = pd.to_numeric(series, errors="coerce")

            if op == "=":
                mask = series == val
            elif op == ">":
                mask = series_num > float(val)
            elif op == "<":
                mask = series_num < float(val)
            elif op == ">=":
                mask = series_num >= float(val)
            elif op == "<=":
                mask = series_num <= float(val)
            else:
                continue

            df.loc[mask, name] = output

    return df

# -----------------------------------
# EXECUTION ENGINE
# -----------------------------------
def execute_etl(df, instructions):

    df = df.copy()

    df = apply_filters(df, instructions.get("filters"))

    df = apply_new_columns(df, instructions.get("new_columns"))

    if instructions.get("aggregation"):
        col = instructions["aggregation"]["column"]
        op = instructions["aggregation"]["operator"]

        df[col] = pd.to_numeric(df[col], errors="coerce")

        if instructions.get("group_by"):
            grouped = df.groupby(instructions["group_by"])

            if op == "avg":
                df = grouped[col].mean().reset_index()
            elif op == "sum":
                df = grouped[col].sum().reset_index()
            elif op == "count":
                df = grouped[col].count().reset_index()
            elif op == "min":
                df = grouped[col].min().reset_index()
            elif op == "max":
                df = grouped[col].max().reset_index()

    if instructions.get("ranking"):
        r = instructions["ranking"]
        part = r["partition_by"]
        order = r["order_by"]
        direction = r["order"]
        top_n = r["top_n"]

        df[order] = pd.to_numeric(df[order], errors="coerce")

        df = (
            df.sort_values(order, ascending=(direction == "asc"))
              .groupby(part)
              .head(top_n)
        )

    if instructions.get("sort"):
        col = instructions["sort"]["column"]
        direction = instructions["sort"]["order"]
        df = df.sort_values(col, ascending=(direction == "asc"))

    if instructions.get("limit"):
        df = df.head(instructions["limit"])

    return df

# -----------------------------------
# UI
# -----------------------------------
uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    st.subheader("Preview Data")
    st.dataframe(df)

    user_prompt = st.text_area("Enter ETL instruction")

    if st.button("Run ETL"):
        try:
            instructions = get_clean_json(user_prompt)
            result = execute_etl(df, instructions)
            st.success("ETL executed successfully.")
            st.dataframe(result)
        except Exception as e:
            st.error(f"ETL failed: {str(e)}")

# -----------------------------------
# JIRA BREAKDOWN (UNCHANGED)
# -----------------------------------
st.markdown("---")
st.header("Jira Breakdown")

jira_input = st.text_area("Enter Business Requirement")

if st.button("Generate Jira Stories"):
    response = client.chat.completions.create(
        model="llama3-70b-8192",
        messages=[
            {"role": "system", "content": "Generate structured Jira Epics, Stories, Subtasks, and Acceptance Criteria."},
            {"role": "user", "content": jira_input}
        ],
        temperature=0.3
    )
    st.write(response.choices[0].message.content)
