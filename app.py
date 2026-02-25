import streamlit as st
import pandas as pd
import os
import datetime
import json
from groq import Groq
from io import BytesIO

# -----------------------------------
# PAGE CONFIG
# -----------------------------------
st.set_page_config(page_title="Enterprise AI Platform", layout="wide")

# -----------------------------------
# THEME (UNCHANGED)
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
    st.error("Set GROQ_API_KEY in Streamlit Secrets.")
    st.stop()

client = Groq(api_key=GROQ_API_KEY)

if "history" not in st.session_state:
    st.session_state.history = []

# -----------------------------------
# SAFE TYPE DETECTION
# -----------------------------------
def is_numeric_column(series):
    try:
        pd.to_numeric(series.dropna().iloc[:10])
        return True
    except:
        return False

# -----------------------------------
# LLM PARSER
# -----------------------------------
def parse_intent(prompt, columns):
    system_prompt = f"""
Return STRICT JSON only.

Supported types:
filter
conditional_column
aggregation

Structure:

{{
"type": "",
"logic": "AND" | "OR",
"filters": [],
"new_columns": [],
"aggregation": {{}}
}}

Available columns: {columns}
"""

    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        temperature=0
    )

    return response.choices[0].message.content.strip()

# -----------------------------------
# SAFE RULE ENGINE
# -----------------------------------
def apply_rules(df, intent_json):
    try:
        intent = json.loads(intent_json)
    except:
        st.error("Invalid JSON from AI.")
        return df

    df = df.copy().fillna("")
    intent_type = intent.get("type")

    # ================= FILTER =================
    if intent_type == "filter":
        logic = intent.get("logic", "AND")
        conditions = []

        for f in intent.get("filters", []):
            col = f.get("column")
            op = f.get("operator")
            val = f.get("value")

            if col not in df.columns:
                continue

            series = df[col]

            if is_numeric_column(series):
                series = pd.to_numeric(series, errors="coerce")
                try:
                    if op == "between":
                        low, high = map(float, str(val).split(","))
                        cond = series.between(low, high)
                    else:
                        val = float(val)
                        if op == ">":
                            cond = series > val
                        elif op == "<":
                            cond = series < val
                        elif op == ">=":
                            cond = series >= val
                        elif op == "<=":
                            cond = series <= val
                        elif op == "=":
                            cond = series == val
                        else:
                            continue
                except:
                    continue
            else:
                series = series.astype(str).str.strip().str.lower()
                if op == "=":
                    cond = series == str(val).strip().lower()
                elif op == "in":
                    values = [v.strip().lower() for v in str(val).split(",")]
                    cond = series.isin(values)
                else:
                    continue

            conditions.append(cond)

        if conditions:
            final_condition = conditions[0]
            for c in conditions[1:]:
                final_condition = (
                    final_condition | c if logic == "OR"
                    else final_condition & c
                )
            df = df[final_condition]

        return df

    # ================= CONDITIONAL COLUMNS =================
    if intent_type == "conditional_column":
        for rule in intent.get("new_columns", []):
            cond_def = rule.get("condition", {})
            col = cond_def.get("column")
            op = cond_def.get("operator")
            val = cond_def.get("value")

            if col not in df.columns:
                continue

            series = df[col]

            if not is_numeric_column(series):
                continue

            series = pd.to_numeric(series, errors="coerce")

            try:
                val = float(val)
            except:
                continue

            if op == ">":
                cond = series > val
            elif op == "<":
                cond = series < val
            elif op == ">=":
                cond = series >= val
            elif op == "<=":
                cond = series <= val
            elif op == "=":
                cond = series == val
            else:
                continue

            df[rule.get("new_column")] = cond.map(
                lambda x: rule.get("true_value") if x else rule.get("false_value")
            )

        return df

    # ================= AGGREGATION =================
    if intent_type == "aggregation":
        agg = intent.get("aggregation", {})
        group_cols = agg.get("group_by", [])
        metrics = agg.get("metrics", [])

        agg_dict = {}
        for m in metrics:
            col = m.get("column")
            op = m.get("operation")
            if col in df.columns and is_numeric_column(df[col]):
                agg_dict[col] = op

        if group_cols:
            return df.groupby(group_cols).agg(agg_dict).reset_index()
        else:
            return df.agg(agg_dict).to_frame().T

    return df

# -----------------------------------
# TABS
# -----------------------------------
tab1, tab2 = st.tabs(["AI ETL Engine", "AI Jira Breakdown"])

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

        try:
            intent_json = parse_intent(etl_prompt, df.columns.tolist())
            transformed_df = apply_rules(df, intent_json)
        except Exception as e:
            st.error(f"ETL failed: {e}")
            transformed_df = df.copy()

        st.subheader("Generated Intent JSON")
        st.code(intent_json)

        st.subheader("Transformed Output")
        st.dataframe(transformed_df, use_container_width=True)

        st.session_state.history.append({
            "Time": datetime.datetime.now(),
            "Prompt": etl_prompt,
            "Rows Before": original_rows,
            "Rows After": len(transformed_df)
        })

        col1, col2 = st.columns(2)
        col1.download_button(
            "Download CSV",
            transformed_df.to_csv(index=False).encode("utf-8"),
            "etl_output.csv",
            "text/csv"
        )

        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            transformed_df.to_excel(writer, sheet_name="Transformed_Data", index=False)
            pd.DataFrame(st.session_state.history).to_excel(writer, sheet_name="Audit_Log", index=False)

        col2.download_button(
            "Download Excel",
            output.getvalue(),
            "etl_output.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# JIRA TAB UNCHANGED
with tab2:
    st.markdown('<div class="section-title">Business Description</div>', unsafe_allow_html=True)
    jira_prompt = st.text_area("Describe feature or initiative", height=140)

    if st.button("Generate Jira Breakdown"):
        if not jira_prompt.strip():
            st.warning("Enter business description.")
            st.stop()

        with st.spinner("Generating Agile breakdown..."):
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
