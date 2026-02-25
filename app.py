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
# SAFE ETL ENGINE
# -----------------------------------
def apply_transformations(df, instructions):
    result_df = df.copy()

    # Normalize string columns
    for col in result_df.select_dtypes(include="object").columns:
        result_df[col] = result_df[col].astype(str).str.strip()

    # Apply filters
    for f in instructions.get("filters", []):
        col = f["column"]
        op = f["operator"]
        val = f["value"]

        if col not in result_df.columns:
            continue

        # Numeric comparison
        if pd.api.types.is_numeric_dtype(result_df[col]):
            val = float(val)
            if op == ">":
                result_df = result_df[result_df[col] > val]
            elif op == "<":
                result_df = result_df[result_df[col] < val]
            elif op == ">=":
                result_df = result_df[result_df[col] >= val]
            elif op == "<=":
                result_df = result_df[result_df[col] <= val]
            elif op == "==":
                result_df = result_df[result_df[col] == val]
            elif op == "!=":
                result_df = result_df[result_df[col] != val]

        # String comparison
        else:
            val = str(val).strip().lower()
            series = result_df[col].str.lower()
            if op == "==":
                result_df = result_df[series == val]
            elif op == "!=":
                result_df = result_df[series != val]

    # Remove nulls
    for col in instructions.get("dropna", []):
        if col in result_df.columns:
            result_df = result_df[result_df[col].notna() & (result_df[col] != "")]

    return result_df

# -----------------------------------
# TABS
# -----------------------------------
tab1, tab2 = st.tabs(["AI ETL Engine", "AI Jira Breakdown"])

# ===================================
# ========== AI ETL TAB =============
# ===================================
with tab1:

    st.markdown('<div class="section-title">Business Description</div>', unsafe_allow_html=True)
    etl_prompt = st.text_area("Describe data transformation", height=140)
    uploaded_file = st.file_uploader("Upload CSV File", type=["csv"])

    if st.button("Execute ETL"):

        if not etl_prompt.strip():
            st.warning("Enter transformation description.")
            st.stop()
        if not uploaded_file:
            st.warning("Upload CSV file.")
            st.stop()

        df = pd.read_csv(uploaded_file)
        original_rows = len(df)

        system_prompt = f"""
You are an enterprise ETL instruction generator.

Return ONLY valid JSON.

Format:
{{
  "filters": [
      {{"column": "Salary", "operator": ">", "value": 75000}}
  ],
  "dropna": []
}}

Allowed operators:
>, <, >=, <=, ==, !=

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
            st.error("AI returned invalid JSON.")
            st.stop()

        transformed_df = apply_transformations(df, instructions)

        st.subheader("AI Instructions")
        st.json(instructions)

        st.subheader("Transformed Output")
        st.dataframe(transformed_df, use_container_width=True)

        st.session_state.history.append({
            "Time": datetime.datetime.now(),
            "Prompt": etl_prompt,
            "Rows Before": original_rows,
            "Rows After": len(transformed_df)
        })

        # Export
        st.markdown('<div class="section-title">Export Results</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)

        csv_data = transformed_df.to_csv(index=False).encode("utf-8")
        col1.download_button("Download CSV", csv_data, "etl_output.csv", "text/csv")

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

# ===================================
# ========== JIRA TAB ==============
# ===================================
with tab2:

    st.markdown('<div class="section-title">Business Description</div>', unsafe_allow_html=True)
    jira_prompt = st.text_area("Describe feature or initiative", height=140)

    if st.button("Generate Jira Breakdown"):

        if not jira_prompt.strip():
            st.warning("Enter business description.")
            st.stop()

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

        st.markdown('<div class="section-title">Export Jira Output</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)

        col1.download_button("Download as TXT", jira_output, "jira_breakdown.txt", "text/plain")

        jira_df = pd.DataFrame({"Jira Breakdown": [jira_output]})
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            jira_df.to_excel(writer, sheet_name="Jira_Output", index=False)

        col2.download_button(
            "Download as Excel",
            output.getvalue(),
            "jira_breakdown.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ===================================
# HISTORY PANEL
# ===================================
st.markdown("---")
st.markdown('<div class="section-title">Transformation History</div>', unsafe_allow_html=True)

if st.session_state.history:
    st.dataframe(pd.DataFrame(st.session_state.history))
else:
    st.info("No transformations executed yet.")
