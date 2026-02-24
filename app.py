import streamlit as st
import pandas as pd
import os
import re
import datetime
from groq import Groq
from io import BytesIO

# -----------------------------------
# PAGE CONFIG
# -----------------------------------
st.set_page_config(page_title="Enterprise AI Platform", layout="wide")

# -----------------------------------
# ENTERPRISE THEME
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
.history-box {
    background-color: #f7f7f7;
    padding: 10px;
    border-radius: 6px;
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

# -----------------------------------
# SESSION STATE INIT
# -----------------------------------
if "history" not in st.session_state:
    st.session_state.history = []

# -----------------------------------
# TABS
# -----------------------------------
tab1, tab2 = st.tabs(["AI ETL Engine", "AI Jira Breakdown"])

# ===================================
# ========== AI ETL TAB ============
# ===================================

with tab1:

    st.markdown('<div class="section-title">Business Description</div>', unsafe_allow_html=True)

    business_prompt = st.text_area(
        "Describe data transformation",
        key="etl_prompt",
        height=150
    )

    spark_mode = st.toggle("Enable Spark Mode (for large datasets)", value=False)

    uploaded_file = st.file_uploader("Upload CSV File", type=["csv"])

    if st.button("Execute ETL"):

        if not business_prompt.strip():
            st.warning("Enter transformation description.")
            st.stop()

        if not uploaded_file:
            st.warning("Upload CSV file.")
            st.stop()

        df = pd.read_csv(uploaded_file)
        original_rows = len(df)

        with st.spinner("Generating enterprise transformation..."):

            system_prompt = f"""
You are a Senior Enterprise Data Engineer.

STRICT RULES:
- DataFrame name is df
- Handle nulls using fillna("")
- Strip spaces using .str.strip()
- Compare strings using .str.lower()
- No inplace=True
- No loops for filtering
- Use vectorized pandas
- Return ONLY executable Python code
- Columns available: {df.columns.tolist()}
"""

            def generate_code(error=None):
                messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": business_prompt}
                ]
                if error:
                    messages.append({"role": "user", "content": f"Fix error: {error}"})

                response = client.chat.completions.create(
                    model="llama-3.1-8b-instant",
                    messages=messages,
                    temperature=0.1
                )
                code = response.choices[0].message.content
                code = re.sub(r"```python", "", code)
                code = re.sub(r"```", "", code)
                return code.strip()

            try:
                code = generate_code()

                # Security block
                banned = ["import os", "import sys", "subprocess", "eval(", "exec(", "open("]
                if any(b in code for b in banned):
                    st.error("Unsafe code detected.")
                    st.stop()

                local_env = {"df": df.copy(), "pd": pd}
                exec(code, {}, local_env)
                transformed_df = local_env["df"]

            except Exception as e:
                code = generate_code(str(e))
                local_env = {"df": df.copy(), "pd": pd}
                exec(code, {}, local_env)
                transformed_df = local_env["df"]

        st.subheader("Generated Code")
        st.code(code)

        st.subheader("Transformed Output")
        st.dataframe(transformed_df, use_container_width=True)

        # Audit
        audit = {
            "Time": datetime.datetime.now(),
            "Prompt": business_prompt,
            "Rows Before": original_rows,
            "Rows After": len(transformed_df)
        }
        st.session_state.history.append(audit)

        # Excel export
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            transformed_df.to_excel(writer, sheet_name="Transformed_Data", index=False)
            pd.DataFrame(st.session_state.history).to_excel(writer, sheet_name="Audit_Log", index=False)

        st.download_button(
            "Download Excel Report",
            output.getvalue(),
            "enterprise_output.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ===================================
# ========== JIRA TAB ==============
# ===================================

with tab2:

    st.markdown('<div class="section-title">Business Description</div>', unsafe_allow_html=True)

    jira_prompt_input = st.text_area(
        "Describe feature or initiative",
        key="jira_prompt",
        height=150
    )

    if st.button("Generate Jira Breakdown"):

        if not jira_prompt_input.strip():
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

Return professional structured format.
"""

            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": jira_system_prompt},
                    {"role": "user", "content": jira_prompt_input}
                ],
                temperature=0.3
            )

            output = response.choices[0].message.content

        st.subheader("Jira Breakdown")
        st.markdown(output)

# ===================================
# ========== HISTORY PANEL =========
# ===================================

st.markdown("---")
st.markdown('<div class="section-title">Transformation History</div>', unsafe_allow_html=True)

if st.session_state.history:
    st.dataframe(pd.DataFrame(st.session_state.history))
else:
    st.info("No transformations executed yet.")
