import streamlit as st
import pandas as pd
import os
import re
import datetime
from groq import Groq
from difflib import get_close_matches
from io import BytesIO

# -----------------------------------
# PAGE CONFIG
# -----------------------------------
st.set_page_config(page_title="Enterprise AI Platform", layout="wide")

# -----------------------------------
# WELLS ENTERPRISE THEME
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
    font-size: 32px;
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
# GROQ CONFIG
# -----------------------------------
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    st.error("Please set GROQ_API_KEY in Streamlit Secrets.")
    st.stop()

client = Groq(api_key=GROQ_API_KEY)

# -----------------------------------
# BUSINESS DESCRIPTION FIRST
# -----------------------------------
st.markdown('<div class="section-title">Business Description</div>', unsafe_allow_html=True)

business_prompt = st.text_area(
    "Describe business requirement or data transformation",
    height=150
)

col1, col2 = st.columns(2)
etl_clicked = col1.button("Run AI ETL")
jira_clicked = col2.button("Generate Jira Breakdown")

# ===================================
# ========== AI ETL ENGINE ==========
# ===================================

if etl_clicked:

    if not business_prompt.strip():
        st.warning("Please enter business description.")
        st.stop()

    st.markdown('<div class="section-title">Upload Dataset</div>', unsafe_allow_html=True)
    uploaded_file = st.file_uploader("Upload CSV File", type=["csv"])

    if not uploaded_file:
        st.info("Please upload file to proceed.")
        st.stop()

    df = pd.read_csv(uploaded_file)
    original_count = len(df)

    st.dataframe(df, use_container_width=True)

    system_prompt = f"""
You are a Senior Enterprise Data Engineer.

STRICT RULES:
- DataFrame name is df
- Always handle nulls using fillna("")
- Always strip spaces using .str.strip()
- Always compare strings case-insensitive using .str.lower()
- Never use inplace=True
- Never use loops for filtering
- Use vectorized pandas operations
- Return ONLY executable Python code
- Columns available: {df.columns.tolist()}

Safe filtering pattern:
df = df[~df["column"].fillna("").str.strip().str.lower().eq("value")]
"""

    def generate_code(error_message=None):
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": business_prompt}
        ]
        if error_message:
            messages.append({
                "role": "user",
                "content": f"Previous code failed with error: {error_message}. Fix it."
            })

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
            st.error("Unsafe code detected. Execution blocked.")
            st.stop()

        local_env = {"df": df.copy(), "pd": pd}
        exec(code, {}, local_env)
        transformed_df = local_env["df"]

    except Exception as e:
        code = generate_code(str(e))
        local_env = {"df": df.copy(), "pd": pd}
        exec(code, {}, local_env)
        transformed_df = local_env["df"]

    st.markdown('<div class="section-title">Generated Code</div>', unsafe_allow_html=True)
    st.code(code)

    st.markdown('<div class="section-title">Transformed Output</div>', unsafe_allow_html=True)
    st.dataframe(transformed_df, use_container_width=True)

    # Audit Log
    audit = pd.DataFrame({
        "Timestamp": [datetime.datetime.now()],
        "Prompt": [business_prompt],
        "Rows Before": [original_count],
        "Rows After": [len(transformed_df)]
    })

    # Excel Export
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        transformed_df.to_excel(writer, sheet_name="Transformed_Data", index=False)
        audit.to_excel(writer, sheet_name="Audit_Log", index=False)

    st.download_button(
        "Download Excel Report",
        output.getvalue(),
        "enterprise_etl_output.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# ===================================
# ======== AI JIRA ENGINE ===========
# ===================================

if jira_clicked:

    if not business_prompt.strip():
        st.warning("Please enter business description.")
        st.stop()

    jira_prompt = """
You are a Senior Agile Delivery Manager.

Generate:
- One Epic
- Multiple User Stories
- Acceptance Criteria for each
- Subtasks

Return structured professional format.
"""

    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {"role": "system", "content": jira_prompt},
            {"role": "user", "content": business_prompt}
        ],
        temperature=0.3
    )

    jira_output = response.choices[0].message.content

    st.markdown('<div class="section-title">Jira Breakdown</div>', unsafe_allow_html=True)
    st.markdown(jira_output)
