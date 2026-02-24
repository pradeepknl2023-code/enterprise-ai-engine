import streamlit as st
import pandas as pd
import os
import re
from groq import Groq

# -----------------------------------
# PAGE CONFIG
# -----------------------------------
st.set_page_config(
    page_title="Enterprise AI ETL Engine",
    layout="wide"
)

# -----------------------------------
# WELLS FARGO ENTERPRISE THEME
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
    font-size: 34px;
    margin: 0;
    font-weight: bold;
}
.section-title {
    color: #B31B1B;
    font-size: 22px;
    font-weight: 600;
    margin-top: 20px;
}
.stButton>button {
    background-color: #B31B1B;
    color: white;
    font-weight: bold;
    border-radius: 6px;
    padding: 10px 24px;
}
.stButton>button:hover {
    background-color: #8E1414;
    color: #FFC72C;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
<h1>Enterprise AI ETL Transformation Engine</h1>
</div>
""", unsafe_allow_html=True)

# -----------------------------------
# GROQ CONFIG
# -----------------------------------
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not GROQ_API_KEY:
    st.error("GROQ_API_KEY not found in environment variables.")
    st.stop()

client = Groq(api_key=GROQ_API_KEY)

# -----------------------------------
# FILE UPLOAD
# -----------------------------------
st.markdown('<div class="section-title">Upload Dataset</div>', unsafe_allow_html=True)

uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

df = None
if uploaded_file:
    df = pd.read_csv(uploaded_file)
    st.dataframe(df, use_container_width=True)

# -----------------------------------
# PROMPT INPUT
# -----------------------------------
st.markdown('<div class="section-title">Describe Transformation</div>', unsafe_allow_html=True)

prompt = st.text_area(
    "Example: Remove records where Last_Name = kumar and create full_name column",
    height=120
)

# -----------------------------------
# RUN BUTTON (ALWAYS VISIBLE)
# -----------------------------------
run_clicked = st.button("Run Enterprise ETL")

# -----------------------------------
# EXECUTION
# -----------------------------------
if run_clicked:

    if df is None:
        st.warning("Please upload a CSV file before running ETL.")
        st.stop()

    if not prompt.strip():
        st.warning("Please enter a transformation description.")
        st.stop()

    with st.spinner("Generating enterprise-grade Pandas transformation..."):

        system_prompt = f"""
You are a Senior Enterprise Data Engineer working in a regulated banking environment.

STRICT RULES:
- DataFrame name is df
- Always handle null values using fillna("")
- Always strip whitespace using .str.strip()
- Always perform case-insensitive comparison using .str.lower()
- Never use inplace=True
- Never use loops for filtering
- Always use vectorized pandas operations
- Never explain anything
- Return ONLY executable Python code
- Columns available: {df.columns.tolist()}

For filtering strings use:
df = df[~df["column"].fillna("").str.strip().str.lower().eq("value")]
"""

        try:
            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
            )

            generated_code = response.choices[0].message.content.strip()

            # Remove markdown if model adds it
            generated_code = re.sub(r"```python", "", generated_code)
            generated_code = re.sub(r"```", "", generated_code)

            st.markdown('<div class="section-title">Generated Pandas Code</div>', unsafe_allow_html=True)
            st.code(generated_code, language="python")

            # Safe execution
            safe_globals = {"pd": pd}
            safe_locals = {"df": df.copy()}

            exec(generated_code, safe_globals, safe_locals)

            transformed_df = safe_locals["df"]

            st.markdown('<div class="section-title">Transformed Output</div>', unsafe_allow_html=True)
            st.dataframe(transformed_df, use_container_width=True)

            csv = transformed_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download Transformed CSV",
                csv,
                "transformed.csv",
                "text/csv"
            )

        except Exception as e:
            st.error(f"Execution Error: {e}")
