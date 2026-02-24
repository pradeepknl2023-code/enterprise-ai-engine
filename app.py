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

st.title("🚀 Enterprise AI ETL Transformation Engine")

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
uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file)

    st.subheader("📊 Original Data")
    st.dataframe(df)

    prompt = st.text_area(
        "Describe your transformation (example: remove records where last_name = kumar and create full_name column)"
    )

    if st.button("🚀 Run Transformation"):

        with st.spinner("Generating enterprise-grade Pandas code..."):

            system_prompt = f"""
You are a Senior Python Data Engineer working in an enterprise banking environment.

STRICT RULES:

1. The dataframe name is df.
2. Always handle null values using fillna("") before string operations.
3. Always use case-insensitive comparison (.str.lower()).
4. Always strip spaces (.str.strip()).
5. Never use inplace=True.
6. Never use print statements.
7. Never explain anything.
8. Return ONLY executable Python code.
9. If filtering strings, always use this pattern:

df = df[~df["column"].fillna("").str.strip().str.lower().eq("value")]

10. If creating new columns, use vectorized pandas operations.
11. Never assume column exists without using exact column names provided.
12. Columns available: {df.columns.tolist()}

Return only Python code.
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

                # -----------------------------------
                # CLEAN LLM OUTPUT (remove markdown)
                # -----------------------------------
                generated_code = re.sub(r"```python", "", generated_code)
                generated_code = re.sub(r"```", "", generated_code)

                st.subheader("🧠 Generated Pandas Code")
                st.code(generated_code, language="python")

                # -----------------------------------
                # SAFE EXECUTION ENVIRONMENT
                # -----------------------------------
                safe_globals = {"pd": pd}
                safe_locals = {"df": df.copy()}

                exec(generated_code, safe_globals, safe_locals)

                transformed_df = safe_locals["df"]

                st.subheader("✅ Transformed Data")
                st.dataframe(transformed_df)

                csv = transformed_df.to_csv(index=False).encode("utf-8")

                st.download_button(
                    "📥 Download Transformed CSV",
                    csv,
                    "transformed.csv",
                    "text/csv"
                )

            except Exception as e:
                st.error(f"Execution Error: {e}")
