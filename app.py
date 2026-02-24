import streamlit as st
import pandas as pd
import re
from io import BytesIO

# ==========================================================
# PAGE CONFIG
# ==========================================================
st.set_page_config(
    page_title="ReqIntelligence AI Platform",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ==========================================================
# HEADER STYLE
# ==========================================================
st.markdown("""
<style>
.main-header {
    background-color: #b31b1b;
    padding: 18px;
    text-align: center;
}
.main-header h1 {
    color: #ffcc00;
    font-size: 28px;
    margin: 0;
}
.sub-text {
    text-align: center;
    font-size: 15px;
    color: grey;
    margin-top: 8px;
}
.stButton button {
    background-color: #b31b1b;
    color: white;
    font-weight: bold;
}
.stDownloadButton button {
    background-color: #006400;
    color: white;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
    <h1>ReqIntelligence AI Platform</h1>
</div>
""", unsafe_allow_html=True)

st.markdown('<div class="sub-text">Hybrid AI + Pandas ETL Automation Engine</div>', unsafe_allow_html=True)

st.divider()

# ==========================================================
# HELPER FUNCTIONS
# ==========================================================

def normalize_columns(df):
    df.columns = [col.strip() for col in df.columns]
    return df


def find_column(df, text):
    for col in df.columns:
        if col.lower() in text.lower():
            return col
    return None


def apply_etl_rules(df, description):
    df = normalize_columns(df)
    desc = description.lower()

    # REMOVE STARTS WITH
    if "remove" in desc and "starts" in desc:
        column = find_column(df, desc)
        match = re.search(r"starts\s+(with|as)\s+([\w@.]+)", desc)

        if column and match:
            value = match.group(2)
            df = df[~df[column].astype(str).str.lower().str.startswith(value.lower())]
            return df, f"Removed rows where {column} starts with '{value}'"

    # REMOVE EQUALS
    if "remove" in desc and "equals" in desc:
        column = find_column(df, desc)
        match = re.search(r"equals\s+([\w@.]+)", desc)

        if column and match:
            value = match.group(1)
            df = df[df[column].astype(str).str.lower() != value.lower()]
            return df, f"Removed rows where {column} equals '{value}'"

    # GREATER THAN
    if "greater than" in desc:
        column = find_column(df, desc)
        match = re.search(r"greater than\s+(\d+)", desc)

        if column and match:
            value = float(match.group(1))
            df = df[pd.to_numeric(df[column], errors='coerce') > value]
            return df, f"Filtered rows where {column} > {value}"

    return df, "No matching rule found. Try clearer instruction."


def convert_df_to_csv(df):
    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue()


# ==========================================================
# UI
# ==========================================================

st.subheader("Upload Dataset")
uploaded_file = st.file_uploader("Upload CSV File", type=["csv"])

st.subheader("Business Transformation Rule")
business_description = st.text_area(
    "Describe transformation in plain English",
    placeholder="Example: remove records where Email starts with pradeep"
)

# ==========================================================
# SUBMIT BUTTON
# ==========================================================

if st.button("Apply Transformation"):

    if not uploaded_file:
        st.error("Please upload a CSV file.")
    elif not business_description:
        st.error("Please enter a transformation rule.")
    else:
        df = pd.read_csv(uploaded_file)

        st.subheader("Original Data")
        st.dataframe(df.head())

        updated_df, message = apply_etl_rules(df, business_description)

        st.success(message)

        st.subheader("Processed Data")
        st.dataframe(updated_df)

        csv_data = convert_df_to_csv(updated_df)

        st.download_button(
            label="Download Processed CSV",
            data=csv_data,
            file_name="processed_output.csv",
            mime="text/csv"
        )

else:
    st.info("Upload CSV, enter rule, and click Apply Transformation.")
