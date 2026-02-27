import os
import streamlit as st

st.set_page_config(page_title="Enterprise AI Engine")

# Sync secrets
if "GEMINI_API_KEY" in st.secrets:
    os.environ["GEMINI_API_KEY"] = st.secrets["GEMINI_API_KEY"]

st.title("Enterprise AI Engine")
st.write("Gemini key exists:", bool(os.environ.get("GEMINI_API_KEY")))
