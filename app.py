import streamlit as st
import os

# Sync Streamlit secrets to environment
if hasattr(st, "secrets"):
    for key in st.secrets:
        os.environ[key] = st.secrets[key]

st.write("Gemini Key Exists:", bool(os.environ.get("GEMINI_API_KEY")))
