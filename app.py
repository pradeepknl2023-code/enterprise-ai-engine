import os
import streamlit as st

os.environ["GEMINI_API_KEY"] = st.secrets.get("GEMINI_API_KEY", "")
os.environ["GROQ_API_KEY"] = st.secrets.get("GROQ_API_KEY", "")
