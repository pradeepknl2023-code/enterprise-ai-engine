from litellm import completion
import streamlit as st
import os

response = completion(
    model="gemini/gemini-1.5-flash",
    messages=[{"role": "user", "content": "Hello"}],
    api_key=os.environ.get("GEMINI_API_KEY")
)

st.write(response.choices[0].message.content)
