from litellm import completion
import streamlit as st

response = completion(
    model="google/gemini-1.5-flash",
    messages=[{"role": "user", "content": "Hello"}],
    api_key=st.secrets["GEMINI_API_KEY"]
)

st.write(response.choices[0].message.content)
