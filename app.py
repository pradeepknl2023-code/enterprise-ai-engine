"""
gemini_test.py  —  Confirms Gemini is ACTUALLY being called
============================================================
Deploy alongside app.py, then open:
  https://your-app.streamlit.app/?page=test
OR run as a separate Streamlit app:
  streamlit run gemini_test.py
"""

import os, sys, time
import streamlit as st

st.set_page_config(page_title="🧪 Gemini Direct Test", layout="centered")

# ── Get key ───────────────────────────────────────────────────
def get_key():
    val = os.environ.get("GEMINI_API_KEY", "").strip()
    if val:
        return val
    try:
        val = str(st.secrets.get("GEMINI_API_KEY", "")).strip()
        if val:
            os.environ["GEMINI_API_KEY"] = val
        return val
    except Exception:
        return ""

GEMINI_MODELS = [
    "gemini/gemini-2.0-flash",
    "gemini/gemini-2.0-flash-exp",
    "gemini/gemini-2.0-flash-001",
    "gemini/gemini-1.5-flash",
    "gemini/gemini-1.5-flash-latest",
]

# ── UI ────────────────────────────────────────────────────────
st.markdown("## 🧪 Gemini Direct Test")
st.markdown("Hits **Gemini only** — bypasses the router entirely. Proves your key works.")
st.markdown("---")

key = get_key()

if not key:
    st.error("❌ GEMINI_API_KEY not found in secrets or environment.")
    st.code('GEMINI_API_KEY = "AIzaSy_your_key_here"', language="toml")
    st.stop()

st.success(f"✅ Key found: `{key[:10]}...{key[-4:]}`")

# ── Controls ──────────────────────────────────────────────────
model = st.selectbox("Model string to test", GEMINI_MODELS)
prompt = st.text_input(
    "Test prompt",
    value="Reply with exactly: GEMINI_CONFIRMED and today's date."
)

if st.button("🚀 Hit Gemini NOW", type="primary", use_container_width=True):
    with st.spinner(f"Calling {model} directly — no router, no fallback..."):
        try:
            import litellm
            litellm.drop_params = True
            litellm.set_verbose = False

            t0 = time.time()
            resp = litellm.completion(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=100,
                api_key=key,
            )
            elapsed = time.time() - t0
            reply   = resp.choices[0].message.content.strip()
            tokens  = getattr(resp.usage, "total_tokens", "?")
            actual  = getattr(resp, "model", model)

            # Big green success box
            st.markdown(f"""
<div style="background:#E8F5E9;border:3px solid #2E7D32;border-radius:12px;padding:20px 24px;margin:16px 0;">
  <div style="font-size:22px;font-weight:700;color:#1B5E20;margin-bottom:12px;">
    ✅ GEMINI IS CONFIRMED WORKING
  </div>
  <div style="font-size:15px;color:#2E7D32;margin-bottom:16px;">
    "{reply}"
  </div>
  <table style="font-size:13px;color:#333;border-collapse:collapse;width:100%;">
    <tr><td style="padding:4px 12px 4px 0;color:#666;">Model requested</td><td><b>{model}</b></td></tr>
    <tr><td style="padding:4px 12px 4px 0;color:#666;">Model responded</td><td><b>{actual}</b></td></tr>
    <tr><td style="padding:4px 12px 4px 0;color:#666;">Tokens used</td><td><b>{tokens}</b></td></tr>
    <tr><td style="padding:4px 12px 4px 0;color:#666;">Latency</td><td><b>{elapsed:.2f}s</b></td></tr>
  </table>
</div>
""", unsafe_allow_html=True)

            st.info(f"✅ Use `model=\"{model}\"` in ai_router.py — it works with your key.")

        except Exception as e:
            err = str(e)
            if "429" in err or "rate" in err.lower():
                st.warning(f"🟡 **Rate Limited** — Gemini key is valid but quota hit. Wait 60s and retry.")
                st.code(err[:300])
            elif "401" in err or "403" in err or "auth" in err.lower() or "invalid" in err.lower():
                st.error(f"🔴 **Auth Failed** — Your GEMINI_API_KEY is wrong or expired.")
                st.markdown("Get a new key at: https://aistudio.google.com/app/apikey")
                st.code(err[:300])
            elif "404" in err or "not found" in err.lower() or "not supported" in err.lower():
                st.warning(f"⚫ **Model string not recognised** — try a different model from the dropdown above.")
                st.code(err[:300])
            else:
                st.error(f"❌ Unexpected error")
                st.code(err[:500])

st.markdown("---")
st.markdown("""
**What this confirms:**
- ✅ Your `GEMINI_API_KEY` is valid and loaded correctly
- ✅ The model string works with your version of LiteLLM
- ✅ Gemini responds — so if your main app uses Groq, it's a rate limit / cooldown issue

**If you get Rate Limited here:**
- Wait 60 seconds and try again
- You're on the free tier: **15 requests/minute max**
- In your main app, click **⚡ Reset All Cooldowns** in the Provider Status panel
""")
