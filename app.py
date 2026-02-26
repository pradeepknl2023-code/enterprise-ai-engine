import streamlit as st
import pandas as pd
import numpy as np
import os
import re
import math
import string
import datetime
import json
import time
from groq import Groq
from io import BytesIO

# -----------------------------------
# PAGE CONFIG
# -----------------------------------
st.set_page_config(page_title="Enterprise AI Platform", layout="wide")

# -----------------------------------
# FULL CSS
# -----------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Rajdhani:wght@600;700&display=swap');
* { font-family: 'Inter', sans-serif; }

/* ── BUILT BY BANNER ── */
.built-by-banner {
    display: flex; align-items: center; justify-content: flex-end;
    gap: 8px; padding: 6px 16px 0 0; margin-bottom: -6px;
}
.built-by-banner .byline { font-size: 11px; color: #999; letter-spacing: 0.8px; text-transform: uppercase; }
.built-by-banner .author { font-family: 'Rajdhani', sans-serif; font-size: 15px; font-weight: 700; color: #B31B1B; letter-spacing: 1px; }
.built-by-banner .dot { width: 6px; height: 6px; background: #FFC72C; border-radius: 50%; display: inline-block; }

/* ── MAIN HEADER ── */
.main-header {
    background: linear-gradient(135deg, #B31B1B 0%, #7a1212 100%);
    padding: 22px 28px; border-radius: 10px; margin-bottom: 20px;
    display: flex; align-items: center; justify-content: space-between;
    box-shadow: 0 4px 15px rgba(179,27,27,0.3);
}
.main-header h1 { color: #FFC72C; margin: 0; font-family: 'Rajdhani', sans-serif; font-size: 28px; font-weight: 700; letter-spacing: 1px; }
.main-header .header-sub { color: rgba(255,255,255,0.6); font-size: 12px; margin-top: 4px; letter-spacing: 0.5px; }
.main-header .version-badge { background: rgba(255,199,44,0.15); border: 1px solid rgba(255,199,44,0.4); color: #FFC72C; padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 600; letter-spacing: 1px; }

/* ── SECTION TITLE ── */
.section-title { color: #B31B1B; font-weight: 600; font-size: 16px; margin-top: 20px; text-transform: uppercase; letter-spacing: 0.5px; }

/* ── BUTTONS ── */
.stButton>button { background-color: #B31B1B; color: white; font-weight: bold; border-radius: 6px; }
.stButton>button:hover { background-color: #8E1414; color: #FFC72C; }

/* ── METRICS ROW ── */
.metric-row { display: flex; gap: 12px; margin: 16px 0 8px 0; flex-wrap: wrap; }
.metric-box { background: white; border: 1px solid #E8E8E8; border-top: 3px solid #B31B1B; border-radius: 8px; padding: 14px 18px; min-width: 120px; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.06); flex: 1; }
.metric-box .metric-value { font-size: 28px; font-weight: 700; color: #B31B1B; font-family: 'Rajdhani', sans-serif; }
.metric-box .metric-label { font-size: 10px; color: #999; text-transform: uppercase; letter-spacing: 0.8px; margin-top: 2px; }

/* ── PIPELINE LOG STEPS ── */
.pipeline-steps { padding: 4px 0; }
.pipeline-step { display: flex; align-items: flex-start; gap: 12px; padding: 10px 0; border-bottom: 1px solid #F0F0F0; font-size: 14px; color: #333; }
.pipeline-step:last-child { border-bottom: none; }
.step-num { background: #B31B1B; color: white; border-radius: 50%; width: 22px; height: 22px; display: flex; align-items: center; justify-content: center; font-size: 11px; font-weight: 700; min-width: 22px; }
.step-icon { font-size: 18px; min-width: 24px; }
.step-text { flex: 1; line-height: 1.5; }

/* ── GDE EXECUTION FLOW ── */
.gde-container { background: #0D1117; border-radius: 10px; padding: 24px 20px; margin: 12px 0; overflow-x: auto; box-shadow: inset 0 2px 8px rgba(0,0,0,0.4); }
.gde-flow { display: flex; align-items: center; gap: 0; min-width: max-content; padding: 8px 0; }
.gde-node { display: flex; flex-direction: column; align-items: center; gap: 6px; }
.gde-node-box { border-radius: 8px; padding: 10px 16px; text-align: center; min-width: 120px; transition: all 0.3s ease; }
.gde-node-box.input { background: #1a2744; border: 2px solid #1E90FF; color: #7BB8FF; }
.gde-node-box.transform { background: #1a2a1a; border: 2px solid #00C853; color: #69F0AE; }
.gde-node-box.transform.running { background: #2a2a0a; border: 2px solid #FFD600; color: #FFD600; box-shadow: 0 0 12px rgba(255,214,0,0.4); animation: pulse 1s infinite; }
.gde-node-box.transform.done { background: #0d2137; border: 2px solid #29B6F6; color: #29B6F6; box-shadow: 0 0 10px rgba(41,182,246,0.3); }
.gde-node-box.output { background: #1a1a2e; border: 2px solid #AB47BC; color: #CE93D8; }
.gde-node-box.output.done { background: #0d2137; border: 2px solid #29B6F6; color: #29B6F6; box-shadow: 0 0 10px rgba(41,182,246,0.3); }
@keyframes pulse { 0%,100% { box-shadow: 0 0 8px rgba(255,214,0,0.3); } 50% { box-shadow: 0 0 20px rgba(255,214,0,0.7); } }
.gde-node-title { font-size: 11px; font-weight: 700; letter-spacing: 0.8px; text-transform: uppercase; }
.gde-node-sub { font-size: 10px; opacity: 0.7; margin-top: 2px; }
.gde-node-count { font-size: 14px; font-weight: 700; font-family: 'Rajdhani', sans-serif; margin-top: 4px; }
.gde-node-label { font-size: 10px; color: #666; text-align: center; max-width: 130px; }
.gde-arrow { display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 4px; padding: 0 6px; min-width: 70px; }
.gde-count-label { font-size: 10px; white-space: nowrap; text-align: center; }
.gde-legend { display: flex; gap: 20px; margin-top: 16px; flex-wrap: wrap; }
.gde-legend-item { display: flex; align-items: center; gap: 6px; font-size: 11px; color: #888; }
.legend-dot { width: 10px; height: 10px; border-radius: 2px; }

/* ── JIRA CARDS ── */
.epic-card {
    background: linear-gradient(135deg, #B31B1B 0%, #7a1212 100%);
    border-radius: 10px; padding: 20px 24px; margin: 16px 0;
    box-shadow: 0 4px 15px rgba(179,27,27,0.3);
}
.epic-title { color: #FFC72C; font-family: 'Rajdhani', sans-serif; font-size: 22px; font-weight: 700; letter-spacing: 0.5px; }
.epic-value { color: rgba(255,255,255,0.85); font-size: 13px; margin-top: 6px; line-height: 1.6; }
.epic-meta { display: flex; gap: 10px; margin-top: 12px; flex-wrap: wrap; }
.epic-badge { background: rgba(255,199,44,0.2); border: 1px solid rgba(255,199,44,0.5); color: #FFC72C; padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; }

.story-card {
    background: white; border: 1px solid #E8E8E8; border-left: 4px solid #B31B1B;
    border-radius: 8px; padding: 16px 20px; margin: 10px 0;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
}
.story-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 10px; }
.story-id { font-size: 11px; color: #999; font-weight: 600; letter-spacing: 0.5px; }
.story-title { font-size: 14px; font-weight: 600; color: #1a1a1a; margin: 4px 0 8px 0; line-height: 1.4; }
.story-desc { font-size: 13px; color: #555; line-height: 1.6; font-style: italic; }
.story-badges { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 10px; align-items: center; }
.badge-priority-critical { background: #FFEBEE; color: #C62828; border: 1px solid #EF9A9A; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 700; }
.badge-priority-high { background: #FFF3E0; color: #E65100; border: 1px solid #FFCC80; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 700; }
.badge-priority-medium { background: #FFFDE7; color: #F57F17; border: 1px solid #FFF176; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 700; }
.badge-priority-low { background: #F5F5F5; color: #616161; border: 1px solid #E0E0E0; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 700; }
.badge-points { background: #FFC72C; color: #1a1a1a; padding: 2px 10px; border-radius: 10px; font-size: 12px; font-weight: 700; }
.badge-sprint { background: #E8F5E9; color: #2E7D32; border: 1px solid #A5D6A7; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }
.badge-type { background: #E3F2FD; color: #1565C0; border: 1px solid #90CAF9; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }

.ac-section { margin-top: 12px; }
.ac-title { font-size: 11px; font-weight: 700; color: #B31B1B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
.ac-item { font-size: 12px; color: #444; padding: 4px 0 4px 12px; border-left: 2px solid #FFC72C; margin: 4px 0; line-height: 1.5; }

.subtask-section { margin-top: 10px; }
.subtask-title { font-size: 11px; font-weight: 700; color: #666; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
.subtask-item { display: flex; align-items: center; gap: 8px; font-size: 12px; color: #555; padding: 3px 0; }
.subtask-hrs { font-size: 10px; color: #999; background: #F5F5F5; padding: 1px 6px; border-radius: 8px; }

.risk-card { background: #FFF8E1; border: 1px solid #FFE082; border-left: 4px solid #FFC72C; border-radius: 8px; padding: 14px 18px; margin: 10px 0; }
.risk-title { font-size: 13px; font-weight: 700; color: #E65100; margin-bottom: 6px; }
.risk-item { font-size: 12px; color: #555; padding: 3px 0 3px 12px; border-left: 2px solid #FFB300; margin: 3px 0; }

.dod-card { background: #E8F5E9; border: 1px solid #A5D6A7; border-left: 4px solid #2E7D32; border-radius: 8px; padding: 14px 18px; margin: 10px 0; }
.dod-title { font-size: 13px; font-weight: 700; color: #1B5E20; margin-bottom: 6px; }
.dod-item { font-size: 12px; color: #2E7D32; padding: 3px 0 3px 12px; border-left: 2px solid #66BB6A; margin: 3px 0; }

.jira-metrics { display: flex; gap: 12px; margin: 16px 0; flex-wrap: wrap; }
.jira-metric-box { background: white; border: 1px solid #E8E8E8; border-top: 3px solid #B31B1B; border-radius: 8px; padding: 12px 16px; min-width: 100px; text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.06); flex: 1; }
.jira-metric-value { font-size: 24px; font-weight: 700; color: #B31B1B; font-family: 'Rajdhani', sans-serif; }
.jira-metric-label { font-size: 10px; color: #999; text-transform: uppercase; letter-spacing: 0.8px; }

.project-type-selector { display: flex; gap: 10px; flex-wrap: wrap; margin: 10px 0; }
.pt-badge { padding: 8px 16px; border-radius: 20px; border: 2px solid #E0E0E0; background: white; font-size: 13px; cursor: pointer; font-weight: 500; transition: all 0.2s; }
</style>
""", unsafe_allow_html=True)

# ── BUILT BY BANNER ──
st.markdown("""
<div class="built-by-banner">
    <span class="byline">Built by</span>
    <span class="dot"></span>
    <span class="author">PRADEEP</span>
    <span class="dot"></span>
    <span class="byline">Enterprise AI</span>
</div>
""", unsafe_allow_html=True)

# ── MAIN HEADER ──
st.markdown("""
<div class="main-header">
    <div>
        <div class="header-sub">AI POWERED &nbsp;·&nbsp; LLAMA 3.3 &nbsp;·&nbsp; PANDAS</div>
        <h1>⚡ Enterprise AI Transformation &amp; Delivery Platform</h1>
    </div>
    <div class="version-badge">v3.0</div>
</div>
""", unsafe_allow_html=True)

# -----------------------------------
# GROQ SETUP
# -----------------------------------
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    st.error("Set GROQ_API_KEY in Streamlit Secrets.")
    st.stop()
client = Groq(api_key=GROQ_API_KEY)

if "history" not in st.session_state:
    st.session_state.history = []
if "jira_result" not in st.session_state:
    st.session_state.jira_result = None


# ============================================================
# UTILITY
# ============================================================
def extract_code(raw: str) -> str:
    fenced = re.search(r"```(?:python)?\s*\n(.*?)```", raw, re.DOTALL)
    if fenced:
        return fenced.group(1).strip()
    lines = [ln for ln in raw.splitlines() if not ln.strip().startswith("```")]
    return "\n".join(lines).strip()


# ============================================================
# GDE EXECUTION FLOW — Real-time with step-by-step animation
# ============================================================
def make_gde_html(dataframes, file_names, code, result_df, state,
                  read_count=0, transform_count=0, out_count=0):
    """
    state: 'idle' | 'reading' | 'transforming' | 'done'
    Counts update at each stage to show real record flow.
    """
    aliases = list(dataframes.keys())
    real_aliases = [a for a in aliases if a != "df"]
    if not real_aliases:
        real_aliases = list(aliases)[:1] or ["df"]

    has_join = len(real_aliases) >= 2

    # Detect ops from code
    code_lower = (code or "").lower()
    trans_ops = []
    if "merge" in code_lower or "join" in code_lower: trans_ops.append("JOIN")
    if "groupby" in code_lower and "rank" in code_lower: trans_ops.append("RANK")
    if "pd.cut" in code_lower or "pd.qcut" in code_lower: trans_ops.append("BUCKET")
    if "re.sub" in code_lower or "replace" in code_lower: trans_ops.append("CLEAN")
    if "fillna" in code_lower: trans_ops.append("FILLNA")
    if not trans_ops: trans_ops.append("TRANSFORM")
    trans_label = " · ".join(trans_ops[:3])

    primary_rows    = dataframes[real_aliases[0]].shape[0]
    secondary_rows  = dataframes[real_aliases[1]].shape[0] if has_join else 0
    total_in        = primary_rows + secondary_rows if has_join else primary_rows

    fname1 = file_names[0] if file_names else "file1.csv"
    fname2 = file_names[1] if len(file_names) > 1 else ""

    out_rows = len(result_df) if state == "done" and result_df is not None else out_count
    out_cols = len(result_df.columns) if state == "done" and result_df is not None else 0

    # Arrow colours per state
    arrow1_color = "#29B6F6" if state in ("transforming","done") else ("#FFD600" if state == "reading" else "#444")
    arrow2_color = "#29B6F6" if state == "done" else "#444"

    # Node states
    input_border  = "#1E90FF" if state in ("reading","transforming","done") else "#333"
    input_bg      = "#1a2744" if state in ("reading","transforming","done") else "#111"
    input_color   = "#7BB8FF" if state in ("reading","transforming","done") else "#444"

    if state == "transforming":
        trans_border = "#FFD600"; trans_bg = "#2a2a0a"; trans_color = "#FFD600"; trans_anim = "animation: pulse 1s infinite;"
    elif state == "done":
        trans_border = "#29B6F6"; trans_bg = "#0d2137"; trans_color = "#29B6F6"; trans_anim = ""
    else:
        trans_border = "#333"; trans_bg = "#111"; trans_color = "#444"; trans_anim = ""

    if state == "done":
        out_border = "#29B6F6"; out_bg = "#0d2137"; out_color = "#29B6F6"
    else:
        out_border = "#AB47BC"; out_bg = "#1a1a2e"; out_color = "#CE93D8"

    trans_status = "🟡 RUNNING" if state == "transforming" else ("🔵 COMPLETE" if state == "done" else "⏳ WAITING")

    # Record counts shown at each node — show real numbers as they flow
    in_count_display  = f"{primary_rows:,}" if state in ("reading","transforming","done") else "–"
    in2_count_display = f"{secondary_rows:,}" if has_join and state in ("reading","transforming","done") else "–"
    tr_count_display  = f"{total_in:,} rec" if state in ("transforming","done") else "–"
    out_count_display = f"{out_rows:,}" if state == "done" else "–"

    def svg_arrow(color, label=""):
        uid = abs(hash(label + color)) % 99999
        return f"""
        <div class="gde-arrow">
            <svg width="70" height="18" viewBox="0 0 70 18" xmlns="http://www.w3.org/2000/svg">
                <defs>
                    <marker id="ah{uid}" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
                        <polygon points="0 0, 6 3, 0 6" fill="{color}" />
                    </marker>
                </defs>
                <line x1="2" y1="9" x2="62" y2="9" stroke="{color}" stroke-width="2.5" marker-end="url(#ah{uid})" />
            </svg>
            <div class="gde-count-label" style="color:{color};">{label}</div>
        </div>"""

    # Build flow HTML
    html = '<div class="gde-flow">'

    if has_join:
        html += f"""
        <div class="gde-node">
            <div style="display:flex;flex-direction:column;gap:10px;">
                <div class="gde-node">
                    <div class="gde-node-box" style="background:{input_bg};border:2px solid {input_border};color:{input_color};min-width:120px;border-radius:8px;padding:10px 14px;text-align:center;">
                        <div class="gde-node-title">📂 INPUT 1</div>
                        <div class="gde-node-sub">{fname1}</div>
                        <div class="gde-node-count">{in_count_display} rows</div>
                    </div>
                    <div class="gde-node-label">{real_aliases[0]}</div>
                </div>
                <div class="gde-node">
                    <div class="gde-node-box" style="background:{input_bg};border:2px solid {input_border};color:{input_color};min-width:120px;border-radius:8px;padding:10px 14px;text-align:center;">
                        <div class="gde-node-title">📂 INPUT 2</div>
                        <div class="gde-node-sub">{fname2}</div>
                        <div class="gde-node-count">{in2_count_display} rows</div>
                    </div>
                    <div class="gde-node-label">{real_aliases[1]}</div>
                </div>
            </div>
        </div>"""
        html += svg_arrow(arrow1_color, f"{primary_rows+secondary_rows:,} in" if state in ("transforming","done") else "")
    else:
        html += f"""
        <div class="gde-node">
            <div class="gde-node-box" style="background:{input_bg};border:2px solid {input_border};color:{input_color};min-width:120px;border-radius:8px;padding:10px 14px;text-align:center;">
                <div class="gde-node-title">📂 INPUT</div>
                <div class="gde-node-sub">{fname1}</div>
                <div class="gde-node-count">{in_count_display} rows</div>
            </div>
            <div class="gde-node-label">{real_aliases[0]}</div>
        </div>"""
        html += svg_arrow(arrow1_color, f"{primary_rows:,} in" if state in ("transforming","done") else "")

    html += f"""
    <div class="gde-node">
        <div class="gde-node-box" style="background:{trans_bg};border:2px solid {trans_border};color:{trans_color};min-width:130px;border-radius:8px;padding:10px 14px;text-align:center;{trans_anim}">
            <div class="gde-node-title">⚙ {trans_label}</div>
            <div class="gde-node-sub">AI GENERATED</div>
            <div class="gde-node-count">{tr_count_display}</div>
        </div>
        <div class="gde-node-label">{trans_status}</div>
    </div>"""

    html += svg_arrow(arrow2_color, f"{out_count_display} out" if state == "done" else "")

    html += f"""
    <div class="gde-node">
        <div class="gde-node-box" style="background:{out_bg};border:2px solid {out_border};color:{out_color};min-width:120px;border-radius:8px;padding:10px 14px;text-align:center;">
            <div class="gde-node-title">💾 OUTPUT</div>
            <div class="gde-node-sub">{out_cols} columns</div>
            <div class="gde-node-count">{out_count_display} rows</div>
        </div>
        <div class="gde-node-label">RESULT</div>
    </div>"""

    html += "</div>"

    legend = """
    <div class="gde-legend">
        <div class="gde-legend-item"><div class="legend-dot" style="background:#1E90FF;"></div> Input</div>
        <div class="gde-legend-item"><div class="legend-dot" style="background:#FFD600;"></div> Running</div>
        <div class="gde-legend-item"><div class="legend-dot" style="background:#29B6F6;"></div> Complete</div>
        <div class="gde-legend-item"><div class="legend-dot" style="background:#AB47BC;"></div> Output</div>
    </div>"""

    return f'<div class="gde-container">{html}{legend}</div>'


# ============================================================
# PIPELINE LOG
# ============================================================
def build_pipeline_log(code, dataframes, result_df, file_names, original_rows):
    aliases = list(dataframes.keys())
    summary_prompt = f"""
You are a data pipeline narrator for a business audience (no technical background).
Code executed:
```python
{code}
```
Input files: {file_names}
Rows before: {original_rows}, Rows after: {len(result_df)}
Columns in result: {result_df.columns.tolist()}

Describe what this pipeline did in 4-8 plain-English bullet steps.
Rules:
- NO Python, NO code, NO technical jargon.
- Each step starts with action verb: Loaded, Joined, Cleaned, Computed, Filtered, Sorted, Selected.
- Mention actual column names and values.
- One sentence per step max.
- Return ONLY a JSON array of strings: ["Step one", "Step two"]
- No markdown, no explanation outside the JSON array.
"""
    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": summary_prompt}],
            temperature=0.2,
        )
        raw = resp.choices[0].message.content.strip()
        arr_match = re.search(r"\[.*\]", raw, re.DOTALL)
        steps = json.loads(arr_match.group()) if arr_match else [raw]
    except Exception:
        steps = [
            f"Loaded {original_rows:,} rows from {', '.join(file_names)}",
            "Applied AI-generated transformations",
            f"Produced {len(result_df):,} rows × {len(result_df.columns)} columns",
        ]

    icon_map = {
        "load":"📂","read":"📂","join":"🔗","merge":"🔗","combined":"🔗",
        "clean":"🧹","strip":"🧹","remov":"🧹","replac":"🧹",
        "comput":"⚙️","calculat":"⚙️","creat":"⚙️","add":"⚙️","generat":"⚙️",
        "filter":"🔍","kept":"🔍","exclud":"🔍","select":"🔍",
        "sort":"↕️","order":"↕️","rank":"🏅","format":"✏️",
    }
    def pick_icon(t):
        tl = t.lower()
        for kw, icon in icon_map.items():
            if kw in tl: return icon
        return "✅"

    steps_html = '<div class="pipeline-steps">'
    for i, step in enumerate(steps, 1):
        icon = pick_icon(step)
        steps_html += f"""
        <div class="pipeline-step">
            <span class="step-num">{i}</span>
            <span class="step-icon">{icon}</span>
            <span class="step-text">{step}</span>
        </div>"""
    steps_html += "</div>"

    new_cols = [c for c in result_df.columns if c not in list(dataframes.values())[0].columns]
    n_joins = max(0, len([a for a in aliases if a != "df"]) - 1)

    metrics_html = f"""
    <div class="metric-row">
        <div class="metric-box"><div class="metric-value">{len(file_names)}</div><div class="metric-label">Files</div></div>
        <div class="metric-box"><div class="metric-value">{original_rows:,}</div><div class="metric-label">Rows In</div></div>
        <div class="metric-box"><div class="metric-value">{len(result_df):,}</div><div class="metric-label">Rows Out</div></div>
        <div class="metric-box"><div class="metric-value">{len(result_df.columns)}</div><div class="metric-label">Columns</div></div>
        <div class="metric-box"><div class="metric-value">{len(new_cols)}</div><div class="metric-label">New Cols</div></div>
        <div class="metric-box"><div class="metric-value">{n_joins}</div><div class="metric-label">Joins</div></div>
    </div>"""

    return metrics_html, steps_html


# ============================================================
# EXECUTION ENGINE
# ============================================================
def safe_exec_multi(dataframes: dict, code: str) -> pd.DataFrame:
    code = extract_code(code)
    exec_globals = {**globals(), **dataframes}
    try:
        exec(compile(code, "<ai_etl>", "exec"), exec_globals)
    except Exception as exc:
        raise RuntimeError(f"Execution failed: {exc}\n\nCode:\n{code}") from exc
    primary = "df" if "df" in dataframes else list(dataframes.keys())[0]
    output = exec_globals.get("result", exec_globals.get(primary, list(dataframes.values())[0]))
    if not isinstance(output, pd.DataFrame):
        raise RuntimeError(f"AI produced {type(output).__name__} instead of DataFrame.")
    return output


# ============================================================
# SYSTEM PROMPT BUILDER (ETL)
# ============================================================
def build_system_prompt(dataframes: dict) -> str:
    schema_lines = ""
    for alias, df in dataframes.items():
        dtypes = {c: str(t) for c, t in df.dtypes.items()}
        schema_lines += f"\n  {alias}: columns={df.columns.tolist()}, dtypes={dtypes}, shape={df.shape}"

    aliases = list(dataframes.keys())
    primary = aliases[0] if aliases else "df"
    join_examples = ""
    if len(aliases) >= 2:
        a, b = aliases[0], aliases[1]
        common = list(set(dataframes[a].columns) & set(dataframes[b].columns))
        jcol = common[0] if common else "id"
        join_examples = f"""
result = pd.merge({a}, {b}, on='{jcol}', how='inner')
result = pd.merge({a}, {b}, on='{jcol}', how='left')
result = pd.merge({a}, {b}, left_on='emp_id', right_on='employee_id', how='inner')
"""

    return f"""You are a Senior Enterprise Data Engineer. Follow every rule below.
AVAILABLE DATAFRAMES:{schema_lines}
PRIMARY DATAFRAME: '{primary}'
RULES:
1. Use ONLY the dataframe aliases listed above.
2. Store the final output in a variable named 'result'.
3. You MAY write `import` statements for any standard Python module.
4. Handle nulls: .fillna("") for strings, .fillna(0) for numerics.
5. Strip whitespace: .str.strip() before string comparisons.
6. String equality: .str.strip().str.lower() == "value".
7. Use vectorised pandas operations — never iterate rows with loops.
8. Do NOT output any explanation, markdown fences, or comments.
9. Return ONLY executable Python code.
10. Apply exact filters from the prompt.
11. Cast numeric columns where needed: pd.to_numeric(..., errors='coerce').
FEW-SHOT EXAMPLES:
result = {primary}[{primary}['SALARY'] > 70000]
result = {primary}[{primary}['DEPARTMENT'].str.strip().str.lower() == "it"]
import re
df_tmp = {primary}.copy()
df_tmp['PHONE_CLEAN'] = df_tmp['PHONE_NUMBER'].apply(lambda x: re.sub(r'[^0-9]', '', str(x)))
result = df_tmp
df_tmp = {primary}.copy()
df_tmp['RANK'] = df_tmp.groupby('DEPARTMENT_ID')['SALARY'].rank(method='dense', ascending=False)
result = df_tmp
df_tmp = {primary}.copy()
df_tmp['GRADE'] = pd.cut(df_tmp['SALARY'], bins=[0,10000,20000,float('inf')], labels=['LOW','MEDIUM','HIGH'])
result = df_tmp
{join_examples}"""


# ============================================================
# JIRA SYSTEM PROMPT — per project type
# ============================================================
PROJECT_TYPE_PROMPTS = {
    "🌐 Web Application": """You are a Senior Agile Delivery Manager specialising in Web Application delivery.
Focus on: Frontend components, REST APIs, authentication flows, responsive design, browser compatibility, SEO, performance optimisation, CI/CD pipelines.""",

    "📱 Mobile App": """You are a Senior Agile Delivery Manager specialising in Mobile Application delivery (iOS/Android).
Focus on: Native/cross-platform considerations, offline capability, push notifications, app store submission, device compatibility, mobile UX patterns, battery/data optimisation.""",

    "📊 Data / ETL Pipeline": """You are a Senior Agile Delivery Manager specialising in Data Engineering and ETL pipelines.
Focus on: Data ingestion, transformation logic, data quality checks, pipeline monitoring, schema validation, incremental loads, error handling, data lineage, scheduling.""",

    "🔗 API / Integration": """You are a Senior Agile Delivery Manager specialising in API and Systems Integration.
Focus on: API design (REST/GraphQL), authentication (OAuth/JWT), rate limiting, versioning, third-party integrations, webhook handling, contract testing, API documentation.""",

    "☁️ Cloud / Infrastructure": """You are a Senior Agile Delivery Manager specialising in Cloud Infrastructure and DevOps.
Focus on: Infrastructure as Code, cloud architecture, auto-scaling, cost optimisation, security hardening, monitoring/alerting, disaster recovery, containerisation (Docker/K8s).""",

    "🔒 Security Feature": """You are a Senior Agile Delivery Manager specialising in Cybersecurity features.
Focus on: Threat modelling, OWASP compliance, penetration testing requirements, encryption standards, access control, audit logging, security scanning in CI/CD, compliance (GDPR/SOC2).""",

    "🤖 AI / ML Feature": """You are a Senior Agile Delivery Manager specialising in AI and Machine Learning product delivery.
Focus on: Model training pipeline, data preparation, model evaluation metrics, A/B testing, model serving infrastructure, monitoring for drift, explainability, responsible AI considerations.""",

    "📋 General / Other": """You are a Senior Agile Delivery Manager with 15+ years enterprise software delivery experience across all domains.
Apply best practices for Agile delivery, sprint planning, and technical decomposition.""",
}

def build_jira_prompt(description, project_type, team_size, sprint_length, methodology):
    system = PROJECT_TYPE_PROMPTS.get(project_type, PROJECT_TYPE_PROMPTS["📋 General / Other"])

    user = f"""
BUSINESS REQUIREMENT:
{description}

TEAM CONTEXT:
- Project Type: {project_type}
- Team Size: {team_size} people
- Sprint Length: {sprint_length} weeks
- Methodology: {methodology}

Generate a complete, production-ready Jira breakdown. Return ONLY valid JSON in exactly this structure:
{{
  "epic": {{
    "title": "Epic title here",
    "business_value": "Clear business value statement (2-3 sentences)",
    "objective": "Specific measurable objective",
    "estimated_sprints": 3,
    "definition_of_done": ["DOD item 1", "DOD item 2", "DOD item 3"]
  }},
  "stories": [
    {{
      "id": "US-001",
      "title": "User story title",
      "user_story": "As a [role], I want [feature], so that [benefit]",
      "priority": "High",
      "story_points": 5,
      "sprint": "Sprint 1",
      "type": "Feature",
      "acceptance_criteria": [
        "Given [context], When [action], Then [outcome]",
        "Given [context], When [action], Then [outcome]"
      ],
      "subtasks": [
        {{"title": "Subtask description", "hours": 4}},
        {{"title": "Subtask description", "hours": 3}}
      ]
    }}
  ],
  "risks": [
    {{"title": "Risk title", "description": "Risk description and mitigation"}}
  ],
  "dependencies": ["Dependency 1", "Dependency 2"]
}}

RULES:
- Generate 4-7 User Stories covering the full scope
- Story Points must be Fibonacci: 1, 2, 3, 5, 8, 13
- Priority must be one of: Critical, High, Medium, Low
- Type must be one of: Feature, Bug, Technical Debt, Spike, Enhancement
- Each story needs 3-5 Acceptance Criteria in Gherkin (Given/When/Then)
- Each story needs 2-4 Subtasks with realistic hour estimates
- Include 2-4 risks with mitigation strategies
- Assign stories to sprints logically (dependencies first)
- Return ONLY the JSON object, no markdown, no explanation
"""
    return system, user


def render_jira_cards(data):
    """Render parsed Jira JSON as styled cards"""
    epic = data.get("epic", {})
    stories = data.get("stories", [])
    risks = data.get("risks", [])
    dependencies = data.get("dependencies", [])

    # ── METRICS ROW ──
    total_points = sum(s.get("story_points", 0) for s in stories)
    sprints_needed = epic.get("estimated_sprints", "?")
    html = f"""
    <div class="jira-metrics">
        <div class="jira-metric-box"><div class="jira-metric-value">{len(stories)}</div><div class="jira-metric-label">Stories</div></div>
        <div class="jira-metric-box"><div class="jira-metric-value">{total_points}</div><div class="jira-metric-label">Total Points</div></div>
        <div class="jira-metric-box"><div class="jira-metric-value">{sprints_needed}</div><div class="jira-metric-label">Sprints</div></div>
        <div class="jira-metric-box"><div class="jira-metric-value">{len(risks)}</div><div class="jira-metric-label">Risks</div></div>
        <div class="jira-metric-box"><div class="jira-metric-value">{len(dependencies)}</div><div class="jira-metric-label">Dependencies</div></div>
    </div>"""

    # ── EPIC CARD ──
    dod_items = "".join(f'<div class="dod-item">✓ {d}</div>' for d in epic.get("definition_of_done", []))
    html += f"""
    <div class="epic-card">
        <div class="epic-title">🏆 EPIC: {epic.get('title','')}</div>
        <div class="epic-value">{epic.get('business_value','')}</div>
        <div class="epic-value" style="margin-top:6px;"><b>Objective:</b> {epic.get('objective','')}</div>
        <div class="epic-meta">
            <span class="epic-badge">📅 {sprints_needed} Sprints</span>
            <span class="epic-badge">📊 {total_points} Story Points</span>
            <span class="epic-badge">📝 {len(stories)} Stories</span>
        </div>
    </div>"""

    # DOD
    if epic.get("definition_of_done"):
        html += f'<div class="dod-card"><div class="dod-title">✅ Definition of Done</div>{dod_items}</div>'

    return html, stories, risks, dependencies


# ============================================================
# TABS
# ============================================================
tab1, tab2, tab3 = st.tabs(["⚡ AI ETL Engine", "📋 AI Jira Breakdown", "🎬 Demo Video"])


# ============================================================
# TAB 1 — AI ETL ENGINE
# ============================================================
with tab1:
    st.markdown('<div class="section-title">Business Description</div>', unsafe_allow_html=True)
    etl_prompt = st.text_area("Describe data transformation", key="etl_prompt", height=140)

    uploaded_files = st.file_uploader(
        "Upload CSV File(s) — upload multiple files to enable joins",
        type=["csv"], accept_multiple_files=True, key="etl_upload",
    )

    if uploaded_files:
        st.markdown('<div class="section-title">Uploaded Files Preview</div>', unsafe_allow_html=True)
        _preview_dfs = {}
        for i, f in enumerate(uploaded_files):
            alias = f"df{i+1}" if len(uploaded_files) > 1 else "df"
            _df = pd.read_csv(f)
            _preview_dfs[alias] = _df
            with st.expander(f"📄 {f.name}  →  alias: `{alias}`  |  {_df.shape[0]:,} rows × {_df.shape[1]} cols"):
                st.dataframe(_df.head(5), use_container_width=True)
        if len(uploaded_files) > 1:
            st.info(f"**{len(uploaded_files)} files loaded.** Reference them as: {', '.join(f'`{a}`' for a in _preview_dfs)}.")

    if st.button("▶ Execute ETL", key="run_etl"):
        if not etl_prompt.strip():
            st.warning("Enter a transformation description.")
            st.stop()
        if not uploaded_files:
            st.warning("Upload at least one CSV file.")
            st.stop()

        dataframes = {}
        for i, f in enumerate(uploaded_files):
            f.seek(0)
            alias = f"df{i+1}" if len(uploaded_files) > 1 else "df"
            dataframes[alias] = pd.read_csv(f)
        if len(uploaded_files) == 1:
            dataframes["df"] = list(dataframes.values())[0]

        primary_alias  = "df" if len(uploaded_files) == 1 else "df1"
        original_rows  = dataframes[primary_alias].shape[0]
        system_prompt  = build_system_prompt(dataframes)
        file_names     = [f.name for f in uploaded_files]

        # ── GDE FLOW SECTION ──
        st.markdown('<div class="section-title">⚡ Execution Flow</div>', unsafe_allow_html=True)
        gde_slot = st.empty()

        # STAGE 1 — READING (inputs light up, arrow activates)
        gde_slot.markdown(make_gde_html(dataframes, file_names, "", None, "reading"), unsafe_allow_html=True)
        time.sleep(0.6)

        # ── AI CODE GENERATION + EXECUTION ──
        MAX_ATTEMPTS  = 3
        ai_code       = ""
        transformed_df = None
        last_error    = None
        successful_attempt = 1

        conversation = [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": etl_prompt},
        ]

        # STAGE 2 — TRANSFORMING (yellow pulse)
        gde_slot.markdown(make_gde_html(dataframes, file_names, "", None, "transforming"), unsafe_allow_html=True)

        with st.spinner("⚙️ AI is generating and executing pipeline..."):
            for attempt in range(1, MAX_ATTEMPTS + 1):
                if last_error and attempt > 1:
                    conversation.append({"role": "assistant", "content": ai_code})
                    conversation.append({
                        "role": "user",
                        "content": (
                            f"Attempt {attempt-1} raised:\n{last_error}\n\n"
                            "Fix: no markdown fences, store result in 'result', "
                            "don't redefine pd or dataframe aliases."
                        ),
                    })
                response = client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=conversation, temperature=0.1,
                )
                ai_code = response.choices[0].message.content
                try:
                    transformed_df = safe_exec_multi(dataframes, ai_code)
                    last_error = None
                    successful_attempt = attempt
                    break
                except Exception as exc:
                    last_error = str(exc)
                    if attempt == MAX_ATTEMPTS:
                        st.error(f"ETL failed after {MAX_ATTEMPTS} attempts.\n\n{exc}")
                        transformed_df = list(dataframes.values())[0].copy()

        # STAGE 3 — DONE (all blue, real output counts)
        gde_slot.markdown(
            make_gde_html(dataframes, file_names, extract_code(ai_code), transformed_df, "done"),
            unsafe_allow_html=True
        )

        # ── PIPELINE SUMMARY ──
        st.markdown('<div class="section-title">📊 Pipeline Execution Summary</div>', unsafe_allow_html=True)
        with st.spinner("Generating plain-English pipeline summary..."):
            metrics_html, steps_html = build_pipeline_log(
                extract_code(ai_code), dataframes, transformed_df, file_names, original_rows,
            )
        st.markdown(metrics_html, unsafe_allow_html=True)
        with st.expander("📋 View detailed pipeline steps", expanded=False):
            st.markdown(steps_html, unsafe_allow_html=True)

        # ── RESULTS ──
        st.markdown('<div class="section-title">Transformed Output</div>', unsafe_allow_html=True)
        total_rows = len(transformed_df)
        col_info, col_select = st.columns([3, 1])
        col_info.markdown(
            f"<span style='font-size:13px;color:#666;'>Total records: <b>{total_rows:,}</b></span>",
            unsafe_allow_html=True
        )
        display_options = [20, 50, 100, 500, 1000]
        if total_rows <= 50000:
            display_options.append(total_rows)
        display_options = sorted(set(n for n in display_options if n <= total_rows)) or [total_rows]
        display_n = col_select.selectbox("Show rows", options=display_options, index=0, key="display_rows")
        st.dataframe(transformed_df.head(display_n), use_container_width=True)

        # Audit log
        st.session_state.history.append({
            "Time":        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Prompt":      etl_prompt,
            "Files":       ", ".join(file_names),
            "Rows Before": original_rows,
            "Rows After":  len(transformed_df),
            "Status":      "OK" if last_error is None else "FAILED",
        })

        # ── EXPORT ──
        st.markdown('<div class="section-title">Export Results</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        csv_bytes = transformed_df.to_csv(index=False).encode("utf-8")
        col1.download_button("⬇ Download CSV", csv_bytes, "etl_output.csv", "text/csv")
        xlsx_buf = BytesIO()
        with pd.ExcelWriter(xlsx_buf, engine="xlsxwriter") as writer:
            transformed_df.to_excel(writer, sheet_name="Transformed_Data", index=False)
            pd.DataFrame(st.session_state.history).to_excel(writer, sheet_name="Audit_Log", index=False)
        col2.download_button(
            "⬇ Download Excel", xlsx_buf.getvalue(), "etl_output.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


# ============================================================
# TAB 2 — AI JIRA BREAKDOWN (fully upgraded)
# ============================================================
with tab2:

    st.markdown('<div class="section-title">Project Configuration</div>', unsafe_allow_html=True)

    # ── PROJECT TYPE ──
    project_type = st.selectbox(
        "Project Type",
        list(PROJECT_TYPE_PROMPTS.keys()),
        key="proj_type"
    )

    # ── TEAM CONFIG ──
    col_a, col_b, col_c = st.columns(3)
    team_size     = col_a.selectbox("Team Size", [2,3,4,5,6,8,10,12,15,20], index=3, key="team_size")
    sprint_length = col_b.selectbox("Sprint Length (weeks)", [1, 2, 3], index=1, key="sprint_len")
    methodology   = col_c.selectbox("Methodology", ["Scrum", "Kanban", "SAFe", "Scrumban"], key="methodology")

    st.markdown('<div class="section-title">Business Requirement</div>', unsafe_allow_html=True)
    jira_prompt = st.text_area(
        "Describe the feature, initiative or product requirement in detail",
        key="jira_prompt", height=160,
        placeholder="Example: Build a customer self-service portal where users can view invoices, raise support tickets, track order status, and manage their account profile. Must support SSO login and mobile-responsive design."
    )

    if st.button("🚀 Generate Jira Breakdown", key="run_jira"):
        if not jira_prompt.strip():
            st.warning("Enter a business requirement.")
            st.stop()

        with st.spinner(f"🧠 Generating {project_type} Jira breakdown using LLaMA 3.3 70B..."):
            sys_prompt, user_prompt = build_jira_prompt(
                jira_prompt, project_type, team_size, sprint_length, methodology
            )
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": sys_prompt},
                    {"role": "user",   "content": user_prompt},
                ],
                temperature=0.3,
                max_tokens=4000,
            )
            raw_output = response.choices[0].message.content.strip()

        # Parse JSON
        try:
            json_match = re.search(r"\{.*\}", raw_output, re.DOTALL)
            jira_data  = json.loads(json_match.group()) if json_match else {}
        except Exception:
            jira_data = {}

        if not jira_data:
            st.error("Could not parse structured output. Showing raw response:")
            st.markdown(raw_output)
            st.stop()

        st.session_state.jira_result = {"data": jira_data, "raw": raw_output, "type": project_type}

    # ── RENDER RESULTS ──
    if st.session_state.jira_result:
        jira_data    = st.session_state.jira_result["data"]
        project_type = st.session_state.jira_result["type"]
        stories      = jira_data.get("stories", [])
        risks        = jira_data.get("risks", [])
        dependencies = jira_data.get("dependencies", [])

        st.markdown('<div class="section-title">📊 Breakdown Summary</div>', unsafe_allow_html=True)
        header_html, stories, risks, dependencies = render_jira_cards(jira_data)
        st.markdown(header_html, unsafe_allow_html=True)

        # ── USER STORIES — each as expander card ──
        st.markdown('<div class="section-title">📝 User Stories</div>', unsafe_allow_html=True)

        priority_badge = {
            "critical": "badge-priority-critical",
            "high":     "badge-priority-high",
            "medium":   "badge-priority-medium",
            "low":      "badge-priority-low",
        }

        for story in stories:
            pri   = story.get("priority", "Medium")
            pts   = story.get("story_points", 0)
            sid   = story.get("id", "US-?")
            stype = story.get("type", "Feature")
            pbadge = priority_badge.get(pri.lower(), "badge-priority-medium")

            with st.expander(f"  {sid} · {story.get('title','')}  [{pri}] [{pts} pts]", expanded=False):
                # Story card HTML
                ac_items = "".join(
                    f'<div class="ac-item">• {ac}</div>'
                    for ac in story.get("acceptance_criteria", [])
                )
                sub_items = "".join(
                    f'<div class="subtask-item">☐ {s.get("title","")} <span class="subtask-hrs">~{s.get("hours",0)}h</span></div>'
                    for s in story.get("subtasks", [])
                )

                st.markdown(f"""
                <div class="story-card">
                    <div class="story-id">{sid} &nbsp;·&nbsp; {project_type}</div>
                    <div class="story-title">{story.get('title','')}</div>
                    <div class="story-desc">{story.get('user_story','')}</div>
                    <div class="story-badges">
                        <span class="{pbadge}">🔴 {pri}</span>
                        <span class="badge-points">⭐ {pts} pts</span>
                        <span class="badge-sprint">🏃 {story.get('sprint','')}</span>
                        <span class="badge-type">🏷 {stype}</span>
                    </div>
                    <div class="ac-section">
                        <div class="ac-title">✅ Acceptance Criteria</div>
                        {ac_items}
                    </div>
                    <div class="subtask-section">
                        <div class="subtask-title">🔧 Subtasks</div>
                        {sub_items}
                    </div>
                </div>""", unsafe_allow_html=True)

                # Copy story button
                story_text = f"{sid}: {story.get('user_story','')}\n\nAC:\n" + "\n".join(
                    f"- {ac}" for ac in story.get("acceptance_criteria", [])
                )
                st.code(story_text, language=None)

        # ── RISKS ──
        if risks:
            st.markdown('<div class="section-title">⚠️ Risks & Dependencies</div>', unsafe_allow_html=True)
            risk_html = '<div class="risk-card"><div class="risk-title">⚠️ Identified Risks</div>'
            for r in risks:
                risk_html += f'<div class="risk-item"><b>{r.get("title","")}</b> — {r.get("description","")}</div>'
            risk_html += "</div>"
            if dependencies:
                risk_html += '<div class="risk-card" style="border-left-color:#1E90FF;background:#E3F2FD;"><div class="risk-title" style="color:#1565C0;">🔗 Dependencies</div>'
                for d in dependencies:
                    risk_html += f'<div class="risk-item" style="border-left-color:#1E90FF;">{d}</div>'
                risk_html += "</div>"
            st.markdown(risk_html, unsafe_allow_html=True)

        # ── EXPORT ──
        st.markdown('<div class="section-title">Export Jira Output</div>', unsafe_allow_html=True)
        col1, col2, col3 = st.columns(3)

        # TXT export
        txt_lines = [f"EPIC: {jira_data.get('epic',{}).get('title','')}\n"]
        for s in stories:
            txt_lines.append(f"\n{s.get('id','')} — {s.get('title','')}")
            txt_lines.append(f"  {s.get('user_story','')}")
            txt_lines.append(f"  Priority: {s.get('priority','')} | Points: {s.get('story_points','')} | Sprint: {s.get('sprint','')}")
            txt_lines.append("  Acceptance Criteria:")
            for ac in s.get("acceptance_criteria", []):
                txt_lines.append(f"    - {ac}")
            txt_lines.append("  Subtasks:")
            for sub in s.get("subtasks", []):
                txt_lines.append(f"    □ {sub.get('title','')} (~{sub.get('hours',0)}h)")
        col1.download_button("⬇ Download TXT", "\n".join(txt_lines), "jira_breakdown.txt", "text/plain")

        # Excel export — one sheet per story
        xlsx_buf = BytesIO()
        with pd.ExcelWriter(xlsx_buf, engine="xlsxwriter") as writer:
            # Summary sheet
            summary_rows = []
            for s in stories:
                summary_rows.append({
                    "ID": s.get("id",""), "Title": s.get("title",""),
                    "User Story": s.get("user_story",""), "Priority": s.get("priority",""),
                    "Story Points": s.get("story_points",""), "Sprint": s.get("sprint",""),
                    "Type": s.get("type",""),
                })
            pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Stories_Summary", index=False)

            # AC sheet
            ac_rows = []
            for s in stories:
                for ac in s.get("acceptance_criteria", []):
                    ac_rows.append({"Story ID": s.get("id",""), "Story Title": s.get("title",""), "Acceptance Criteria": ac})
            if ac_rows:
                pd.DataFrame(ac_rows).to_excel(writer, sheet_name="Acceptance_Criteria", index=False)

            # Subtasks sheet
            sub_rows = []
            for s in stories:
                for sub in s.get("subtasks", []):
                    sub_rows.append({"Story ID": s.get("id",""), "Subtask": sub.get("title",""), "Estimated Hours": sub.get("hours",0)})
            if sub_rows:
                pd.DataFrame(sub_rows).to_excel(writer, sheet_name="Subtasks", index=False)

            # Risks sheet
            if risks:
                pd.DataFrame(risks).to_excel(writer, sheet_name="Risks", index=False)

        col2.download_button(
            "⬇ Download Excel (Jira Import)",
            xlsx_buf.getvalue(), "jira_breakdown.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # JSON export
        col3.download_button(
            "⬇ Download JSON",
            json.dumps(jira_data, indent=2),
            "jira_breakdown.json", "application/json"
        )




# ============================================================
# TAB 3 — DEMO VIDEO
# ============================================================
with tab3:
    st.markdown('<div class="section-title">🎬 Platform Demo</div>', unsafe_allow_html=True)

    # Info banner
    st.markdown("""
    <div style="background:linear-gradient(135deg,#B31B1B,#7a1212);border-radius:10px;
                padding:20px 28px;margin-bottom:20px;box-shadow:0 4px 15px rgba(179,27,27,0.3);">
        <div style="color:#FFC72C;font-family:'Rajdhani',sans-serif;font-size:22px;font-weight:700;">
            ⚡ See the Platform in Action
        </div>
        <div style="color:rgba(255,255,255,0.85);font-size:13px;margin-top:8px;line-height:1.7;">
            This short demo walks through both tools — the AI ETL Engine and AI Jira Breakdown —
            showing real-time execution flow, record counts, card-based outputs and export options.
        </div>
        <div style="display:flex;gap:12px;margin-top:14px;flex-wrap:wrap;">
            <span style="background:rgba(255,199,44,0.2);border:1px solid rgba(255,199,44,0.5);
                         color:#FFC72C;padding:3px 12px;border-radius:12px;font-size:12px;font-weight:600;">
                ⏱ ~30 seconds
            </span>
            <span style="background:rgba(255,199,44,0.2);border:1px solid rgba(255,199,44,0.5);
                         color:#FFC72C;padding:3px 12px;border-radius:12px;font-size:12px;font-weight:600;">
                📊 5 Feature Walkthroughs
            </span>
            <span style="background:rgba(255,199,44,0.2);border:1px solid rgba(255,199,44,0.5);
                         color:#FFC72C;padding:3px 12px;border-radius:12px;font-size:12px;font-weight:600;">
                🎬 HD 1280×720
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── VIDEO PLAYER ──
    # Place your demo video file as: assets/enterprise_ai_demo.mp4
    # Then it will auto-play here. Instructions below if not yet uploaded.
    import os, base64

    video_path = "assets/enterprise_ai_demo.mp4"
    if os.path.exists(video_path):
        with open(video_path, "rb") as vf:
            video_bytes = vf.read()
        st.video(video_bytes)
    else:
        st.markdown("""
        <div style="background:#1a1a2e;border:2px dashed #B31B1B;border-radius:10px;
                    padding:40px;text-align:center;margin:20px 0;">
            <div style="font-size:48px;">🎬</div>
            <div style="color:#FFC72C;font-size:18px;font-weight:700;margin:12px 0;">
                Demo Video Not Found
            </div>
            <div style="color:#888;font-size:13px;line-height:1.7;">
                Place <code style="background:#111;padding:2px 6px;border-radius:4px;color:#29B6F6;">
                enterprise_ai_demo.mp4</code> inside an
                <code style="background:#111;padding:2px 6px;border-radius:4px;color:#29B6F6;">
                assets/</code> folder in your GitHub repo root.
            </div>
        </div>
        """, unsafe_allow_html=True)

    # ── FEATURE WALKTHROUGH CARDS ──
    st.markdown('<div class="section-title">📋 Whats Covered in the Demo</div>', unsafe_allow_html=True)

    features = [
        ("01", "⚡", "Real-time ETL Execution Flow",
         "Watch the Abinitio GDE-style pipeline animate through 3 stages: Reading → Transforming → Done. Record counts update live as data flows through each node."),
        ("02", "📂", "Multi-file Upload & Auto JOIN",
         "Upload one or multiple CSVs. The engine auto-detects joins, assigns aliases (df1, df2), and generates optimised pandas code via LLaMA 3.3 70B."),
        ("03", "📊", "Pipeline Summary & Metrics",
         "After execution: files processed, rows in/out, new columns created, joins applied — all in a clean metrics dashboard with plain-English step log."),
        ("04", "📋", "AI Jira Breakdown by Project Type",
         "Select your project type (ETL, Web, Mobile, API, Cloud, Security, AI/ML), configure your team, and get a full Epic + Stories + Gherkin AC + Subtasks in seconds."),
        ("05", "💾", "Export in 5 Formats",
         "ETL results as CSV or Excel (with audit log). Jira output as Excel (multi-sheet), TXT, or JSON — ready to import directly into Jira or any ticket system."),
    ]

    for step, icon, title, desc in features:
        st.markdown(f"""
        <div style="background:white;border:1px solid #E8E8E8;border-left:4px solid #B31B1B;
                    border-radius:8px;padding:16px 20px;margin:8px 0;
                    box-shadow:0 2px 8px rgba(0,0,0,0.06);">
            <div style="display:flex;align-items:center;gap:14px;">
                <div style="background:#B31B1B;color:white;border-radius:50%;
                            width:34px;height:34px;display:flex;align-items:center;
                            justify-content:center;font-size:13px;font-weight:700;
                            min-width:34px;">{step}</div>
                <div style="font-size:22px;">{icon}</div>
                <div>
                    <div style="font-size:14px;font-weight:700;color:#1a1a1a;">{title}</div>
                    <div style="font-size:12px;color:#555;margin-top:3px;line-height:1.5;">{desc}</div>
                </div>
            </div>
        </div>""", unsafe_allow_html=True)


# ============================================================
# HISTORY
# ============================================================
st.markdown("---")
st.markdown('<div class="section-title">ETL Transformation History</div>', unsafe_allow_html=True)
if st.session_state.history:
    st.dataframe(pd.DataFrame(st.session_state.history), use_container_width=True)
else:
    st.info("No transformations executed yet.")
