"""
Data Engineer Desk — app.py  (fully merged, single file)
=========================================================
Contains everything in one place:
  • AI Agent  (OpenAI GPT-5.2 + DuckDuckGo web search)
  • All 15 section renderers
  • Main Streamlit app + sidebar + navigation

Only external file needed: data.py  (all DE content / data arrays)

Run:
    streamlit run app.py

Install:
    pip install streamlit pandas graphviz openai duckduckgo-search
"""

# ─── Standard library ────────────────────────────────────────────
import json
import re

# ─── Third-party ─────────────────────────────────────────────────
import streamlit as st
import pandas as pd

# ─── Page config MUST be first Streamlit call ────────────────────
st.set_page_config(
    page_title="Data Engineer Desk",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Data (kept in separate file to keep this file readable) ─────
from data import (
    TOOLS, TOOL_CATEGORIES, PIPELINE_STAGES, ARCH_DATA,
    CLOUD_DATA, DESIGN_PATTERNS, PRINCIPLES, GLOSSARY,
    STREAM_STAGES, STREAM_CONCEPTS, COMPARISONS,
    REAL_WORLD, INTERVIEW_QA, IQ_CATEGORIES,
    ROLES, LEARN_PATH, ANTI_PATTERNS, CHECKLIST,
)


# ══════════════════════════════════════════════════════════════════
# SECTION 1 — AI AGENT  (OpenAI GPT-5.2 + DuckDuckGo search)
# ══════════════════════════════════════════════════════════════════

AGENT_SYSTEM_PROMPT = """You are DataBot — a senior data engineering expert assistant embedded in the
Data Engineer Desk reference application.

Your role:
1. Explain data engineering concepts clearly and in depth
2. You will be given web search results as context — use them to find real documentation links
3. Always provide real documentation/reference links from the search context provided
4. Summarize concepts in a structured, readable way

For EVERY response, return a JSON object with this EXACT structure (no extra text):
{
  "summary": "One clear sentence explaining what this is",
  "explanation": "Detailed explanation in 2-4 paragraphs. Use **bold** for key terms, bullet lists for multiple points",
  "key_concepts": ["concept 1", "concept 2", "concept 3"],
  "real_world_example": "A concrete real-world example or analogy that makes this tangible",
  "links": [
    {"title": "Official Docs Title", "url": "https://actual-url.com", "type": "Official Docs"},
    {"title": "Tutorial Title",      "url": "https://actual-url.com", "type": "Tutorial"},
    {"title": "Article Title",       "url": "https://actual-url.com", "type": "Article"}
  ],
  "related_topics": ["related topic 1", "related topic 2", "related topic 3"],
  "difficulty": "Beginner | Intermediate | Advanced",
  "follow_up_questions": ["Question to ask next?", "Another follow-up?"]
}

CRITICAL: Return ONLY valid JSON. No markdown fences. No text before or after.
Use REAL URLs from the search results. Never fabricate links."""

SUGGESTED_TOPICS = [
    "Apache Kafka architecture",
    "dbt incremental models",
    "Delta Lake vs Apache Iceberg",
    "Airflow DAG best practices",
    "Spark partitioning strategies",
    "CDC with Debezium",
    "Data Mesh architecture",
    "Snowflake virtual warehouses",
    "SCD Type 2 implementation",
    "Streaming with Apache Flink",
    "Data quality with Great Expectations",
    "ELT vs ETL patterns",
    "Medallion architecture",
    "Data lineage and observability",
    "Feature stores for ML",
]


class DEAgent:
    """OpenAI GPT-5.2 agent with DuckDuckGo web search."""

    def __init__(self, api_key: str):
        from openai import OpenAI
        self.client  = OpenAI(api_key=api_key)
        self.model   = "gpt-5.2"
        self.history: list[dict] = []
        self.history_display: list[dict] = []

    def ask(self, question: str) -> dict:
        self.history_display.append({"role": "user", "content": question})
        search_ctx = self._search(question)

        user_msg = (
            f"Question: {question}\n\n"
            f"Web search results — use these URLs in the links field:\n"
            f"{'='*60}\n{search_ctx}\n{'='*60}\n\n"
            f"Respond with a thorough explanation in the required JSON format."
        )

        messages = [{"role": "system", "content": AGENT_SYSTEM_PROMPT}]
        for turn in self.history[-6:]:
            messages.append({"role": turn["role"], "content": turn["content"]})
        messages.append({"role": "user", "content": user_msg})

        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            max_tokens=2048,
            temperature=0.3,
            response_format={"type": "json_object"},
        )
        result_text = response.choices[0].message.content
        self.history.append({"role": "user",      "content": question})
        self.history.append({"role": "assistant", "content": result_text})

        parsed = self._parse(result_text)
        self.history_display.append({"role": "assistant", "content": parsed})
        return parsed

    def clear_history(self):
        self.history         = []
        self.history_display = []

    def _search(self, query: str, n: int = 8) -> str:
        try:
            from duckduckgo_search import DDGS
            results = []
            with DDGS() as d:
                for r in d.text(f"{query} data engineering documentation", max_results=n):
                    results.append(f"Title: {r['title']}\nURL: {r['href']}\nSnippet: {r['body'][:250]}\n")
                for r in d.text(f"{query} official docs site:docs.* OR site:readthedocs.io OR site:github.com", max_results=4):
                    results.append(f"Title: {r['title']}\nURL: {r['href']}\nSnippet: {r['body'][:250]}\n")
            return "\n---\n".join(results) if results else "No results. Use training knowledge."
        except Exception as e:
            return f"Search unavailable ({e}). Use training knowledge for links."

    def _parse(self, text: str) -> dict:
        fallback = {
            "summary": "Response received", "explanation": text,
            "key_concepts": [], "real_world_example": "",
            "links": [], "related_topics": [],
            "difficulty": "Intermediate", "follow_up_questions": [],
        }
        if not text or not text.strip():
            return fallback
        try:
            return json.loads(text.strip())
        except json.JSONDecodeError:
            pass
        m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                pass
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass
        fallback["explanation"] = text
        return fallback


# ══════════════════════════════════════════════════════════════════
# SECTION 2 — SHARED COMPONENT HELPERS
# ══════════════════════════════════════════════════════════════════

def _tags_html(items: list, color: str = "#00d4b4") -> str:
    return " ".join(
        f'<span style="background:{color}15;color:{color};border:1px solid {color}40;'
        f'border-radius:4px;padding:2px 8px;font-size:11px;font-family:monospace;'
        f'margin:2px;display:inline-block">{i}</span>'
        for i in items
    )

def _info(text: str):
    st.markdown(
        f'<div style="background:rgba(0,212,180,0.07);border:1px solid rgba(0,212,180,0.25);'
        f'border-radius:8px;padding:10px 13px;font-size:12px;color:#7fd6c6;line-height:1.6;margin:8px 0">'
        f'💡 {text}</div>', unsafe_allow_html=True)

def _warn(text: str):
    st.markdown(
        f'<div style="background:rgba(245,166,35,0.08);border:1px solid rgba(245,166,35,0.3);'
        f'border-radius:8px;padding:10px 13px;font-size:12px;color:#c8a05a;line-height:1.6;margin:8px 0">'
        f'⚠️ {text}</div>', unsafe_allow_html=True)

def _sh(text: str):   # section header
    st.markdown(
        f'<div style="font-size:10px;font-weight:600;color:#00d4b4;text-transform:uppercase;'
        f'letter-spacing:2px;font-family:monospace;margin-bottom:12px;margin-top:4px">// {text}</div>',
        unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════
# SECTION 3 — SECTION RENDERERS  (all 15 tabs)
# ══════════════════════════════════════════════════════════════════

# ── 1. Overview ──────────────────────────────────────────────────
def render_overview():
    _sh("what does a data engineer do?")
    cards = [
        ("🔁","Build Pipelines",       "Design & automate data flows — ingestion, transformation and delivery at scale.","#00d4b4","ETL Pipeline"),
        ("🛠","Pick the Right Tools",  "Choose ingestion, processing, orchestration and storage based on volume & cost.", "#4a9eff","Tools & Stack"),
        ("🏗","Design Architecture",   "Lambda, Kappa, Lakehouse, Data Mesh — pick the right pattern for your org.",    "#9b59f5","Architecture"),
        ("☁️","Deploy on Cloud",       "AWS, GCP, Azure — every managed service mapped to when and why you'd use it.",  "#f5a623","Cloud & Services"),
        ("⚡","Streaming & Real-time", "Kafka, Flink, exactly-once semantics, windowing — build sub-second pipelines.", "#00d4b4","Streaming & Real-time"),
        ("📐","Apply Principles",      "Idempotency, observability, schema evolution — non-negotiables in prod DE.",    "#2ecc71","Engineering Principles"),
        ("🧩","Design Patterns",       "CDC, Medallion Architecture, SCD, Event Sourcing — patterns every DE must know.","#e056a0","Design Patterns"),
        ("⚠️","Avoid Anti-patterns",   "God pipelines, no raw layer, polling OLTP, non-idempotent loads — avoid these.", "#ff6b6b","Anti-patterns"),
        ("🚀","Career Roadmap",        "Junior → Mid → Senior → Staff — skills, techs, and what to learn at each level.","#f5a623","Career Roadmap"),
        ("🎯","Interview Prep",        "10 real interview questions with detailed answers and code examples.",            "#4a9eff","Interview Q&A"),
    ]
    cols = st.columns(3)
    for i, (icon, title, desc, color, dest) in enumerate(cards):
        with cols[i % 3]:
            st.markdown(
                f'<div style="background:rgba(22,34,54,0.6);border:1px solid {color}30;'
                f'border-top:2px solid {color};border-radius:10px;padding:14px;margin-bottom:10px;min-height:130px">'
                f'<div style="font-size:20px;margin-bottom:6px">{icon}</div>'
                f'<div style="font-size:13px;font-weight:600;color:#c8ddf0;margin-bottom:4px">{title}</div>'
                f'<div style="font-size:11px;color:#8899aa;line-height:1.5;margin-bottom:8px">{desc}</div>'
                f'<span style="background:{color}15;color:{color};border:1px solid {color}40;'
                f'border-radius:4px;padding:2px 8px;font-size:10px;font-family:monospace">→ {dest}</span></div>',
                unsafe_allow_html=True)
    _info("<strong>Mental model:</strong> Sources = faucets · Pipelines = pipes · Warehouse/Lake = tank · Dashboards/ML = taps. Your job: keep water flowing clean, fast, tested and without leaks — 24/7.")

# ── 2. Tools & Stack ─────────────────────────────────────────────
def render_tools():
    _sh("tools & technology stack")
    cat_filter = st.radio("Filter", TOOL_CATEGORIES, horizontal=True)
    items = TOOLS if cat_filter == "All" else [t for t in TOOLS if t["cat"] == cat_filter]
    CAT_COLORS = {"streaming":"#f5a623","processing":"#ff6b6b","storage":"#4a9eff",
                  "orchestration":"#9b59f5","transformation":"#2ecc71","ingestion":"#ff6b6b",
                  "quality":"#2ecc71","catalog":"#e056a0"}
    cols = st.columns(3)
    for i, tool in enumerate(items):
        color = CAT_COLORS.get(tool["cat"], "#00d4b4")
        with cols[i % 3]:
            with st.container(border=True):
                st.markdown(
                    f'<div style="font-size:13px;font-weight:600;color:#c8ddf0;margin-bottom:2px">'
                    f'<span style="display:inline-block;width:7px;height:7px;border-radius:50%;'
                    f'background:{color};margin-right:6px;vertical-align:middle"></span>{tool["name"]}</div>'
                    f'<div style="font-size:10px;color:{color};font-family:monospace;margin-bottom:6px">{tool["cat"]}</div>'
                    f'<div style="font-size:11px;color:#8899aa;line-height:1.45">{tool["desc"]}</div>',
                    unsafe_allow_html=True)

# ── 3. ETL Pipeline ──────────────────────────────────────────────
def render_pipeline():
    _sh("end-to-end etl/elt pipeline — select a stage")
    labels = [f"{s['icon']} {s['label']}" for s in PIPELINE_STAGES]
    sel    = st.radio("Stage", labels, horizontal=True, label_visibility="collapsed")
    stage  = PIPELINE_STAGES[labels.index(sel)]
    st.markdown(
        f'<div style="background:rgba(22,34,54,0.6);border:1px solid rgba(0,212,180,0.2);'
        f'border-radius:10px;padding:16px;margin-bottom:12px">'
        f'<div style="font-size:15px;font-weight:600;color:#c8ddf0;margin-bottom:6px">'
        f'{stage["icon"]} {stage["title"]}</div>'
        f'<div style="font-size:12px;color:#8899aa;line-height:1.65">{stage["desc"]}</div></div>',
        unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        st.markdown('<div style="font-size:10px;color:#556677;text-transform:uppercase;letter-spacing:1.5px;font-family:monospace;margin-bottom:6px">Common tools</div>', unsafe_allow_html=True)
        st.markdown(_tags_html(stage["tools"]), unsafe_allow_html=True)
        st.markdown('<div style="font-size:10px;color:#556677;text-transform:uppercase;letter-spacing:1.5px;font-family:monospace;margin:10px 0 6px">Key concepts</div>', unsafe_allow_html=True)
        st.markdown(_tags_html(stage["concepts"], "#9b59f5"), unsafe_allow_html=True)
    with c2:
        _warn("<strong>Common pitfalls:</strong><br>" + "<br>".join(f"• {p}" for p in stage["pitfalls"]))
    st.markdown('<div style="font-size:10px;color:#556677;text-transform:uppercase;letter-spacing:1.5px;font-family:monospace;margin:12px 0 6px">Production code snippet</div>', unsafe_allow_html=True)
    st.code(stage["code"], language=stage["lang"])

# ── 4. Architecture ──────────────────────────────────────────────
def render_architecture():
    _sh("architecture patterns")
    sel  = st.radio("Pattern", list(ARCH_DATA.keys()), horizontal=True, label_visibility="collapsed")
    arch = ARCH_DATA[sel]
    st.markdown(f'<div style="font-size:12px;color:#8899aa;margin-bottom:12px">{arch["desc"]}</div>', unsafe_allow_html=True)
    try:
        st.graphviz_chart(arch["dot"], use_container_width=True)
    except Exception:
        st.info("Install graphviz: pip install graphviz")
    c1, c2 = st.columns(2)
    with c1:
        with st.container(border=True):
            st.markdown('<div style="font-size:10px;color:#556677;text-transform:uppercase;letter-spacing:1.5px;font-family:monospace;margin-bottom:6px">When to use</div>', unsafe_allow_html=True)
            st.markdown(f'<div style="font-size:12px;color:#8899aa;line-height:1.6">{arch["when"]}</div>', unsafe_allow_html=True)
        with st.container(border=True):
            st.markdown('<div style="font-size:10px;color:#556677;text-transform:uppercase;letter-spacing:1.5px;font-family:monospace;margin-bottom:6px">Real-world usage</div>', unsafe_allow_html=True)
            st.markdown(f'<div style="font-size:12px;color:#8899aa;line-height:1.6">{arch["usage"]}</div>', unsafe_allow_html=True)
    with c2:
        with st.container(border=True):
            st.markdown('<div style="font-size:10px;color:#556677;text-transform:uppercase;letter-spacing:1.5px;font-family:monospace;margin-bottom:6px">Trade-offs</div>', unsafe_allow_html=True)
            for t in arch["tradeoffs"]:
                st.markdown(f'<div style="font-size:12px;color:#8899aa;padding:2px 0">• {t}</div>', unsafe_allow_html=True)
        with st.container(border=True):
            st.markdown('<div style="font-size:10px;color:#556677;text-transform:uppercase;letter-spacing:1.5px;font-family:monospace;margin-bottom:6px">Key tools</div>', unsafe_allow_html=True)
            st.markdown(f'<div style="font-size:12px;color:#8899aa">{arch["tools"]}</div>', unsafe_allow_html=True)

# ── 5. Cloud & Services ──────────────────────────────────────────
def render_cloud():
    _sh("cloud providers & managed services")
    providers = [p["name"] for p in CLOUD_DATA]
    sel = st.radio("Provider", providers, horizontal=True, label_visibility="collapsed")
    provider = next(p for p in CLOUD_DATA if p["name"] == sel)
    PCOLS = {"AWS":"#f5a623","GCP":"#4a9eff","Azure":"#9b59f5"}
    color = PCOLS.get(sel, "#00d4b4")
    for svc in provider["services"]:
        with st.expander(f"**{svc['name']}** `{svc['cat']}` — {svc['use']}"):
            st.markdown(
                f'<div style="font-size:10px;color:{color};font-family:monospace;margin-bottom:4px">when to use →</div>'
                f'<div style="font-size:12px;color:#8899aa;line-height:1.6">{svc["when"]}</div>',
                unsafe_allow_html=True)

# ── 6. Design Patterns ───────────────────────────────────────────
def render_patterns():
    _sh("data engineering design patterns")
    for pat in DESIGN_PATTERNS:
        with st.expander(f"**{pat['name']}** — _{pat['sub']}_"):
            st.markdown(f'<div style="font-size:12px;color:#8899aa;line-height:1.65;margin-bottom:10px">{pat["detail"]}</div>', unsafe_allow_html=True)
            c1, c2 = st.columns(2)
            with c1:
                st.markdown('<div style="font-size:10px;color:#556677;text-transform:uppercase;letter-spacing:1.5px;font-family:monospace;margin-bottom:6px">When to use</div>', unsafe_allow_html=True)
                st.markdown(f'<div style="font-size:11px;color:#8899aa;margin-bottom:8px">{pat["when"]}</div>', unsafe_allow_html=True)
                st.markdown(_tags_html(pat["tools"]), unsafe_allow_html=True)
            with c2:
                _warn(f"<strong>Pitfall →</strong> {pat['pitfall']}")

# ── 7. Engineering Principles ────────────────────────────────────
def render_principles():
    _sh("engineering principles")
    PCOLS = {"Idempotency":"#00d4b4","Observability":"#4a9eff","Schema Evolution":"#f5a623",
             "Data Quality":"#2ecc71","Backfill Strategy":"#9b59f5","Data Security & Compliance":"#ff6b6b"}
    for p in PRINCIPLES:
        color = PCOLS.get(p["name"], "#00d4b4")
        with st.expander(f"**{p['name']}** — _{p['tagline']}_"):
            st.markdown(f'<div style="font-size:12px;color:#8899aa;line-height:1.65;margin-bottom:10px">{p["desc"]}</div>', unsafe_allow_html=True)
            c1, c2 = st.columns(2)
            with c1:
                st.markdown('<div style="font-size:10px;color:#2ecc71;font-family:monospace;font-weight:600;margin-bottom:4px">✓ DO</div>', unsafe_allow_html=True)
                for d in p["dos"]:
                    st.markdown(f'<div style="font-size:11px;color:#8899aa;padding:2px 0 2px 10px">• {d}</div>', unsafe_allow_html=True)
            with c2:
                st.markdown('<div style="font-size:10px;color:#e74c3c;font-family:monospace;font-weight:600;margin-bottom:4px">✗ DON\'T</div>', unsafe_allow_html=True)
                for d in p["donts"]:
                    st.markdown(f'<div style="font-size:11px;color:#8899aa;padding:2px 0 2px 10px">• {d}</div>', unsafe_allow_html=True)
            st.code(p["example"], language=p["example_lang"])

# ── 8. Streaming & Real-time ─────────────────────────────────────
def render_streaming():
    _sh("streaming & real-time data engineering")
    labels = [f"{s['icon']} {s['label']}" for s in STREAM_STAGES]
    sel   = st.radio("Stage", labels, horizontal=True, label_visibility="collapsed")
    stage = STREAM_STAGES[labels.index(sel)]
    with st.container(border=True):
        st.markdown(f'<div style="font-size:14px;font-weight:600;color:#c8ddf0;margin-bottom:6px">{stage["icon"]} {stage["title"]}</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="font-size:12px;color:#8899aa;line-height:1.65;margin-bottom:10px">{stage["desc"]}</div>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div style="font-size:10px;color:#556677;text-transform:uppercase;letter-spacing:1.5px;font-family:monospace;margin-bottom:6px">Tools</div>', unsafe_allow_html=True)
            st.markdown(_tags_html(stage["tools"]), unsafe_allow_html=True)
        with c2:
            st.markdown('<div style="font-size:10px;color:#556677;text-transform:uppercase;letter-spacing:1.5px;font-family:monospace;margin-bottom:6px">Key concepts</div>', unsafe_allow_html=True)
            st.markdown(_tags_html(stage["concepts"], "#9b59f5"), unsafe_allow_html=True)
        st.code(stage["code"], language="python")
    st.markdown('<div style="font-size:10px;font-weight:600;color:#00d4b4;text-transform:uppercase;letter-spacing:2px;font-family:monospace;margin:14px 0 8px">// key streaming concepts</div>', unsafe_allow_html=True)
    for c in STREAM_CONCEPTS:
        with st.expander(f"**{c['name']}**"):
            st.markdown(f'<div style="font-size:12px;color:#8899aa;line-height:1.65;margin-bottom:8px">{c["desc"]}</div>', unsafe_allow_html=True)
            st.code(c["code"], language="sql" if "TUMBLE" in c["code"] else "python")

# ── 9. Tool Comparisons ──────────────────────────────────────────
def render_comparisons():
    _sh("tool comparisons — make the right choice")
    sel = st.radio("Compare", list(COMPARISONS.keys()), horizontal=True, label_visibility="collapsed")
    cmp = COMPARISONS[sel]
    st.dataframe(pd.DataFrame(cmp["rows"], columns=cmp["columns"]), use_container_width=True, hide_index=True)

# ── 10. Real-world Case Studies ──────────────────────────────────
def render_realworld():
    _sh("real-world case studies")
    CCOLS = {"Uber":"#f5a623","Airbnb":"#ff6b6b","LinkedIn":"#4a9eff",
             "Netflix":"#2ecc71","Stripe":"#9b59f5","DoorDash":"#00d4b4"}
    cols = st.columns(2)
    for i, case in enumerate(REAL_WORLD):
        color = CCOLS.get(case["company"], "#00d4b4")
        with cols[i % 2]:
            st.markdown(
                f'<div style="background:rgba(22,34,54,0.6);border:1px solid {color}25;border-radius:10px;overflow:hidden;margin-bottom:10px">'
                f'<div style="padding:12px 14px;border-bottom:1px solid rgba(0,212,180,0.1)">'
                f'<div style="font-size:14px;font-weight:700;color:{color}">{case["company"]}</div>'
                f'<div style="font-size:10px;color:#8899aa;font-family:monospace">{case["sector"]}</div></div>'
                f'<div style="padding:12px 14px">'
                f'<div style="font-size:10px;color:#556677;font-family:monospace;margin-bottom:5px">stack: {case["stack"]}</div>'
                f'<div style="font-size:11px;color:#8899aa;line-height:1.6;margin-bottom:6px">{case["desc"]}</div>'
                f'<div style="font-size:10px;color:{color};font-family:monospace">key takeaway → {case["learn"]}</div>'
                f'</div></div>', unsafe_allow_html=True)
    _info("Almost every company started with a monolithic DB, moved to a basic warehouse, hit scale pain, adopted Kafka, then settled on a Lakehouse. The tools change; the journey is consistent.")

# ── 11. Interview Q&A ────────────────────────────────────────────
def render_interview():
    _sh("data engineering interview questions & answers")
    cf = st.radio("Category", IQ_CATEGORIES, horizontal=True, label_visibility="collapsed")
    items = INTERVIEW_QA if cf == "All" else [q for q in INTERVIEW_QA if q["cat"] == cf]
    CCOLS = {"Fundamentals":"#4a9eff","Architecture":"#00d4b4","SQL & Modeling":"#2ecc71","Streaming":"#9b59f5","System Design":"#ff6b6b"}
    for qa in items:
        color = CCOLS.get(qa["cat"], "#4a9eff")
        with st.expander(f"**{qa['q']}** `{qa['cat']}`"):
            st.markdown(f'<div style="font-size:12px;color:#8899aa;line-height:1.65;margin-bottom:10px">{qa["ans"]}</div>', unsafe_allow_html=True)
            st.code(qa["code"], language="sql" if qa["code"].strip().startswith(("SELECT","INSERT","MERGE","CREATE","--")) else "python")

# ── 12. Career Roadmap ───────────────────────────────────────────
def render_career():
    _sh("data engineering career roadmap")
    names = [r["name"] for r in ROLES]
    sel   = st.radio("Level", names, horizontal=True, label_visibility="collapsed")
    role  = next(r for r in ROLES if r["name"] == sel)
    RCOLS = {"Junior DE":"#4a9eff","Mid-level DE":"#00d4b4","Senior DE":"#9b59f5","Staff / Principal DE":"#f5a623"}
    color = RCOLS.get(sel, "#00d4b4")
    c1, c2 = st.columns([2, 1])
    with c1:
        st.markdown(f'<div style="font-size:13px;font-weight:600;color:{color};margin-bottom:2px">{role["name"]} <span style="color:#556677;font-size:11px">{role["level"]}</span></div>', unsafe_allow_html=True)
        st.markdown(f'<div style="font-size:12px;color:#8899aa;margin-bottom:12px">{role["desc"]}</div>', unsafe_allow_html=True)
        for s in role["skills"]:
            st.markdown(f'<div style="font-size:11px;color:#8899aa;margin-bottom:2px">{s["name"]}</div>', unsafe_allow_html=True)
            st.progress(s["pct"] / 100)
    with c2:
        with st.container(border=True):
            for tech in role["techs"]:
                st.markdown(f'<div style="font-size:11px;color:#8899aa;padding:2px 0">• {tech}</div>', unsafe_allow_html=True)
        _warn(f"<strong>What to learn next →</strong> {role['next']}")
    st.markdown('<hr style="border:none;border-top:1px solid rgba(0,212,180,0.1);margin:14px 0">', unsafe_allow_html=True)
    _sh("learning path — ordered by priority")
    for step in LEARN_PATH:
        with st.expander(f"**{step['step']}.** {step['title']}"):
            for item in step["items"]:
                st.markdown(f'<div style="font-size:12px;color:#8899aa;padding:4px 0 4px 12px;border-bottom:1px solid rgba(0,212,180,0.08)">› {item}</div>', unsafe_allow_html=True)

# ── 13. Anti-patterns ────────────────────────────────────────────
def render_antipatterns():
    _sh("common data engineering anti-patterns — what NOT to do")
    APCOLS = {"The God Pipeline":"#ff6b6b","No Raw Layer (Transform-on-Ingest)":"#f5a623",
              "Polling OLTP in Production":"#ff6b6b","Non-Idempotent Pipelines":"#9b59f5",
              "Ignoring Small Files Problem":"#00d4b4","No Monitoring or Alerting":"#f5a623"}
    for ap in ANTI_PATTERNS:
        color = APCOLS.get(ap["name"], "#ff6b6b")
        with st.expander(f"**{ap['name']}** — _{ap['tagline']}_"):
            st.markdown(f'<div style="font-size:12px;color:#8899aa;line-height:1.65;margin-bottom:10px">{ap["desc"]}</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div style="background:rgba(46,204,113,0.07);border:1px solid rgba(46,204,113,0.25);'
                f'border-radius:7px;padding:9px 12px;font-size:12px;color:#7fbf7f;margin-bottom:10px">'
                f'<strong style="font-size:10px;font-family:monospace;color:#2ecc71">✓ THE FIX</strong><br>{ap["fix"]}</div>',
                unsafe_allow_html=True)
            st.code(ap["code"], language="sql" if ap["code"].strip().startswith(("--","MERGE","INSERT")) else "python")

# ── 14. Pipeline Checklist ───────────────────────────────────────
def render_checklist():
    _sh("production pipeline readiness checklist")
    SCOLS = {"Ingestion":"#4a9eff","Transformation":"#9b59f5","Orchestration":"#f5a623",
             "Data Quality":"#2ecc71","Security & Compliance":"#ff6b6b","Performance & Cost":"#00d4b4"}
    total   = sum(len(s["items"]) for s in CHECKLIST)
    checked = sum(1 for s in CHECKLIST for i in range(len(s["items"])) if st.session_state.get(f"chk_{s['section']}_{i}", False))
    pct = int(checked / total * 100) if total else 0
    c1, c2, c3 = st.columns([3,1,1])
    with c1: st.progress(pct / 100)
    with c2: st.metric("Progress", f"{checked} / {total}")
    with c3: st.metric("Score",    f"{pct}%")
    if pct == 100: st.success("Production-grade. Ship it! 🚀")
    elif pct >= 75: st.warning("Almost there — review remaining items.")
    elif pct >= 40: st.info("Good progress. Address remaining items before going live.")
    else:           st.error("Start checking — every item makes the pipeline more resilient.")
    st.markdown('<hr style="border:none;border-top:1px solid rgba(0,212,180,0.1);margin:12px 0">', unsafe_allow_html=True)
    for section in CHECKLIST:
        color = SCOLS.get(section["section"], "#00d4b4")
        st.markdown(f'<div style="font-size:11px;font-weight:600;color:{color};font-family:monospace;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:8px">{section["section"]}</div>', unsafe_allow_html=True)
        for i, item in enumerate(section["items"]):
            st.checkbox(item, key=f"chk_{section['section']}_{i}")

# ── 15. Glossary ─────────────────────────────────────────────────
def render_glossary():
    _sh("data engineering glossary")
    search = st.text_input("Search terms...", placeholder="e.g. CDC, SCD, Watermark")
    items  = GLOSSARY if not search else [g for g in GLOSSARY if search.lower() in g["term"].lower() or search.lower() in g["def"].lower()]
    if not items:
        st.info(f"No results for '{search}'. Try a different term.")
        return
    for g in items:
        with st.expander(f"**{g['term']}** — {g['short']}"):
            st.markdown(f'<div style="font-size:12px;color:#8899aa;line-height:1.65;margin-bottom:10px">{g["def"]}</div>', unsafe_allow_html=True)
            st.code(g["example"], language="sql" if g["example"].strip().startswith(("SELECT","MERGE","CREATE","--")) else "python")
            st.markdown(_tags_html(g["refs"], "#4a9eff"), unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════
# SECTION 4 — AGENT RESPONSE RENDERER
# ══════════════════════════════════════════════════════════════════

def _render_agent_response(result: dict):
    """Render a structured agent response with 4 tabs."""
    if not isinstance(result, dict):
        st.markdown(
            f'<div style="background:rgba(22,34,54,0.7);border:1px solid rgba(0,212,180,0.2);'
            f'border-radius:12px 12px 12px 2px;padding:10px 14px;font-size:13px;color:#c8ddf0">{result}</div>',
            unsafe_allow_html=True)
        return

    DCOLS = {"Beginner":("#2ecc71","rgba(46,204,113,0.15)"),
             "Intermediate":("#f5a623","rgba(245,166,35,0.15)"),
             "Advanced":("#ff6b6b","rgba(255,107,107,0.15)")}
    diff      = result.get("difficulty", "Intermediate")
    dcol, dbg = DCOLS.get(diff, ("#8899aa","rgba(136,153,170,0.15)"))

    st.markdown('<div style="font-size:10px;font-family:monospace;color:#00d4b4;margin-bottom:4px">⬡ DataBot (GPT-5.2)</div>', unsafe_allow_html=True)
    st.markdown(
        f'<div style="background:rgba(0,212,180,0.08);border:1px solid rgba(0,212,180,0.25);'
        f'border-radius:8px;padding:10px 14px;margin-bottom:10px">'
        f'<span style="background:{dbg};color:{dcol};border:1px solid {dcol}40;font-size:10px;'
        f'font-family:monospace;padding:2px 8px;border-radius:4px;margin-bottom:6px;display:inline-block">{diff}</span>'
        f'<div style="font-size:13px;font-weight:600;color:#c8ddf0">{result.get("summary","")}</div></div>',
        unsafe_allow_html=True)

    tabs = st.tabs(["📖 Explanation", "🔗 Docs & Links", "🧠 Key Concepts", "❓ Follow-up"])

    with tabs[0]:
        st.markdown(f'<div style="font-size:12px;color:#c8ddf0;line-height:1.75">{result.get("explanation","")}</div>', unsafe_allow_html=True)
        if ex := result.get("real_world_example",""):
            st.markdown(
                f'<div style="background:rgba(155,89,245,0.08);border:1px solid rgba(155,89,245,0.25);'
                f'border-radius:8px;padding:10px 13px;margin-top:10px">'
                f'<div style="font-size:10px;color:#9b59f5;font-family:monospace;margin-bottom:4px">REAL-WORLD EXAMPLE</div>'
                f'<div style="font-size:12px;color:#c8ddf0;line-height:1.6">{ex}</div></div>',
                unsafe_allow_html=True)

    with tabs[1]:
        TYPE_COLS = {"Official Docs":"#00d4b4","Tutorial":"#4a9eff","Article":"#9b59f5","GitHub":"#f5a623","Guide":"#2ecc71"}
        links = result.get("links", [])
        if links:
            for link in links:
                url   = link.get("url","#");  title = link.get("title",url)
                ltype = link.get("type","Resource"); lc = TYPE_COLS.get(ltype,"#8899aa")
                st.markdown(
                    f'<div style="background:rgba(22,34,54,0.8);border:1px solid rgba(0,212,180,0.15);'
                    f'border-radius:8px;padding:10px 12px;margin-bottom:6px">'
                    f'<span style="background:{lc}15;color:{lc};border:1px solid {lc}30;font-size:10px;'
                    f'font-family:monospace;padding:1px 7px;border-radius:3px;margin-right:6px">{ltype}</span>'
                    f'<a href="{url}" target="_blank" style="color:#c8ddf0;font-size:13px;font-weight:500;text-decoration:none">{title}</a>'
                    f'<div style="font-size:11px;color:#556677;font-family:monospace;margin-top:3px;'
                    f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{url}</div></div>',
                    unsafe_allow_html=True)
        else:
            st.caption("No documentation links returned.")

    with tabs[2]:
        kcs = result.get("key_concepts",[])
        if kcs:
            st.markdown('<div style="font-size:10px;color:#556677;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px">Key concepts</div>', unsafe_allow_html=True)
            cols = st.columns(2)
            for i, kc in enumerate(kcs):
                with cols[i%2]:
                    st.markdown(f'<div style="background:rgba(0,212,180,0.08);border:1px solid rgba(0,212,180,0.2);border-radius:6px;padding:6px 10px;font-size:12px;color:#c8ddf0;margin-bottom:5px">• {kc}</div>', unsafe_allow_html=True)
        related = result.get("related_topics",[])
        if related:
            st.markdown('<div style="font-size:10px;color:#556677;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin:10px 0 6px">Related topics</div>', unsafe_allow_html=True)
            rc = st.columns(2)
            for i, rt in enumerate(related):
                with rc[i%2]:
                    if st.button(f"→ {rt}", key=f"rel_{rt}_{id(result)}", use_container_width=True):
                        st.session_state.pending_query = rt

    with tabs[3]:
        fqs = result.get("follow_up_questions",[])
        if fqs:
            st.markdown('<div style="font-size:11px;color:#8899aa;margin-bottom:8px">Click to ask DataBot:</div>', unsafe_allow_html=True)
            for fq in fqs:
                if st.button(fq, key=f"fq_{fq}_{id(result)}", use_container_width=True):
                    st.session_state.pending_query = fq
        else:
            st.caption("No follow-up questions available.")

    st.markdown('<hr style="border:none;border-top:1px solid rgba(0,212,180,0.08);margin:8px 0">', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════
# SECTION 5 — LANDING PAGE (Overview + AI Agent)
# ══════════════════════════════════════════════════════════════════

def render_landing():
    st.markdown("""
    <div style="margin-bottom:20px">
        <div style="font-size:26px;font-weight:700;color:#c8ddf0;margin-bottom:4px">⬡ Data Engineer Desk</div>
        <div style="font-size:14px;color:#8899aa">Complete reference + GPT-5.2 AI agent that explains any concept, finds docs and provides real links</div>
    </div>
    """, unsafe_allow_html=True)

    agent_col, nav_col = st.columns([3, 2], gap="large")

    # ── Left: AI Agent ──
    with agent_col:
        ok  = st.session_state.agent is not None
        dot = "#00d4b4" if ok else "#ff6b6b"
        txt = "ready · GPT-5.2" if ok else "no API key"
        st.markdown(
            f'<div style="background:rgba(22,34,54,0.8);border:1px solid rgba(0,212,180,0.25);'
            f'border-radius:10px 10px 0 0;padding:11px 15px;display:flex;align-items:center;gap:10px">'
            f'<div style="width:8px;height:8px;border-radius:50%;background:{dot}"></div>'
            f'<div style="font-size:14px;font-weight:600;color:#c8ddf0;flex:1">DataBot — AI Agent</div>'
            f'<div style="font-size:10px;color:#8899aa;font-family:monospace">{txt}</div></div>'
            f'<div style="background:rgba(22,34,54,0.5);border:1px solid rgba(0,212,180,0.15);'
            f'border-top:none;border-radius:0 0 10px 10px;padding:14px;margin-bottom:12px">',
            unsafe_allow_html=True)

        if not ok:
            st.markdown("""
            <div style="text-align:center;padding:24px 0">
                <div style="font-size:36px;margin-bottom:10px">🤖</div>
                <div style="font-size:14px;font-weight:600;color:#c8ddf0;margin-bottom:6px">DataBot needs your OpenAI API key</div>
                <div style="font-size:12px;color:#8899aa;line-height:1.8">
                    1. Go to <strong>platform.openai.com/api-keys</strong><br>
                    2. Create a new secret key<br>
                    3. Paste it in the <strong>sidebar → API Key</strong> field<br>
                    4. The green dot appears → DataBot is live
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            if not st.session_state.chat_history:
                st.markdown("""
                <div style="background:rgba(0,212,180,0.05);border:1px solid rgba(0,212,180,0.15);border-radius:10px;padding:13px;margin-bottom:10px">
                    <div style="font-size:10px;font-family:monospace;color:#00d4b4;margin-bottom:5px">⬡ DataBot (GPT-5.2)</div>
                    <div style="font-size:13px;color:#c8ddf0;line-height:1.7">
                        Hi! Ask me about any data engineering concept — Kafka, Spark, dbt, Airflow,
                        Delta Lake, streaming patterns, or anything else.<br><br>
                        I'll explain it in depth, search the web for the latest information, and give you
                        real links to official documentation.
                    </div>
                </div>
                """, unsafe_allow_html=True)

            for msg in st.session_state.chat_history:
                if msg["role"] == "user":
                    st.markdown(
                        f'<div style="text-align:right;margin-bottom:2px"><span style="font-size:10px;font-family:monospace;color:#4a9eff">You</span></div>'
                        f'<div style="background:rgba(74,158,255,0.12);border:1px solid rgba(74,158,255,0.3);'
                        f'border-radius:12px 12px 2px 12px;padding:10px 14px;font-size:13px;color:#c8ddf0;'
                        f'max-width:85%;margin-left:auto;margin-bottom:10px">{msg["content"]}</div>',
                        unsafe_allow_html=True)
                else:
                    _render_agent_response(msg["content"])

            # Suggested topic buttons
            st.markdown('<div style="font-size:10px;color:#556677;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin:10px 0 6px">Suggested topics</div>', unsafe_allow_html=True)
            tc = st.columns(2)
            for i, topic in enumerate(SUGGESTED_TOPICS[:8]):
                with tc[i % 2]:
                    if st.button(topic, key=f"st_{i}", use_container_width=True):
                        st.session_state.pending_query = topic

            # Input form
            st.markdown("<div style='margin-top:8px'></div>", unsafe_allow_html=True)
            with st.form("agent_form", clear_on_submit=True):
                user_input = st.text_area(
                    "Ask DataBot",
                    placeholder="e.g. 'Explain Kafka consumer groups' or 'How does Z-ordering work in Delta Lake?'",
                    height=80, label_visibility="collapsed",
                )
                c1, c2 = st.columns([3, 1])
                with c1: submitted = st.form_submit_button("Ask DataBot ↗", use_container_width=True)
                with c2: cleared   = st.form_submit_button("Clear chat",    use_container_width=True)

            if cleared:
                st.session_state.chat_history = []
                if st.session_state.agent:
                    st.session_state.agent.clear_history()
                st.rerun()
            if submitted and user_input.strip():
                st.session_state.pending_query = user_input.strip()

        st.markdown("</div>", unsafe_allow_html=True)

    # ── Right: Quick nav ──
    with nav_col:
        st.markdown('<div style="font-size:10px;color:#556677;font-family:monospace;text-transform:uppercase;letter-spacing:2px;margin-bottom:10px">// desk sections</div>', unsafe_allow_html=True)
        NAV = [
            ("🛠","Tools & Stack",         "28 tools across all DE categories",           "#4a9eff"),
            ("🔁","ETL Pipeline",          "7-stage pipeline with code + pitfalls",       "#00d4b4"),
            ("🏗","Architecture",          "Lambda, Kappa, Lakehouse, Data Mesh",         "#9b59f5"),
            ("☁️","Cloud & Services",      "AWS · GCP · Azure — when to use each",       "#f5a623"),
            ("🧩","Design Patterns",       "8 patterns: Medallion, CDC, SCD & more",      "#e056a0"),
            ("📐","Engineering Principles","Idempotency, observability, security",        "#2ecc71"),
            ("⚡","Streaming & Real-time", "Kafka, Flink, windowing, exactly-once",       "#00d4b4"),
            ("📊","Tool Comparisons",      "Side-by-side: warehouses, orchestrators",     "#4a9eff"),
            ("🏢","Real-world Cases",      "Uber, Airbnb, Netflix, Stripe stacks",        "#f5a623"),
            ("🎯","Interview Q&A",         "10 real questions + code answers",            "#ff6b6b"),
            ("🚀","Career Roadmap",        "Junior → Staff skill levels + learning path", "#9b59f5"),
            ("✅","Pipeline Checklist",    "36-item production readiness checklist",      "#2ecc71"),
        ]
        for icon, title, desc, color in NAV:
            st.markdown(
                f'<div style="background:rgba(22,34,54,0.5);border:1px solid {color}20;'
                f'border-left:2px solid {color};border-radius:7px;padding:8px 11px;margin-bottom:5px">'
                f'<div style="font-size:12px;font-weight:600;color:#c8ddf0;margin-bottom:1px">{icon} {title}</div>'
                f'<div style="font-size:11px;color:#8899aa">{desc}</div></div>',
                unsafe_allow_html=True)

    # Process pending query
    if st.session_state.pending_query and st.session_state.agent:
        query = st.session_state.pending_query
        st.session_state.pending_query = None
        st.session_state.chat_history.append({"role": "user", "content": query})
        with st.spinner("🔍 DataBot is searching and thinking..."):
            result = st.session_state.agent.ask(query)
        st.session_state.chat_history.append({"role": "assistant", "content": result})
        st.rerun()


# ══════════════════════════════════════════════════════════════════
# SECTION 6 — GLOBAL CSS
# ══════════════════════════════════════════════════════════════════

st.markdown("""
<style>
html,body,[data-testid="stAppViewContainer"]{background-color:#0f1929}
[data-testid="stSidebar"]{background-color:#162236;border-right:1px solid rgba(0,212,180,0.15)}
[data-testid="stSidebar"] *{color:#c8ddf0}
.main .block-container{padding-top:1.2rem;padding-left:2rem;padding-right:2rem}
h1,h2,h3,h4,p,li,label{color:#c8ddf0 !important}
.stMarkdown{color:#c8ddf0}.stCaption{color:#8899aa !important}
.stCode>div{background:#0f1929 !important;border:1px solid rgba(0,212,180,0.15) !important;border-radius:8px !important;font-size:11.5px !important}
[data-testid="stVerticalBlockBorderWrapper"]{border-color:rgba(0,212,180,0.2) !important;border-radius:10px !important;background:rgba(22,34,54,0.5) !important}
.stProgress>div>div{background:linear-gradient(90deg,#00d4b4,#4a9eff) !important;border-radius:4px !important}
.stProgress{margin-bottom:4px !important}
[data-testid="stMetricValue"]{color:#00d4b4 !important;font-size:20px !important}
[data-testid="stMetricLabel"]{color:#8899aa !important}
.streamlit-expanderHeader{background:rgba(22,34,54,0.6) !important;border:1px solid rgba(0,212,180,0.15) !important;border-radius:8px !important;color:#c8ddf0 !important;font-size:13px !important}
.streamlit-expanderContent{background:rgba(15,25,41,0.5) !important;border:1px solid rgba(0,212,180,0.1) !important;border-top:none !important;border-radius:0 0 8px 8px !important}
.stDataFrame{border-radius:10px !important;border:1px solid rgba(0,212,180,0.2) !important;overflow:hidden !important}
[data-testid="stDataFrameResizable"] th{background:#1e3150 !important;color:#00d4b4 !important;font-family:monospace !important;font-size:11px !important}
[data-testid="stDataFrameResizable"] td{color:#8899aa !important;font-size:12px !important}
.stRadio [role="radiogroup"]{flex-wrap:wrap !important;gap:6px !important}
.stRadio label{background:rgba(22,34,54,0.5) !important;border:1px solid rgba(0,212,180,0.2) !important;border-radius:7px !important;padding:4px 12px !important;color:#8899aa !important;font-size:12px !important;cursor:pointer !important}
.stRadio label:has(input:checked){border-color:rgba(0,212,180,0.6) !important;background:rgba(0,212,180,0.12) !important;color:#00d4b4 !important}
.stTextInput input,.stTextArea textarea{background:rgba(22,34,54,0.6) !important;border:1px solid rgba(0,212,180,0.2) !important;border-radius:8px !important;color:#c8ddf0 !important;font-size:13px !important}
.stTextInput input:focus,.stTextArea textarea:focus{border-color:rgba(0,212,180,0.5) !important;box-shadow:0 0 0 1px rgba(0,212,180,0.3) !important}
.stButton>button{background:rgba(0,212,180,0.1) !important;border:1px solid rgba(0,212,180,0.3) !important;color:#00d4b4 !important;border-radius:8px !important}
.stButton>button:hover{background:rgba(0,212,180,0.2) !important;border-color:rgba(0,212,180,0.6) !important}
.stCheckbox label{color:#8899aa !important;font-size:12px !important}
.stSuccess{background:rgba(46,204,113,0.1) !important;border-color:rgba(46,204,113,0.4) !important;color:#7fbf7f !important}
.stInfo{background:rgba(74,158,255,0.1) !important;border-color:rgba(74,158,255,0.4) !important;color:#7faadf !important}
.stWarning{background:rgba(245,166,35,0.1) !important;border-color:rgba(245,166,35,0.4) !important;color:#c8a05a !important}
.stError{background:rgba(255,107,107,0.1) !important;border-color:rgba(255,107,107,0.4) !important;color:#c87f7f !important}
hr{border-color:rgba(0,212,180,0.15) !important}
::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:#334455;border-radius:4px}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════
# SECTION 7 — SESSION STATE
# ══════════════════════════════════════════════════════════════════

for section in CHECKLIST:
    for i in range(len(section["items"])):
        k = f"chk_{section['section']}_{i}"
        if k not in st.session_state:
            st.session_state[k] = False

for k, v in [("agent", None), ("api_key", ""), ("chat_history", []), ("pending_query", None)]:
    if k not in st.session_state:
        st.session_state[k] = v


# ══════════════════════════════════════════════════════════════════
# SECTION 8 — SIDEBAR
# ══════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("""
    <div style="padding:8px 0 12px">
        <div style="font-family:monospace;font-size:15px;font-weight:700;color:#00d4b4">
            ⬡ data<span style="color:#c8ddf0">.engineer</span> desk
        </div>
        <div style="font-size:11px;color:#556677;margin-top:3px">Complete reference · GPT-5.2 powered</div>
    </div>
    <hr style="border:none;border-top:1px solid rgba(0,212,180,0.15);margin-bottom:14px">
    """, unsafe_allow_html=True)

    # ── API key ──
    st.markdown('<div style="font-size:11px;color:#556677;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">AI Agent (GPT-5.2)</div>', unsafe_allow_html=True)

    api_key_input = st.text_input(
        "OpenAI API Key",
        type="password",
        placeholder="sk-proj-...",
        value=st.session_state.api_key,
        help="Get your key at platform.openai.com/api-keys",
    )
    st.caption("Get free key → platform.openai.com/api-keys")

    if api_key_input and api_key_input != st.session_state.api_key:
        st.session_state.api_key = api_key_input
        try:
            st.session_state.agent = DEAgent(api_key_input)
            st.session_state.chat_history = []
            st.success("Agent ready ✓", icon="🤖")
        except Exception as e:
            st.error(f"Could not connect: {e}")
    elif api_key_input and st.session_state.agent is None:
        try:
            st.session_state.agent = DEAgent(api_key_input)
        except Exception as e:
            st.error(f"Could not connect: {e}")

    st.markdown('<hr style="border:none;border-top:1px solid rgba(0,212,180,0.1);margin:12px 0">', unsafe_allow_html=True)

    # ── Navigation ──
    st.markdown('<div style="font-size:11px;color:#556677;font-family:monospace;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">Navigate</div>', unsafe_allow_html=True)

    SECTIONS = {
        "📌 Overview & AI Agent":      "overview",
        "🛠  Tools & Stack":           "tools",
        "🔁  ETL Pipeline":            "pipeline",
        "🏗  Architecture":            "architecture",
        "☁️  Cloud & Services":        "cloud",
        "🧩  Design Patterns":         "patterns",
        "📐  Engineering Principles":  "principles",
        "⚡  Streaming & Real-time":    "streaming",
        "📊  Tool Comparisons":        "comparisons",
        "🏢  Real-world Case Studies": "realworld",
        "🎯  Interview Q&A":          "interview",
        "🚀  Career Roadmap":         "career",
        "⚠️  Anti-patterns":           "antipatterns",
        "✅  Pipeline Checklist":      "checklist",
        "📖  Glossary":                "glossary",
    }
    selected = st.radio("nav", list(SECTIONS.keys()), label_visibility="collapsed")
    current  = SECTIONS[selected]

    st.markdown('<hr style="border:none;border-top:1px solid rgba(0,212,180,0.1);margin:12px 0">', unsafe_allow_html=True)
    st.markdown('<div style="font-size:10px;color:#334455;font-family:monospace;line-height:1.8">Built By Gulshan </div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════
# SECTION 9 — RENDER SELECTED SECTION
# ══════════════════════════════════════════════════════════════════

RENDERERS = {
    "overview":     render_landing,
    "tools":        render_tools,
    "pipeline":     render_pipeline,
    "architecture": render_architecture,
    "cloud":        render_cloud,
    "patterns":     render_patterns,
    "principles":   render_principles,
    "streaming":    render_streaming,
    "comparisons":  render_comparisons,
    "realworld":    render_realworld,
    "interview":    render_interview,
    "career":       render_career,
    "antipatterns": render_antipatterns,
    "checklist":    render_checklist,
    "glossary":     render_glossary,
}

RENDERERS[current]()
