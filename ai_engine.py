"""
ai_engine.py — Unified AI Engine for Eagle3D KPI System v2
==========================================================
Supports: Groq (fast), Google Gemini, fallback to rule-based.
8+ AI tools for deep analytics. Chat history. Smart context.
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional

# ── Provider detection ─────────────────────────────────────

def _get_provider():
    """Returns 'groq', 'gemini', or 'rule_based'."""
    if os.environ.get("GROQ_API_KEY"):
        return "groq"
    try:
        import streamlit as st
        if "GROQ_API_KEY" in st.secrets:
            return "groq"
    except Exception:
        pass
    if os.environ.get("GEMINI_API_KEY"):
        return "gemini"
    try:
        import streamlit as st
        if "GEMINI_API_KEY" in st.secrets:
            return "gemini"
    except Exception:
        pass
    return "rule_based"


def _get_api_key(provider):
    """Get API key from env or Streamlit secrets."""
    key_name = "GROQ_API_KEY" if provider == "groq" else "GEMINI_API_KEY"
    key = os.environ.get(key_name, "")
    if key:
        return key
    try:
        import streamlit as st
        return st.secrets.get(key_name, "")
    except Exception:
        return ""


# ── Groq API (OpenAI-compatible) ────────────────────────────

def _call_groq(system_prompt: str, user_message: str, api_key: str) -> str:
    """Call Groq API (fast LLM inference)."""
    import urllib.request
    import json as _json

    url = "https://api.groq.com/openai/v1/chat/completions"
    payload = _json.dumps({
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "temperature": 0.3,
        "max_tokens": 2500,
    }).encode("utf-8")

    req = urllib.request.Request(
        url, data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = _json.loads(resp.read())
            return result["choices"][0]["message"]["content"]
    except Exception as e:
        # Try Gemini as fallback before rule-based
        gemini_key = _get_api_key("gemini")
        if gemini_key:
            try:
                return _call_gemini(system_prompt, user_message, gemini_key)
            except:
                pass
        return f"Groq API error: {e}. Falling back to rule-based analysis.\n\n" + _rule_based_answer(user_message, system_prompt)


# ── Gemini API ──────────────────────────────────────────────

def _call_gemini(system_prompt: str, user_message: str, api_key: str) -> str:
    """Call Google Gemini API."""
    import urllib.request
    import json as _json

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
    payload = _json.dumps({
        "system_instruction": {"parts": [{"text": system_prompt}]},
        "contents": [{"parts": [{"text": user_message}]}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 2500},
    }).encode("utf-8")

    req = urllib.request.Request(
        url, data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = _json.loads(resp.read())
            return result["candidates"][0]["content"]["parts"][0]["text"]
    except Exception as e:
        return f"Gemini API error: {e}. Falling back to rule-based analysis.\n\n" + _rule_based_answer(user_message, system_prompt)


# ── Rule-based fallback ─────────────────────────────────────

def _rule_based_answer(question: str, data_context: str) -> str:
    """Intelligent rule-based analysis when no AI API is available."""
    q = question.lower()
    lines = [f"## 📊 Analysis\n"]
    
    # Extract numbers from context
    import re
    numbers = re.findall(r'(\d+)\s*(?:sign|signup|upload|paid|customer|session|conversion)', q + ' ' + data_context, re.IGNORECASE)
    
    # Smart pattern matching
    if any(w in q for w in ["why", "reason", "cause", "dropped", "declined", "decreased"]):
        lines.append("### 🔍 Root Cause Analysis")
        lines.append("Based on the data patterns:")
        lines.append("- **Check if there were any marketing changes** in the comparison period")
        lines.append("- **Seasonal patterns** may be affecting traffic quality")
        lines.append("- **Source mix shifts** could change conversion rates")
        lines.append("- **Technical issues** may have impacted sign-up flow")
        lines.append("\n*Connect Groq or Gemini for deeper AI analysis.*")
    elif any(w in q for w in ["improve", "increase", "grow", "better", "optimize", "strategy"]):
        lines.append("### 💡 Growth Recommendations")
        lines.append("Based on current data patterns:")
        lines.append("1. **Optimize top-performing channels** — double down on what's working")
        lines.append("2. **Improve onboarding flow** — reduce friction from sign-up to first upload")
        lines.append("3. **Content marketing** — create tutorials for Unreal Engine / 3D streaming")
        lines.append("4. **AI search optimization** — ensure docs are indexed by ChatGPT, Gemini, Perplexity")
        lines.append("5. **Referral program** — incentivize word-of-mouth with credits")
        lines.append("\n*Connect Groq or Gemini for data-specific recommendations.*")
    elif any(w in q for w in ["predict", "forecast", "future", "next week", "next month"]):
        lines.append("### 🔮 Forecast Note")
        lines.append("Visit the **🔮 Predictions** page for ML-powered forecasts.")
        lines.append("The ensemble model uses Moving Average + Linear Regression + Exponential Smoothing.")
        lines.append("\n*Connect Groq or Gemini for narrative-driven forecasting.*")
    else:
        lines.append(data_context)
        lines.append("\n---")
        lines.append("*Connect Groq or Gemini API for deeper AI analysis.*")
    
    lines.append("\n---")
    lines.append("*Analysis by Eagle3D Intelligence Engine (rule-based mode)*")
    return "\n".join(lines)


# ── Data Context Builder ────────────────────────────────────

def build_data_context(
    kpi_df=None, utm_df=None, pages_df=None, events_df=None,
    geo_df=None, leads_df=None, period_label: str = "this period",
    prev_kpi_df=None, prev_utm_df=None,
) -> str:
    """Build a comprehensive data summary for AI context."""
    lines = []
    
    # KPI Data
    if kpi_df is not None and not kpi_df.empty:
        s = int(pd.to_numeric(kpi_df.get("signups", 0), errors="coerce").fillna(0).sum())
        u = int(pd.to_numeric(kpi_df.get("first_uploads", 0), errors="coerce").fillna(0).sum())
        p = int(pd.to_numeric(kpi_df.get("paid_customers", 0), errors="coerce").fillna(0).sum())
        
        lines.append(f"### CRM KPI Data ({period_label})")
        lines.append(f"- Sign-ups: {s}")
        lines.append(f"- First Uploads: {u}")
        lines.append(f"- Paid Customers: {p}")
        
        # Funnel rates
        s2u = (u / s * 100) if s > 0 else 0
        u2p = (p / u * 100) if u > 0 else 0
        s2p = (p / s * 100) if s > 0 else 0
        lines.append(f"- Sign-up → Upload rate: {s2u:.1f}%")
        lines.append(f"- Upload → Paid rate: {u2p:.1f}%")
        lines.append(f"- Sign-up → Paid rate: {s2p:.1f}%")
        
        if prev_kpi_df is not None and not prev_kpi_df.empty:
            ps = int(pd.to_numeric(prev_kpi_df.get("signups", 0), errors="coerce").fillna(0).sum())
            pu = int(pd.to_numeric(prev_kpi_df.get("first_uploads", 0), errors="coerce").fillna(0).sum())
            pp = int(pd.to_numeric(prev_kpi_df.get("paid_customers", 0), errors="coerce").fillna(0).sum())
            
            s_change = ((s - ps) / ps * 100) if ps > 0 else 0
            u_change = ((u - pu) / pu * 100) if pu > 0 else 0
            p_change = ((p - pp) / pp * 100) if pp > 0 else 0
            
            lines.append(f"\n### Comparison (vs Previous Period)")
            lines.append(f"- Sign-ups: {s} vs {ps} ({s_change:+.1f}%)")
            lines.append(f"- Uploads: {u} vs {pu} ({u_change:+.1f}%)")
            lines.append(f"- Paid: {p} vs {pp} ({p_change:+.1f}%)")
        
        # Daily breakdown
        if "date" in kpi_df.columns and len(kpi_df) > 1:
            lines.append(f"\n### Daily Breakdown")
            for _, row in kpi_df.head(14).iterrows():
                d = row.get("date", "")
                ds = int(pd.to_numeric(row.get("signups", 0), errors="coerce") or 0)
                du = int(pd.to_numeric(row.get("first_uploads", 0), errors="coerce") or 0)
                dp = int(pd.to_numeric(row.get("paid_customers", 0), errors="coerce") or 0)
                lines.append(f"- {d}: {ds} signups, {du} uploads, {dp} paid")
    
    # Lead Sources
    if leads_df is not None and not leads_df.empty:
        lines.append(f"\n### CRM Lead Sources ({period_label})")
        for _, row in leads_df.head(10).iterrows():
            src = row.get("Lead Source", "?")
            cnt = int(pd.to_numeric(row.get("Signups", 0), errors="coerce") or 0)
            pct = row.get("% of Total", 0)
            lines.append(f"- {src}: {cnt} signups ({pct}%)")
    
    # Traffic Sources
    if utm_df is not None and not utm_df.empty:
        lines.append(f"\n### GA4 Traffic ({period_label})")
        src_col = "source_normalized" if "source_normalized" in utm_df.columns else "sessionSource"
        sessions_col = "sessions" if "sessions" in utm_df.columns else src_col
        conv_col = "conversions" if "conversions" in utm_df.columns else None
        
        by_src = utm_df.groupby(src_col).agg(
            sessions=(sessions_col, "sum"),
        ).sort_values("sessions", ascending=False).head(10)
        
        for src, row in by_src.iterrows():
            lines.append(f"- {src}: {int(row['sessions'])} sessions")
    
    return "\n".join(lines)


# ── System Prompt ───────────────────────────────────────────

SYSTEM_PROMPT = """You are Eagle3D AI Analytics Assistant — an expert data analyst for Eagle3D Streaming (a pixel streaming / 3D cloud rendering platform).

Your role:
- Answer questions about sign-ups, uploads, paid customers, traffic, lead sources, conversions
- Provide data-driven insights and business recommendations
- Identify trends, anomalies, and opportunities
- Suggest actionable next steps
- Compare periods and explain changes

Rules:
- Always use the provided data to answer — never make up numbers
- If data is missing or insufficient, say so clearly
- Be concise but thorough
- Use markdown formatting (headers, bullet points, bold)
- When suggesting actions, be specific and actionable
- Consider the business context: Eagle3D is a B2B SaaS for 3D streaming
- Conversion funnel: Traffic → Sign-up → First Upload → Paid Customer

Key business context:
- Sign-ups come from Google, LinkedIn, YouTube, AI tools, referrals
- "First Upload" = user's first project upload (key activation metric)
- The company serves architects, game devs, 3D artists, real estate
- Competitors: Vagon, Parsec, Furioos
- Pricing: Free tier + paid plans
- When reporting numbers, always clarify what is REAL data vs prediction/forecast"""


# ── Main Ask Function ───────────────────────────────────────

def ask_ai(
    question: str,
    kpi_df=None, utm_df=None, pages_df=None, events_df=None,
    geo_df=None, leads_df=None, period_label: str = "this period",
    prev_kpi_df=None, prev_utm_df=None,
) -> dict:
    if not question or len(question.strip()) < 3:
        return {"answer": "Please ask a question.", "provider": "none", "status": "error"}

    provider = _get_provider()
    data_context = build_data_context(
        kpi_df, utm_df, pages_df, events_df, geo_df, leads_df,
        period_label, prev_kpi_df, prev_utm_df,
    )

    user_message = f"""Here is the current data for Eagle3D:

{data_context}

---
User Question: {question}

Please analyze the data above and answer the question thoroughly. Include specific numbers, percentages, and actionable recommendations. Clearly distinguish between REAL data and any forecasts."""

    if provider == "groq":
        api_key = _get_api_key("groq")
        answer = _call_groq(SYSTEM_PROMPT, user_message, api_key)
    elif provider == "gemini":
        api_key = _get_api_key("gemini")
        answer = _call_gemini(SYSTEM_PROMPT, user_message, api_key)
    else:
        answer = _rule_based_answer(question, data_context)

    return {
        "answer": answer,
        "provider": provider,
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
    }


def get_available_tools() -> list:
    """Return list of available AI tools."""
    return [
        {
            "id": "funnel_analysis",
            "name": "📊 Funnel",
            "description": "Analyze the full conversion funnel",
            "prompt": "Analyze the conversion funnel in detail. Where is the biggest drop-off? What specific actions should we take to improve each stage? Include real numbers.",
        },
        {
            "id": "source_attribution",
            "name": "🎯 Sources",
            "description": "Which traffic sources bring the best users?",
            "prompt": "Analyze all traffic and lead sources. Which sources have the highest conversion rates? Which should we invest more in? Which should we stop?",
        },
        {
            "id": "anomaly_detection",
            "name": "🚨 Anomalies",
            "description": "Find unusual patterns or spikes",
            "prompt": "Look for anomalies, unusual patterns, sudden spikes or drops in the data. Compare current period to previous. Flag anything abnormal and explain possible causes.",
        },
        {
            "id": "weekly_brief",
            "name": "📋 Briefing",
            "description": "Comprehensive weekly status report",
            "prompt": "Generate a comprehensive weekly briefing. Include: executive summary, key metrics with real numbers, trends, wins, concerns, and recommended actions.",
        },
        {
            "id": "business_decisions",
            "name": "💡 Decisions",
            "description": "Data-driven business recommendations",
            "prompt": "Based on all the data, what are the top 5 business decisions we should make this week? Be specific, tie each to real data points.",
        },
        {
            "id": "competitor_strategy",
            "name": "🥊 Compete",
            "description": "Competitive analysis and positioning",
            "prompt": "Based on our traffic and conversion data, how should we position Eagle3D against Vagon, Parsec, Furioos? What SEO, content, and partnership strategies?",
        },
        {
            "id": "cohort_analysis",
            "name": "👥 Cohort",
            "description": "Analyze user behavior by signup cohort",
            "prompt": "Analyze the signup-to-upload conversion patterns. Are newer cohorts converting better or worse? What does the data tell us about user activation?",
        },
        {
            "id": "revenue_forecast",
            "name": "💰 Revenue",
            "description": "Revenue prediction based on current pipeline",
            "prompt": "Based on current sign-up and conversion trends, project revenue for next 30/60/90 days. What is the expected customer acquisition? What would improve the forecast?",
        },
        {
            "id": "churn_risk",
            "name": "⚠️ Churn",
            "description": "Identify churn risk signals",
            "prompt": "Analyze the data for churn risk signals. Are there patterns suggesting users are dropping off? What activation metrics should we monitor? Suggest retention strategies.",
        },
        {
            "id": "team_report",
            "name": "📄 Report",
            "description": "Shareable stakeholder report",
            "prompt": "Create a professional stakeholder report with: key metrics summary, period-over-period comparison, top insights, concerns, and next steps. Use tables and clear sections.",
        },
        {
            "id": "growth_hacking",
            "name": "🚀 Growth",
            "description": "Growth hacking strategies based on data",
            "prompt": "Based on our current data, suggest 5 creative growth hacking strategies. Consider our funnel metrics, traffic sources, and conversion rates. Be specific and unconventional.",
        },
        {
            "id": "ai_seo",
            "name": "🤖 AI SEO",
            "description": "Optimize for AI search (ChatGPT, Gemini, Perplexity)",
            "prompt": "How can Eagle3D optimize its online presence for AI-powered search engines? Consider our traffic from AI tools and suggest a comprehensive AI SEO strategy.",
        },
    ]


def run_tool(tool_id: str, kpi_df=None, utm_df=None, pages_df=None,
             events_df=None, geo_df=None, leads_df=None,
             period_label: str = "this period",
             prev_kpi_df=None, prev_utm_df=None) -> dict:
    """Run a predefined AI tool."""
    tools = {t["id"]: t for t in get_available_tools()}
    tool = tools.get(tool_id)
    if not tool:
        return {"answer": f"Tool '{tool_id}' not found.", "provider": "none", "status": "error"}
    
    return ask_ai(
        tool["prompt"],
        kpi_df=kpi_df, utm_df=utm_df, pages_df=pages_df,
        events_df=events_df, geo_df=geo_df, leads_df=leads_df,
        period_label=period_label,
        prev_kpi_df=prev_kpi_df, prev_utm_df=prev_utm_df,
    )
