#!/usr/bin/env python3
"""
AI Assistant Engine - Multi-platform, multi-model, with memory
Reads REAL data from Supabase for accurate grounded answers.
Supports Groq (primary) + Gemini (fallback).
Conversation history stored in Supabase for memory persistence.
"""

import os
import json
import urllib.request
import urllib.error
import hashlib
from datetime import datetime, date, timedelta
from typing import Optional


def _get_sb():
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        try:
            import streamlit as st
            url = str(st.secrets.get("SUPABASE_URL", "")).strip()
            key = str(st.secrets.get("SUPABASE_SERVICE_KEY", "")).strip()
        except Exception:
            pass
    if not url or not key:
        return None
    from supabase import create_client
    return create_client(url, key)


def _get_groq_key():
    v = os.environ.get("GROQ_API_KEY", "")
    if not v:
        try:
            import streamlit as st
            v = str(st.secrets.get("GROQ_API_KEY", "")).strip()
        except Exception:
            pass
    return v


def _get_gemini_key():
    v = os.environ.get("GEMINI_API_KEY", "")
    if not v:
        try:
            import streamlit as st
            v = str(st.secrets.get("GEMINI_API_KEY", "")).strip()
        except Exception:
            pass
    return v


# ═════════════════════════════════════════════════
# CONTEXT BUILDERS - per platform
# ═════════════════════════════════════════════════

def context_kpi():
    sb = _get_sb()
    if not sb:
        return {}
    ctx = {}
    try:
        today = date.today().strftime("%Y-%m-%d")
        month_start = date.today().strftime("%Y-%m-01")
        upload_start = "2025-12-01"
        ur = sb.table("uploads").select("upload_date").eq("final_status","ACCEPTED").order("upload_date").limit(1).execute()
        if ur.data and ur.data[0].get("upload_date"):
            upload_start = str(ur.data[0]["upload_date"])[:10]

        ctx["today"] = today
        ctx["common_period_start"] = upload_start
        ctx["today_signups"]  = sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").gte("signup_date",today).execute().count or 0
        ctx["today_uploads"]  = sb.table("uploads").select("count",count="exact").eq("final_status","ACCEPTED").gte("upload_date",today).execute().count or 0
        ctx["today_paid"]     = sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").gte("first_payment_date",today).execute().count or 0
        ctx["month_signups"]  = sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").gte("signup_date",month_start).execute().count or 0
        ctx["month_uploads"]  = sb.table("uploads").select("count",count="exact").eq("final_status","ACCEPTED").gte("upload_date",month_start).execute().count or 0
        ctx["month_paid"]     = sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").gte("first_payment_date",month_start).execute().count or 0
        ctx["alltime_signups"] = sb.table("signups").select("count",count="exact").eq("final_status","ACCEPTED").gte("signup_date",upload_start).execute().count or 0
        ctx["alltime_uploads"] = sb.table("uploads").select("count",count="exact").eq("final_status","ACCEPTED").gte("upload_date",upload_start).execute().count or 0
        ctx["alltime_paid"]    = sb.table("payments").select("count",count="exact").eq("final_status","ACCEPTED").gte("first_payment_date",upload_start).execute().count or 0
        # Revenue
        pays = sb.table("payments").select("total_spend,first_payment_date").eq("final_status","ACCEPTED").execute().data or []
        ctx["total_revenue"] = round(sum(float(p.get("total_spend") or 0) for p in pays), 2)
        ctx["paying_customers"] = len([p for p in pays if (p.get("total_spend") or 0) > 0])
        if ctx["paying_customers"]:
            ctx["avg_subscription"] = round(ctx["total_revenue"] / ctx["paying_customers"], 2)
        # Conversion
        if ctx["alltime_signups"]:
            ctx["s2u_rate"] = round(ctx["alltime_uploads"] / ctx["alltime_signups"] * 100, 2)
            ctx["s2p_rate"] = round(ctx["alltime_paid"]    / ctx["alltime_signups"] * 100, 2)
        # Top lead sources
        signups_data = sb.table("signups").select("lead_source").eq("final_status","ACCEPTED").execute().data or []
        from collections import Counter
        sources = Counter(s.get("lead_source","") for s in signups_data if s.get("lead_source"))
        ctx["top_lead_sources"] = sources.most_common(10)
    except Exception as e:
        ctx["error"] = str(e)
    return ctx


def context_linkedin():
    sb = _get_sb()
    if not sb:
        return {}
    ctx = {}
    try:
        hl = sb.table("linkedin_highlights_daily").select("*").order("snapshot_date",desc=True).limit(1).execute().data
        if hl:
            ctx["latest"] = hl[0]
        posts = sb.table("linkedin_posts").select("*").order("impressions",desc=True).limit(20).execute().data or []
        ctx["top_posts"] = [
            {"title": p.get("title","")[:200], "impressions": p.get("impressions",0),
             "reactions": p.get("reactions",0), "comments": p.get("comments",0),
             "clicks": p.get("clicks",0), "ctr": p.get("ctr",0),
             "engagement_rate": p.get("engagement_rate",0)}
            for p in posts
        ]
        ctx["total_posts_tracked"] = sb.table("linkedin_posts").select("count",count="exact").execute().count or 0
        fd = sb.table("linkedin_followers_daily").select("snapshot_date,total,delta_total").order("snapshot_date").execute().data or []
        ctx["follower_history"] = fd[-30:]
        comps = sb.table("linkedin_competitors_daily").select("*").order("snapshot_date",desc=True).limit(10).execute().data or []
        ctx["competitors"] = comps
    except Exception as e:
        ctx["error"] = str(e)
    return ctx


def context_youtube():
    ctx = {}
    try:
        from youtube_command_center import get_cached_or_fetch
        d = get_cached_or_fetch(period_days=90)
        ctx["channel"]   = d.get("channel", {})
        ctx["analytics"] = d.get("analytics", {})
        ctx["revenue"]   = d.get("revenue", {})
        vids = d.get("videos", [])
        ctx["total_videos"] = len(vids)
        ctx["top_5_videos"] = sorted(vids, key=lambda v: v.get("views",0), reverse=True)[:5]
        ctx["bottom_5_videos"] = sorted([v for v in vids if v.get("views",0)>0], key=lambda v: v.get("views",0))[:5]
        # Engagement breakdown
        ctx["avg_engagement"] = round(sum(v.get("engagement",0) for v in vids) / len(vids), 2) if vids else 0
        ctx["total_views_all"] = sum(v.get("views",0) for v in vids)
        ctx["videos_with_no_views"] = len([v for v in vids if v.get("views",0)==0])
    except Exception as e:
        ctx["error"] = str(e)
    return ctx


def context_ga4():
    ctx = {}
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
        from google.oauth2 import service_account
        creds = None
        try:
            import streamlit as st
            sa = dict(st.secrets["ga4_service_account"])
            if "private_key" in sa:
                sa["private_key"] = sa["private_key"].replace("\\n", "\n")
            creds = service_account.Credentials.from_service_account_info(sa, scopes=["https://www.googleapis.com/auth/analytics.readonly"])
        except Exception:
            pass
        if not creds:
            return {"error": "GA4 credentials missing"}

        pid = os.environ.get("GA4_PROPERTY_ID","374525971")
        try:
            import streamlit as st
            pid = str(st.secrets.get("GA4_PROPERTY_ID", pid))
        except Exception:
            pass
        client = BetaAnalyticsDataClient(credentials=creds)
        end = date.today().strftime("%Y-%m-%d")
        start = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        r = client.run_report(RunReportRequest(
            property=f"properties/{pid}",
            date_ranges=[DateRange(start_date=start, end_date=end)],
            metrics=[Metric(name="sessions"), Metric(name="totalUsers"), Metric(name="screenPageViews")],
        ))
        if r.rows:
            row = r.rows[0]
            ctx["last_30d_sessions"]  = int(row.metric_values[0].value)
            ctx["last_30d_users"]     = int(row.metric_values[1].value)
            ctx["last_30d_pageviews"] = int(row.metric_values[2].value)

        # Top countries
        rc = client.run_report(RunReportRequest(
            property=f"properties/{pid}",
            date_ranges=[DateRange(start_date=start, end_date=end)],
            dimensions=[Dimension(name="country")],
            metrics=[Metric(name="sessions")],
            limit=10,
        ))
        ctx["top_countries"] = [(rw.dimension_values[0].value, int(rw.metric_values[0].value)) for rw in rc.rows]
    except Exception as e:
        ctx["error"] = str(e)
    return ctx


def context_customer_success():
    sb = _get_sb()
    if not sb:
        return {}
    ctx = {}
    try:
        ctx["total_in_cs_sheet"] = sb.table("customer_success_master").select("count",count="exact").execute().count or 0
        ctx["enriched"] = sb.table("customer_success_enriched").select("count",count="exact").execute().count or 0
        ctx["active_subs"] = sb.table("customer_success_enriched").select("count",count="exact").eq("subscription_status","active").execute().count or 0
        ctx["canceled"] = sb.table("customer_success_enriched").select("count",count="exact").eq("subscription_status","canceled").execute().count or 0
        ctx["delinquent"] = sb.table("customer_success_enriched").select("count",count="exact").eq("stripe_delinquent",True).execute().count or 0
        # Funnel
        funnel = sb.table("customer_success_enriched").select("signup_date,first_upload_date,first_payment_date,days_signup_to_paid").execute().data or []
        with_signup = sum(1 for f in funnel if f.get("signup_date"))
        with_upload = sum(1 for f in funnel if f.get("first_upload_date"))
        with_paid = sum(1 for f in funnel if f.get("first_payment_date"))
        ctx["funnel"] = {"signed_up": with_signup, "uploaded": with_upload, "paid": with_paid}
        days = [f["days_signup_to_paid"] for f in funnel if f.get("days_signup_to_paid")]
        if days:
            ctx["avg_days_to_paid"] = round(sum(days)/len(days), 1)
    except Exception as e:
        ctx["error"] = str(e)
    return ctx


def get_full_context(platform="all"):
    if platform == "kpi":
        return {"kpi": context_kpi()}
    if platform == "linkedin":
        return {"linkedin": context_linkedin()}
    if platform == "youtube":
        return {"youtube": context_youtube()}
    if platform == "ga4":
        return {"ga4": context_ga4()}
    if platform == "customer_success":
        return {"customer_success": context_customer_success()}
    return {
        "kpi":               context_kpi(),
        "linkedin":          context_linkedin(),
        "youtube":           context_youtube(),
        "ga4":               context_ga4(),
        "customer_success":  context_customer_success(),
    }


# ═════════════════════════════════════════════════
# AI CALL with memory
# ═════════════════════════════════════════════════

def _call_groq(messages, max_tokens=2000, temperature=0.2):
    key = _get_groq_key()
    if not key:
        return None, "GROQ_API_KEY not set"
    try:
        req = urllib.request.Request(
            "https://api.groq.com/openai/v1/chat/completions",
            data=json.dumps({
                "model":    "llama-3.3-70b-versatile",
                "messages": messages,
                "max_tokens":  max_tokens,
                "temperature": temperature,
            }).encode(),
            headers={
                "Authorization": f"Bearer {key}",
                "Content-Type":  "application/json",
                "User-Agent":    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept":        "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            r = json.loads(resp.read())
            return r["choices"][0]["message"]["content"], None
    except urllib.error.HTTPError as e:
        return None, f"Groq {e.code}: {e.read().decode()[:200]}"
    except Exception as e:
        return None, f"Groq error: {e}"


def _call_gemini(messages, max_tokens=2000, temperature=0.2):
    key = _get_gemini_key()
    if not key:
        return None, "GEMINI_API_KEY not set"
    try:
        # Convert OpenAI-style to Gemini contents
        contents = []
        for m in messages:
            role = "user" if m["role"] in ("user","system") else "model"
            contents.append({"role": role, "parts": [{"text": m["content"]}]})
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={key}"
        req = urllib.request.Request(
            url,
            data=json.dumps({
                "contents": contents,
                "generationConfig": {"maxOutputTokens": max_tokens, "temperature": temperature},
            }).encode(),
            headers={"Content-Type":"application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            r = json.loads(resp.read())
            return r["candidates"][0]["content"]["parts"][0]["text"], None
    except Exception as e:
        return None, f"Gemini error: {e}"


def ask_ai(question: str, platform: str = "all", history: list = None, user_email: str = "anonymous"):
    """
    Ask AI a question with platform context + conversation history.
    Returns dict: {answer, source, error, context_used, history}
    """
    history = history or []
    context = get_full_context(platform)

    system_prompt = f"""You are the Eagle3D KPI Hub AI Assistant, an expert business intelligence analyst.

You have access to REAL data from the Eagle3D Streaming business across these systems:
- KPI: signups, uploads, paid customers, conversion rates, revenue
- LinkedIn: posts, impressions, reactions, followers, competitors
- YouTube: channel stats, video performance, analytics
- GA4: website sessions, users, page views, countries
- Customer Success: customer health, churn, lifetime, funnel

CURRENT DATA CONTEXT (use this for ALL answers):
{json.dumps(context, indent=2, default=str)[:8000]}

RULES:
1. Use ONLY the data provided above. Never make up numbers.
2. If asked about something not in the data, say "I don't have that data yet"
3. Quote specific numbers from context when answering
4. Be concise but thorough
5. Format with headers, bullets, and bold for readability
6. Always include actionable insights
7. Remember previous conversation context

Today's date: {date.today().isoformat()}
User: {user_email}
Platform focus: {platform}"""

    messages = [{"role": "system", "content": system_prompt}]
    for h in history[-10:]:
        messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": question})

    # Try Groq first
    answer, err = _call_groq(messages, max_tokens=2500)
    if answer:
        _save_conversation(user_email, platform, question, answer, "groq")
        return {"answer": answer, "source": "Groq llama-3.3-70b", "error": None, "context_used": bool(context)}

    # Fallback Gemini
    answer, err2 = _call_gemini(messages, max_tokens=2500)
    if answer:
        _save_conversation(user_email, platform, question, answer, "gemini")
        return {"answer": answer, "source": "Gemini 1.5", "error": None, "context_used": bool(context)}

    return {"answer": None, "source": None, "error": f"Both failed: Groq={err} | Gemini={err2}"}


def _save_conversation(user_email, platform, question, answer, model):
    sb = _get_sb()
    if not sb:
        return
    try:
        sb.table("ai_conversations").insert({
            "user_email":  user_email,
            "platform":    platform,
            "question":    question,
            "answer":      answer,
            "model_used":  model,
            "created_at":  datetime.utcnow().isoformat(),
        }).execute()
    except Exception:
        pass


def get_history(user_email, platform=None, limit=50):
    sb = _get_sb()
    if not sb:
        return []
    try:
        q = sb.table("ai_conversations").select("*").eq("user_email", user_email)
        if platform:
            q = q.eq("platform", platform)
        q = q.order("created_at", desc=True).limit(limit)
        return q.execute().data or []
    except Exception:
        return []


# ═════════════════════════════════════════════════
# AI POWER TOOLS (specialized prompts)
# ═════════════════════════════════════════════════

TOOLS = {
    "youtube": [
        {"id":"content_strategy",     "name":"Content Strategy",       "desc":"90-day strategy from YOUR data"},
        {"id":"competitive_gap",      "name":"Competitive Gap",        "desc":"Find content opportunities", "input":"competitor channel"},
        {"id":"thumbnail_audit",      "name":"Thumbnail Audit",        "desc":"What thumbnails work"},
        {"id":"title_optimizer",      "name":"Title Optimizer",        "desc":"Rewrite for max CTR", "input":"current title"},
        {"id":"upload_schedule",      "name":"Upload Schedule",        "desc":"Data-driven schedule"},
        {"id":"audience_persona",     "name":"Audience Persona",       "desc":"Who your viewers are"},
        {"id":"viral_detector",       "name":"Viral Pattern Detector", "desc":"Replicate viral success"},
        {"id":"comment_insights",     "name":"Comment Insights",       "desc":"Ideas from comments"},
        {"id":"weekly_report",        "name":"Weekly Report",          "desc":"Executive-ready report"},
        {"id":"30day_plan",           "name":"30-Day Plan",            "desc":"Actionable daily plan"},
        {"id":"video_clone",          "name":"Video Clone Blueprint",  "desc":"Replicate top videos", "input":"video title"},
        {"id":"description_writer",   "name":"Description Writer",     "desc":"SEO-optimized descriptions", "input":"video topic"},
        {"id":"tag_optimizer",        "name":"Tag Optimizer",          "desc":"Generate optimal SEO tags", "input":"video topic"},
        {"id":"swot",                 "name":"SWOT Analysis",          "desc":"Strengths/Weaknesses/Opportunities/Threats"},
        {"id":"ab_test",              "name":"A/B Test Planner",       "desc":"Plan title/thumbnail tests", "input":"video URL"},
        {"id":"hook_gen",             "name":"Hook Generator",         "desc":"First 15s hooks", "input":"video topic"},
        {"id":"script_writer",        "name":"Script Writer",          "desc":"Complete video scripts", "input":"video topic"},
        {"id":"shorts_ideas",         "name":"Shorts Ideas",           "desc":"Viral shorts from your videos"},
        {"id":"chapters",             "name":"Auto Chapters",          "desc":"Timestamp chapters", "input":"video URL"},
        {"id":"sponsor_pitch",        "name":"Sponsor Pitch",          "desc":"Brand outreach emails", "input":"brand name"},
        {"id":"community_posts",      "name":"Community Posts",        "desc":"High-engagement posts"},
        {"id":"smart_reply",          "name":"Smart Reply",            "desc":"Reply to comments", "input":"comment text"},
        {"id":"cta_gen",              "name":"CTA Generator",          "desc":"Convert viewers to subs", "input":"video topic"},
        {"id":"trend_radar",          "name":"Trend Radar",            "desc":"Trending topics now", "input":"niche"},
        {"id":"keyword_finder",       "name":"Keyword Finder",         "desc":"Low-comp keywords", "input":"topic"},
        {"id":"channel_audit",        "name":"Full Channel Audit",     "desc":"Complete A-F review"},
        {"id":"revenue_forecast",     "name":"Revenue Forecast",       "desc":"Predict earnings"},
        {"id":"collab_finder",        "name":"Collab Finder",          "desc":"Channels to collab with"},
        {"id":"niche_analyzer",       "name":"Niche Analyzer",         "desc":"Niche saturation", "input":"niche"},
        {"id":"algo_decoder",         "name":"Algorithm Decoder",      "desc":"What YT favors now"},
    ],
    "kpi": [
        {"id":"signup_strategy",      "name":"Signup Growth Strategy", "desc":"Plan to 2x signups"},
        {"id":"funnel_audit",         "name":"Funnel Audit",           "desc":"Where users drop off"},
        {"id":"churn_prevention",     "name":"Churn Prevention Plan",  "desc":"Reduce subscription losses"},
        {"id":"revenue_optimization", "name":"Revenue Optimization",   "desc":"Increase MRR + LTV"},
        {"id":"lead_source_analysis", "name":"Lead Source Analysis",   "desc":"Best vs worst channels"},
        {"id":"region_strategy",      "name":"Region Strategy",        "desc":"Geographic expansion plan"},
        {"id":"month_forecast",       "name":"Month Forecast",         "desc":"Predict end-of-month KPIs"},
        {"id":"conversion_audit",     "name":"Conversion Audit",       "desc":"Sign->Upload->Paid analysis"},
    ],
    "linkedin": [
        {"id":"post_strategy",        "name":"Post Strategy",          "desc":"What to post next"},
        {"id":"top_post_replicate",   "name":"Top Post Replicator",    "desc":"Replicate viral posts"},
        {"id":"follower_growth",      "name":"Follower Growth Plan",   "desc":"Hit your next milestone"},
        {"id":"competitor_steal",     "name":"Competitor Tactics",     "desc":"What competitors do well"},
        {"id":"engagement_audit",     "name":"Engagement Audit",       "desc":"Why posts under-perform"},
        {"id":"content_calendar",     "name":"30-Day Content Calendar","desc":"Daily post plan"},
        {"id":"hashtag_strategy",     "name":"Hashtag Strategy",       "desc":"Best tags for reach", "input":"topic"},
        {"id":"thought_leadership",   "name":"Thought Leadership",     "desc":"Position founder as expert"},
    ],
    "customer_success": [
        {"id":"churn_predict",        "name":"Churn Prediction",       "desc":"Who's about to leave"},
        {"id":"upsell_targets",       "name":"Upsell Targets",         "desc":"Best customers for upgrades"},
        {"id":"at_risk_action",       "name":"At-Risk Action Plan",    "desc":"Save customers at risk"},
        {"id":"onboarding_audit",     "name":"Onboarding Audit",       "desc":"Why signups don't convert"},
        {"id":"customer_segments",    "name":"Customer Segments",      "desc":"Group customers by behavior"},
        {"id":"win_back_canceled",    "name":"Win-Back Campaign",      "desc":"Re-engage canceled users"},
    ],
    "ga4": [
        {"id":"traffic_strategy",     "name":"Traffic Strategy",       "desc":"Grow website visitors"},
        {"id":"seo_opportunities",    "name":"SEO Opportunities",      "desc":"Pages to optimize"},
        {"id":"country_expansion",    "name":"Country Expansion",      "desc":"Where to invest marketing"},
        {"id":"weekly_traffic",       "name":"Weekly Traffic Report",  "desc":"Executive summary"},
    ],
}


def run_tool(platform: str, tool_id: str, user_input: str = "", user_email: str = "anonymous"):
    """Run a specific AI power tool."""
    tools_for_platform = TOOLS.get(platform, [])
    tool = next((t for t in tools_for_platform if t["id"] == tool_id), None)
    if not tool:
        return {"error": f"Unknown tool: {tool_id}"}

    prompt = f"""Run the '{tool['name']}' AI tool: {tool['desc']}.

{'User input: ' + user_input if user_input else ''}

Use the data context to provide a complete, actionable, data-driven response.
Format with clear sections, bullet points, and specific numbers from the data."""

    return ask_ai(prompt, platform=platform, user_email=user_email)


if __name__ == "__main__":
    r = ask_ai("How is the business doing overall?", platform="all", user_email="test")
    print(r.get("answer", r.get("error")))
