# Eagle Analytics Hub — v7.1 Fix Summary

## Issues Fixed

### 1. 🌐📋 Combined Sources — Smart Deduplication

**Problem:** Sources like "Google", "google", "Google Search", "Google Search Console" appeared as separate rows in the Combined Sources table. Same for LinkedIn/Linkedin, YouTube/Youtube, AI/ChatGPT/Claude, etc.

**Root Cause:** The old code only did simple `lower()` matching (`cr["Source"].lower() == src.lower()`). This missed many variants:
- "Google Search Console" ≠ "google" → separate rows
- "ChatGPT" ≠ "AI" → separate rows
- "recommendation" ≠ "Referral" → separate rows

**Fix:** Now uses `source_normalizer.normalize_source()` for BOTH GA4 traffic sources AND CRM lead sources before combining. The normalizer has 4-tier matching:
1. Exact fuzzy match against canonical map (27 source groups, 200+ variants)
2. Contains/compound matching ("Google, Instagram" → Google)
3. Keyword classification (AI, search, social, referral, etc.)
4. Unknown — kept as-is but title-cased

**Files Changed:** `app.py` (Combined Sources section, ~lines 1391-1475)

**Result:** One "Google" row, one "LinkedIn" row, one "AI Tools" row, etc. Sessions + Signups properly summed.

---

### 2. 💳 Stripe Paid Count — Under-counting Customers

**Problem:** Paid customers showing 1 for the month, but should be ~3 or more.

**Root Cause:** `categorize_stripe()` in `process_data.py` required `spend > 0 AND date` to ACCEPT. But some paying customers have:
- `Total spend = $0.00` (refund, pending, display issue)
- Empty `Total spend` field
- The real indicator is `First payment` date — if it exists, customer HAS paid

**Fix:** Three-tier acceptance logic:
1. **First payment date exists** → ACCEPTED (regardless of spend amount)
2. **Payment Count ≥ 1 AND any date** → ACCEPTED
3. **Spend > 0 AND any date** → ACCEPTED (with extended date fallback)

Also added `Payment Count` to Stripe COLUMN_MAP for scraping.

**Files Changed:** `process_data.py`, `scrape_stripe.py`

---

### 3. 📅 Stripe Date Priority — Inconsistent Across System

**Problem:** Different parts of the code used different date fields for Stripe customers:
- `daily_counts.py` used "Created"
- `app.py` Method 2 used "Created" first, then "First payment"
- `reporting_engine.py` used "row_date_used" first, then "Created"
- `kpi_bridge.py` used "Created" first, then "First payment"

**Fix:** Unified priority across ALL files:
1. **First payment** (most reliable — confirms actual payment)
2. **row_date_used** (set by process_data from First payment)
3. **Created** (account creation date, not payment date)

**Files Changed:** `app.py`, `daily_counts.py`, `reporting_engine.py`, `kpi_bridge.py`

---

### 4. 🚀 Auto-Trigger Clarity

**Problem:** Toast said "Auto-triggered daily pipeline (data was stale)" but users didn't know to wait for pipeline to complete.

**Fix:** Toast now says "Auto-triggered daily pipeline (data was stale) — refresh in ~5 min for updated numbers"

**Files Changed:** `app.py`

---

## Files Modified

| File | Changes |
|------|---------|
| `app.py` | Combined Sources smart dedup, Stripe date priority in Method 2 + override delta, auto-trigger toast |
| `process_data.py` | `categorize_stripe()` 3-tier acceptance + Payment Count support |
| `daily_counts.py` | Stripe date field = "First payment", fallback = "Created" |
| `reporting_engine.py` | Stripe date priority: First payment → row_date_used → Created |
| `kpi_bridge.py` | `fetch_paid_details()` date priority: First payment → row_date_used → Created |
| `scrape_stripe.py` | Added `Payment Count` to COLUMN_MAP |

---

## Pipeline Auto-Run Verification

The GitHub Actions workflow (`daily_pipeline.yml`) runs:
- **0:00 UTC daily** — captures previous day's data
- **12:00 UTC daily** — end-of-day capture (AoE standard, all timezones completed)
- **Auto-trigger** from dashboard when data is stale (via `GITHUB_TOKEN` secret)

### ⚠️ MANDATORY: Enable the workflow
1. Go to: https://github.com/fozayelibnayaz/eagle3d-kpi-automation/actions
2. If you see "Workflows aren't being run on this repository" → Click **"I understand my workflows, go ahead and enable them"**
3. The "Daily KPI Pipeline" workflow must show as **active** (not disabled)
4. If it's been 60 days without a run, GitHub auto-disables it

### GitHub Secrets Required

| Secret | Purpose | How to Get |
|--------|---------|------------|
| `GOOGLE_CREDS_JSON` | Google Sheets API | Service account JSON key |
| `MASTER_SHEET_URL` | Master spreadsheet URL | Already set |
| `KPI_EMAIL` | KPI dashboard login | Already set |
| `KPI_PASSWORD` | KPI dashboard login | Already set |
| `STRIPE_COOKIES_JSON` | Stripe dashboard access | Export cookies from browser as JSON array |
| `TELEGRAM_BOT_TOKEN` | Telegram notifications | Already set |
| `TELEGRAM_CHAT_ID` | Telegram chat | Already set |
| `YOUTUBE_API_KEY` | YouTube Data API | Google Cloud Console |
| `YOUTUBE_CHANNEL_ID` | YouTube channel | Already set |
| `LINKEDIN_COMPANY_PAGE` | LinkedIn company URL | Already set |
| `GITHUB_TOKEN` | Auto-trigger pipeline | GitHub PAT with `repo` scope |

---

## Deploy Instructions

```bash
cd eagle3d-kpi-automation
git pull --rebase origin main
git add -A
git commit -m "v7.1: Fix Combined Sources dedup + Stripe paid count + date priority"
git push origin main
```

Or use: `bash deploy_v71_fixes.sh`

Then:
1. ✅ Verify GitHub Actions workflow is enabled
2. ✅ Manually trigger once to test
3. ✅ Check Stripe paid count increases
4. ✅ Check Combined Sources shows deduplicated rows
