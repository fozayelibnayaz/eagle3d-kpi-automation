#!/usr/bin/env python3
# RUN FULL AUDIT - Master audit script
import json
from pathlib import Path
from datetime import datetime

AUDIT_DIR = Path("data_output/audits")
AUDIT_DIR.mkdir(parents=True, exist_ok=True)

def sep(title=""):
    print()
    print("=" * 70)
    if title:
        print("  " + title)
        print("=" * 70)

def main():
    sep("EAGLE3D - FULL DATA INTEGRITY AUDIT")
    print("Timestamp: " + datetime.utcnow().isoformat())
    results = {}

    sep("PRIORITY 1 - DATA INTEGRITY AUDIT")
    try:
        from data_integrity_auditor import run_audit, print_audit_report
        audit = run_audit()
        print_audit_report(audit)
        results["data_integrity"] = "WARN" if audit["validation_failures"] else "PASS"
        results["_audit"] = audit
    except Exception as e:
        print("ERROR: " + str(e))
        results["data_integrity"] = "ERROR"

    sep("PRIORITY 2 - MANUAL OVERRIDE SYSTEM")
    try:
        from override_engine import get_override_summary
        s = get_override_summary()
        print("  Total overrides:    " + str(s["total"]))
        print("  Accepted:           " + str(s["accepted"]))
        print("  Rejected:           " + str(s["rejected"]))
        print("  Pending:            " + str(s["pending"]))
        print("  Audit log entries:  " + str(s["audit_log_entries"]))
        print("  Last change:        " + str(s.get("last_change","Never")))
        print("  Override file:      " + str(Path("data_output/manual_overrides.json").exists()))
        print("  Audit log file:     " + str(Path("data_output/override_audit_log.json").exists()))
        results["overrides"] = "PASS"
    except Exception as e:
        print("ERROR: " + str(e))
        results["overrides"] = "ERROR"

    sep("PRIORITY 4 - ALL-TIME PERIOD LOGIC")
    try:
        from common_period_engine import compute_alltime_metrics
        a = compute_alltime_metrics(use_common_period=True)
        print("  Period:          " + a["period_label"])
        print("  Common Start:    " + str(a["common_start"]))
        print("  Common End:      " + str(a["common_end"]))
        print("  Common Signups:  " + str(a["signups"]))
        print("  Common Uploads:  " + str(a["uploads"]))
        print("  Common Paid:     " + str(a["paid"]))
        print("  S->U Rate:       " + str(a["signup_to_upload"]) + "%")
        print("  S->P Rate:       " + str(a["signup_to_paid"]) + "%")
        print("  Full DB Signups: " + str(a["full_db_signups"]) + " from " + str(a["coverage"]["signups"]["start"]))
        print("  Full DB Uploads: " + str(a["full_db_uploads"]) + " from " + str(a["coverage"]["uploads"]["start"]))
        print("  Full DB Paid:    " + str(a["full_db_paid"])    + " from " + str(a["coverage"]["paid"]["start"]))
        print("  WARNING: Full DB conversion rates INVALID due to coverage gap")
        results["alltime_logic"] = "PASS"
        results["_alltime"] = a
    except Exception as e:
        print("ERROR: " + str(e))
        results["alltime_logic"] = "ERROR"

    sep("PRIORITY 5 - KPI VALIDATION RULES")
    try:
        from validation_engine import validate_kpi_metrics
        a   = results.get("_alltime", {})
        val = validate_kpi_metrics(
            signups=a.get("full_db_signups", 3032),
            uploads=a.get("full_db_uploads", 101),
            paid=a.get("full_db_paid",       432),
        )
        print("  " + val.get_summary())
        for p in val.passed:
            print("  PASS: " + p["rule"] + ": " + p["message"])
        for w in val.warnings:
            print("  WARN: " + w["rule"] + ": " + w["message"])
        for f in val.failures:
            print("  FAIL: " + f["rule"] + ": " + f["message"])
        results["kpi_validation"] = "PASS" if val.is_valid else "FAIL"
    except Exception as e:
        print("ERROR: " + str(e))
        results["kpi_validation"] = "ERROR"

    sep("PRIORITY 8 - CHURN AND AVG SUBSCRIPTION")
    try:
        from churn_calculator import get_churn_display
        churn = get_churn_display()
        print("  Churn Rate:       " + churn["churn_rate_display"])
        print("  Churn Reason:     " + churn["churn_reason"])
        print("  Avg Subscription: " + churn["avg_subscription_display"])
        print("  Avg Sub Reason:   " + churn["avg_subscription_reason"])
        results["churn"] = "PASS"
    except Exception as e:
        print("ERROR: " + str(e))
        results["churn"] = "ERROR"

    sep("PRIORITY 9 - SUPABASE MIGRATION")
    try:
        from supabase_migration_plan import generate_migration_files
        generate_migration_files()
        results["migration"] = "FILES_GENERATED"
    except Exception as e:
        print("ERROR: " + str(e))
        results["migration"] = "ERROR"

    sep("FINAL AUDIT SUMMARY")
    for k, v in results.items():
        if k.startswith("_"):
            continue
        if v in ("PASS","FILES_GENERATED"):
            icon = "PASS"
        elif v == "WARN":
            icon = "WARN"
        else:
            icon = "FAIL"
        print("  [" + icon + "] " + k + ": " + v)

    master = {
        "timestamp": datetime.utcnow().isoformat(),
        "results":   {k: v for k, v in results.items() if not k.startswith("_")},
    }
    out = AUDIT_DIR / "master_audit_latest.json"
    out.write_text(json.dumps(master, indent=2, default=str))
    print("\nReport saved: " + str(out))
    sep()

if __name__ == "__main__":
    main()
