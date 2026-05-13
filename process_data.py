"""
process_data.py
SOURCE:  Google Sheets Raw_FREE, Raw_FIRST_UPLOAD, Raw_STRIPE
OUTPUT:  Google Sheets Verified_FREE, Verified_FIRST_UPLOAD, Verified_STRIPE
FALLBACK: CSV only if Sheets read/write fails
"""
import csv
import json
import re
from pathlib import Path
from datetime import datetime

from sheets_writer import read_tab_data, write_tab_data

DATA_DIR   = Path("data_output")
DATA_DIR.mkdir(exist_ok=True)
PROCESS_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [ProcessData] {msg}", flush=True)


DISPOSABLE = {
    "mailinator.com","guerrillamail.com","tempmail.com","throwaway.email",
    "yopmail.com","sharklasers.com","grr.la","spam4.me","trashmail.com",
    "maildrop.cc","dispostable.com","fakeinbox.com","trashmail.me",
    "discard.email","mailnesia.com","tempr.email","0-mail.com","0815.ru",
}
INTERNAL = {"eagle3dstreaming.com","eagle3d.com"}
SUSPICIOUS = [
    r'^test\d*@', r'^demo\d*@', r'^fake\d*@', r'^sample\d*@',
    r'^noreply@', r'^no-reply@', r'^donotreply@',
]


def extract_email(row: dict) -> str:
    for k in ("Email","email","EMAIL","Email Address",
              "__email_normalized__"):
        if k in row and row[k] and "@" in str(row[k]):
            return str(row[k]).strip()
    for v in row.values():
        if isinstance(v,str) and "@" in v and "." in v and len(v)<200:
            return v.strip()
    return ""


def validate_email(email: str) -> tuple:
    if not email or "@" not in email:
        return False, "no_email"
    e = email.strip().lower()
    parts = e.split("@")
    if len(parts) != 2:
        return False, "malformed"
    local, domain = parts
    if not local or not domain or "." not in domain:
        return False, "missing_parts"
    if len(e) > 254 or len(local) > 64:
        return False, "too_long"
    if domain in DISPOSABLE:
        return False, "disposable"
    if domain in INTERNAL:
        return False, "internal"
    for pat in SUSPICIOUS:
        if re.match(pat, e):
            return False, "suspicious"
    if not re.match(
        r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$', e
    ):
        return False, "invalid_format"
    try:
        from email_validator import validate_email as lib_val
        lib_val(e, check_deliverability=False)
    except ImportError:
        pass
    except Exception as ex:
        return False, f"lib:{ex}"
    return True, "ok"


def ml_score(row: dict) -> float:
    score  = 0.5
    email  = extract_email(row).lower()
    domain = email.split("@")[-1] if "@" in email else ""
    local  = email.split("@")[0]  if "@" in email else ""
    PRO = {
        "gmail.com","yahoo.com","hotmail.com","outlook.com",
        "icloud.com","protonmail.com","me.com","live.com",
    }
    if domain in PRO: score += 0.1
    if re.match(r'^[a-zA-Z]+[\.\-_]?[a-zA-Z]+$', local): score += 0.15
    if local.isdigit(): score -= 0.2
    for k, v in row.items():
        if ("created" in k.lower() or "date" in k.lower()) and v:
            score += 0.1
            break
    return round(min(max(score, 0.0), 1.0), 3)


def process_tab(tab_key: str) -> dict:
    log(f"{'─'*50}")
    log(f"Processing: {tab_key}")

    # Read from Sheets (primary) or CSV fallback
    source = read_tab_data(f"Raw_{tab_key}")
    if not source:
        log(f"{tab_key}: no source data")
        return {"tab":tab_key,"source":0,"verified":0,"skipped":0}

    log(f"{tab_key}: {len(source)} rows from source")

    verified = []
    skipped  = []
    seen     = set()

    for row in source:
        email = extract_email(row)
        if email:
            dom = email.split("@")[-1].lower()
            if dom in INTERNAL:
                skipped.append({**row,"__skip_reason__":"internal"})
                continue

        ok, reason = validate_email(email)
        if not ok:
            skipped.append({**row,"__skip_reason__":reason})
            continue

        key = email.strip().lower()
        if key in seen:
            skipped.append({**row,"__skip_reason__":"duplicate"})
            continue
        seen.add(key)

        verified.append({
            **row,
            "__email_normalized__":  key,
            "__ml_score__":          ml_score(row),
            "__processed_at__":      PROCESS_TS,
            "__validation_status__": "verified",
        })

    log(
        f"{tab_key}: {len(source)} -> "
        f"{len(verified)} verified, {len(skipped)} skipped"
    )

    if skipped:
        sp = DATA_DIR / f"Skipped_{tab_key}.csv"
        try:
            sf = sorted({k for r in skipped for k in r.keys()})
            with open(sp,"w",newline="",encoding="utf-8") as f:
                w = csv.DictWriter(f,fieldnames=sf,extrasaction="ignore")
                w.writeheader()
                w.writerows(skipped)
        except Exception as e:
            log(f"Skipped log error: {e}")

    if verified:
        # PRIMARY: Sheets. FALLBACK: CSV (inside write_tab_data)
        ok = write_tab_data(f"Verified_{tab_key}", verified)
        log(f"Verified_{tab_key}: Sheets={'OK' if ok else 'FAILED->CSV'}")

    return {
        "tab":      tab_key,
        "source":   len(source),
        "verified": len(verified),
        "skipped":  len(skipped),
    }


def main():
    log("=" * 60)
    log(f"PROCESS DATA - {PROCESS_TS}")
    log("=" * 60)

    results = []
    for tab in ("FREE","FIRST_UPLOAD","STRIPE"):
        r = process_tab(tab)
        results.append(r)

    log("=" * 60)
    log("SUMMARY")
    log("=" * 60)
    for r in results:
        log(
            f"  {r['tab']:20s}: "
            f"source={r['source']:4d}  "
            f"verified={r['verified']:4d}  "
            f"skipped={r['skipped']:4d}"
        )

    try:
        with open(DATA_DIR / "processing_report.json","w") as f:
            json.dump({
                "processed_at": PROCESS_TS,
                "results": results
            }, f, indent=2, default=str)
    except Exception as e:
        log(f"Report save: {e}")

    return results


if __name__ == "__main__":
    main()
