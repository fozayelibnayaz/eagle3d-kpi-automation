"""
Email validation engine.
- Syntax check (RFC compliant)
- Disposable domain check (public list + custom blocklist + heuristics)
- MX record check
- Free vs business classification
- Email normalization
"""
import re
import time
import requests
from email_validator import validate_email, EmailNotValidError
import dns.resolver

from config import (
    DATA_DIR, CHECK_MX_RECORDS, MX_TIMEOUT_SECONDS,
    DISPOSABLE_DOMAINS_URL, CUSTOM_DISPOSABLE_DOMAINS,
    SUSPICIOUS_LOCAL_PATTERNS,
)

DISPOSABLE_CACHE_FILE = DATA_DIR / "disposable_domains.txt"
DISPOSABLE_CACHE_AGE_DAYS = 7

FREE_PROVIDERS = {
    "gmail.com", "yahoo.com", "yahoo.co.uk", "yahoo.co.in",
    "hotmail.com", "outlook.com", "live.com", "msn.com",
    "icloud.com", "me.com", "mac.com",
    "aol.com", "protonmail.com", "proton.me", "pm.me",
    "yandex.com", "yandex.ru", "mail.ru",
    "gmx.com", "gmx.de", "web.de",
    "zoho.com", "fastmail.com", "tutanota.com",
    "qq.com", "163.com", "126.com", "sina.com",
    "naver.com", "daum.net",
}

_disposable_set = None
_mx_cache = {}
_compiled_suspicious = [re.compile(p, re.IGNORECASE) for p in SUSPICIOUS_LOCAL_PATTERNS]


def load_disposable_domains():
    global _disposable_set
    if _disposable_set is not None:
        return _disposable_set

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    needs_refresh = True
    if DISPOSABLE_CACHE_FILE.exists():
        age_days = (time.time() - DISPOSABLE_CACHE_FILE.stat().st_mtime) / 86400
        if age_days < DISPOSABLE_CACHE_AGE_DAYS:
            needs_refresh = False

    if needs_refresh:
        try:
            print("   [Validator] Refreshing disposable domains list...")
            r = requests.get(DISPOSABLE_DOMAINS_URL, timeout=15)
            r.raise_for_status()
            DISPOSABLE_CACHE_FILE.write_text(r.text)
        except Exception as e:
            print(f"   [Validator] Could not refresh disposable list ({e}). Using cache.")

    if DISPOSABLE_CACHE_FILE.exists():
        lines = DISPOSABLE_CACHE_FILE.read_text().splitlines()
        public_set = {ln.strip().lower() for ln in lines if ln.strip() and not ln.startswith("#")}
    else:
        public_set = set()

    # Merge with custom blocklist
    custom_set = {d.strip().lower() for d in CUSTOM_DISPOSABLE_DOMAINS if d.strip()}
    _disposable_set = public_set | custom_set

    print(f"   [Validator] Loaded {len(public_set)} public + {len(custom_set)} custom = {len(_disposable_set)} disposable domains.")
    return _disposable_set


def normalize_email(email):
    if not email or "@" not in email:
        return (email or "").strip().lower()
    email = email.strip().lower()
    local, domain = email.rsplit("@", 1)
    if "+" in local:
        local = local.split("+", 1)[0]
    if domain in ("gmail.com", "googlemail.com"):
        local = local.replace(".", "")
        domain = "gmail.com"
    return f"{local}@{domain}"


def domain_has_mx(domain):
    if domain in _mx_cache:
        return _mx_cache[domain]
    result = False
    try:
        resolver = dns.resolver.Resolver()
        resolver.timeout = MX_TIMEOUT_SECONDS
        resolver.lifetime = MX_TIMEOUT_SECONDS
        try:
            answers = resolver.resolve(domain, "MX")
            if len(answers) > 0:
                result = True
        except dns.resolver.NoAnswer:
            try:
                resolver.resolve(domain, "A")
                result = True
            except Exception:
                result = False
        except Exception:
            result = False
    except Exception:
        result = False
    _mx_cache[domain] = result
    return result


def looks_suspicious_local(local):
    """Returns True if the local part matches a fake-looking pattern."""
    for pat in _compiled_suspicious:
        if pat.match(local):
            return True
    return False


def looks_suspicious_domain(domain):
    """Heuristics for fishy domains."""
    # Very long random-looking subdomain like "xkjhsdf123.com"
    if len(domain) > 30:
        return True
    # Domain with no vowels in main part (e.g. "xkpwq.com")
    main = domain.split(".")[0]
    if len(main) >= 5:
        vowels = sum(1 for c in main if c in "aeiouy")
        if vowels == 0:
            return True
    return False


def validate_one(email):
    result = {
        "original": email,
        "normalized": "",
        "domain": "",
        "valid_syntax": False,
        "is_disposable": False,
        "is_free_provider": False,
        "has_mx": False,
        "is_suspicious": False,
        "verdict": "INVALID",
        "reason": "",
    }

    if not email or not isinstance(email, str):
        result["reason"] = "Empty"
        return result

    email = email.strip()
    if not email:
        result["reason"] = "Empty"
        return result

    try:
        v = validate_email(email, check_deliverability=False)
        normalized_email = v.normalized
        result["valid_syntax"] = True
    except EmailNotValidError as e:
        result["reason"] = f"Syntax: {str(e)[:60]}"
        return result

    norm = normalize_email(normalized_email)
    result["normalized"] = norm
    domain = norm.split("@", 1)[1]
    local = norm.split("@", 1)[0]
    result["domain"] = domain

    # Disposable check (public + custom)
    disposable_set = load_disposable_domains()
    if domain in disposable_set:
        result["is_disposable"] = True
        result["verdict"] = "DISPOSABLE"
        result["reason"] = f"Disposable domain: {domain}"
        return result

    # Heuristic suspicious patterns
    if looks_suspicious_local(local):
        result["is_suspicious"] = True
        result["verdict"] = "SUSPICIOUS"
        result["reason"] = f"Suspicious local pattern: {local}"
        return result

    if looks_suspicious_domain(domain):
        result["is_suspicious"] = True
        result["verdict"] = "SUSPICIOUS"
        result["reason"] = f"Suspicious domain pattern: {domain}"
        return result

    if domain in FREE_PROVIDERS:
        result["is_free_provider"] = True

    if CHECK_MX_RECORDS:
        if domain_has_mx(domain):
            result["has_mx"] = True
        else:
            result["verdict"] = "FAKE_DOMAIN"
            result["reason"] = f"No MX record: {domain}"
            return result
    else:
        result["has_mx"] = True

    result["verdict"] = "VALID"
    result["reason"] = "OK"
    return result


def validate_batch(emails):
    load_disposable_domains()
    results = []
    total = len(emails)
    for i, email in enumerate(emails, 1):
        if i % 25 == 0 or i == total:
            print(f"   [Validator] Processed {i}/{total}...")
        results.append(validate_one(email))
    return results
