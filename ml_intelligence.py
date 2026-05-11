"""
MODULE 5: ML Intelligence Layer
Trains a Gradient Boosting model on your accurate Google Sheets dataset
to evaluate legitimacy probability and assign lead conversion tiers.
"""
import joblib
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from sklearn.ensemble import GradientBoostingClassifier
from config import GOOGLE_CREDS_FILE, ACCURATE_DATA_SHEET_URL, MODEL_DIR, MODEL_FILE

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
MODEL_DIR.mkdir(parents=True, exist_ok=True)

FREE_DOMAINS = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com", "aol.com"}

def extract_features(email: str, source: str = "") -> dict:
    email = (email or "").strip().lower()
    source = (source or "").strip().lower()
    if "@" not in email:
        return None
    local, domain = email.rsplit("@", 1)
    
    return {
        "loc_len": len(local),
        "dom_len": len(domain),
        "digits": sum(c.isdigit() for c in local),
        "is_free": int(domain in FREE_DOMAINS),
        "has_dot": int("." in local),
        "src_len": len(source),
        "src_google": int("google" in source),
    }

def train_model():
    print("   [ML Intelligence] Extracting ground-truth training vectors from Sheets...")
    creds = Credentials.from_service_account_file(str(GOOGLE_CREDS_FILE), scopes=SCOPES)
    gc = gspread.authorize(creds)
    try:
        sh = gc.open_by_url(ACCURATE_DATA_SHEET_URL)
    except Exception as e:
        print(f"   [ML Intelligence] ❌ Training load failed: {e}")
        return

    vectors = []
    for ws in sh.worksheets():
        try:
            rows = ws.get_all_values()
            if len(rows) < 2:
                continue
            headers = [h.strip().lower() for h in rows[0]]
            e_idx = next((i for i, h in enumerate(headers) if "email" in h), -1)
            s_idx = next((i for i, h in enumerate(headers) if "source" in h), -1)
            
            if e_idx != -1:
                for r in rows[1:]:
                    if e_idx < len(r) and "@" in r[e_idx]:
                        src = r[s_idx].strip() if s_idx != -1 and s_idx < len(r) else ""
                        vectors.append({"email": r[e_idx].strip(), "src": src, "label": 1})
        except Exception:
            pass

    negatives = [
        {"email": f"asdf{i}@tempmail.com", "src": "test", "label": 0} for i in range(len(vectors)//2 + 5)
    ] + [
        {"email": f"testuser{i}@mailinator.com", "src": "", "label": 0} for i in range(len(vectors)//2 + 5)
    ]
    
    dataset = vectors + negatives
    X_list, y_list = [], []
    for item in dataset:
        feats = extract_features(item["email"], item["src"])
        if feats:
            X_list.append(list(feats.values()))
            y_list.append(item["label"])

    X, y = np.array(X_list), np.array(y_list)
    if len(np.unique(y)) < 2:
        print("   [ML Intelligence] ⚠️ Insufficient target class variance. Skipping build.")
        return

    clf = GradientBoostingClassifier(n_estimators=100, random_state=42)
    clf.fit(X, y)
    
    joblib.dump({"model": clf, "features": list(extract_features("a@b.com").keys())}, MODEL_FILE)
    print(f"   [ML Intelligence] ✅ Evaluator model committed to: {MODEL_FILE}")

_model_cache = None
def predict_scores(email: str, source: str = "") -> dict:
    global _model_cache
    if _model_cache is None:
        if not MODEL_FILE.exists():
            return {"score": 1.0, "tier": "DEFAULT"}
        _model_cache = joblib.load(MODEL_FILE)
        
    feats = extract_features(email, source)
    if not feats:
        return {"score": 0.0, "tier": "LOW"}
        
    X = np.array([list(feats.values())])
    prob = _model_cache["model"].predict_proba(X)[0][1]
    tier = "HIGH" if prob >= 0.7 else ("MEDIUM" if prob >= 0.4 else "LOW")
    return {"score": round(float(prob), 3), "tier": tier}

if __name__ == "__main__":
    train_model()