# This file is just for local verification - not deployed
# Confirms what secrets are needed in Streamlit Cloud
secrets_needed = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_KEY", 
    "MASTER_SHEET_URL",
    "GA4_PROPERTY_ID",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
]
print("Required secrets in Streamlit Cloud:")
for s in secrets_needed:
    print(f"  {s}")
