#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# Eagle 3D Streaming — KPI Dashboard v4.0 Update Script
# ═══════════════════════════════════════════════════════════════
# Run this in your eagle3d-kpi-automation directory on your MacBook:
#   cd ~/eagle3d-kpi-automation   (or wherever your project is)
#   chmod +x update_v4.sh
#   ./update_v4.sh
# ═══════════════════════════════════════════════════════════════

set -e

echo "🦅 Eagle 3D Streaming — KPI Dashboard v4.0 Update"
echo "=================================================="
echo ""

# Backup current app.py
if [ -f app.py ]; then
    cp app.py app.py.backup_$(date +%Y%m%d_%H%M%S)
    echo "✅ Backed up current app.py"
fi

# The new app.py is encoded in base64 below.
# It was generated from the Arena workspace and contains ALL fixes:
# - ValueError crash fixed (fmt_val for all values)
# - Light mode CSS fully working
# - Dark mode CSS fully working
# - Responsive UI for all devices
# - use_container_width deprecation handled
# - CORS/XSRF config conflict fixed
# - All 9 pages working

echo "📝 Decoding new app.py..."

# Read the base64 data from the separate file
if [ -f update_app_b64.txt ]; then
    base64 -d update_app_b64.txt > app.py
    echo "✅ app.py updated from local base64 file"
else
    echo "❌ update_app_b64.txt not found!"
    echo "   Please download both update_v4.sh AND update_app_b64.txt"
    echo "   from the Arena workspace and place them in this directory."
    exit 1
fi

# Fix config.toml
echo "📝 Fixing .streamlit/config.toml..."
cat > .streamlit/config.toml << 'CONFIGEOF'
[server]
headless = true
maxUploadSize = 200

[browser]
gatherUsageStats = false

[theme]
base = "dark"
primaryColor = "#00D4FF"
backgroundColor = "#0B1120"
secondaryBackgroundColor = "#162032"
textColor = "#E8EDF5"
CONFIGEOF
echo "✅ config.toml updated (removed CORS conflict)"



# Git setup
echo ""
echo "📋 Git status:"
git config user.email "eagle3d@automation.dev" 2>/dev/null || true
git config user.name "Eagle3D Automation" 2>/dev/null || true
git add app.py .streamlit/config.toml
git status --short

echo ""
echo "══════════════════════════════════════════════════════"
echo "✅ UPDATE COMPLETE!"
echo "══════════════════════════════════════════════════════"
echo ""
echo "Next steps:"
echo "  1. Test locally:"
echo "     streamlit run app.py --server.port 8501"
echo ""
echo "  2. Push to GitHub (for Streamlit Cloud):"
echo "     git add -A && git commit -m 'v4.0: complete rewrite'"
echo "     git push origin main"
echo ""
echo "  3. Verify Streamlit Cloud picks up the changes"
echo ""
echo "🦅 Eagle 3D Streaming KPI v4.0 — Ready!"
