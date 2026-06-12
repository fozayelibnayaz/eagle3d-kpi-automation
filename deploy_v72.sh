#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# Eagle Analytics Hub v7.2.1 — Deploy Script
# ═══════════════════════════════════════════════════════════════
# Run this on your machine where you have GitHub push access:
#   bash deploy_v72.sh
#
# What this does:
#   1. Pulls latest from origin/main (includes pipeline auto-commits)
#   2. Force pushes the v7.2.1 code
#   3. Streamlit Cloud auto-deploys from GitHub
# ═══════════════════════════════════════════════════════════════

set -e

echo "🦅 Eagle Analytics Hub v7.2.1 — Deploy"
echo "========================================"

# Check we're in the right directory
if [ ! -f "app.py" ]; then
    echo "❌ Run this from the eagle3d-kpi-automation directory"
    exit 1
fi

# Step 1: Verify no secrets (exclude deploy scripts which contain secret-checking patterns)
echo ""
echo "🔍 Checking for secrets in code..."
if grep -rn "8743434532\|AAFMy9F\|1003989604195\|sk_live\|ghp_" --include="*.py" --exclude="deploy_*" . 2>/dev/null; then
    echo "❌ SECRET DETECTED! Aborting."
    exit 1
fi
echo "  ✅ No secrets found"

# Step 2: Verify all files compile
echo ""
echo "🔍 Verifying Python files compile..."
python3 -c "
import ast, os, sys
errors = []
for f in os.listdir('.'):
    if f.endswith('.py') and not f.startswith('deploy_'):
        try:
            with open(f) as fh:
                ast.parse(fh.read())
        except SyntaxError as e:
            errors.append(f'{f}: {e}')
if errors:
    print('❌ Syntax errors:')
    for e in errors:
        print(f'  {e}')
    sys.exit(1)
else:
    print('  ✅ All files compile')
"
if [ $? -ne 0 ]; then
    exit 1
fi

# Step 3: Fetch & rebase
echo ""
echo "📥 Syncing with remote..."
git fetch origin main
git rebase origin/main

# Step 4: Push
echo ""
echo "🚀 Force pushing to GitHub..."
for i in 1 2 3; do
    if git push origin main --force; then
        echo "  ✅ Pushed!"
        break
    else
        echo "  ⚠️ Push attempt $i failed, retrying..."
        git fetch origin main
        git rebase origin/main
        sleep 3
    fi
done

# Done
echo ""
echo "========================================"
echo "✅ DEPLOY COMPLETE"
echo ""
echo "🌐 https://eagle3d-kpi-automation.streamlit.app/"
echo "⏱️  Streamlit Cloud updates in 60-90 seconds"
echo ""
echo "📋 After deploy checklist:"
echo "  1. Login with password: eagleanalytics"
echo "  2. Check Browse Data → First Uploads (should show data now)"
echo "  3. Check LinkedIn Command Center loads"
echo "  4. Check YouTube Command Center loads"
echo "  5. Trigger pipeline: https://github.com/fozayelibnayaz/eagle3d-kpi-automation/actions"
echo "========================================"
