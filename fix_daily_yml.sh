#!/bin/bash
# Fix: Delete daily.yml from GitHub remote
# Run this from your eagle3d-kpi-automation directory

set -e
echo "🔧 Deleting daily.yml from GitHub..."

git fetch origin main

# Check if daily.yml exists on remote
if git ls-tree -r origin/main --name-only | grep -q ".github/workflows/daily.yml"; then
    echo "✅ Found daily.yml on remote — deleting it..."
    
    # Checkout the file from remote so we can remove it
    git checkout origin/main -- .github/workflows/daily.yml 2>/dev/null || true
    
    # Remove it from git tracking
    git rm .github/workflows/daily.yml
    
    # Commit the deletion
    git commit -m "Remove old daily.yml workflow (keeping daily_pipeline.yml only)"
    
    # Push the deletion
    git push origin main
    
    echo "✅ daily.yml deleted from GitHub!"
else
    echo "✅ daily.yml not found on remote (already clean)"
fi

echo ""
echo "🔍 Verifying..."
git fetch origin main
echo "Workflow files on GitHub:"
git ls-tree -r origin/main --name-only | grep ".github/workflows/"

echo ""
echo "✅ Done! Only daily_pipeline.yml should remain."
