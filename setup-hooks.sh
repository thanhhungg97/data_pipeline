#!/bin/bash
# Setup git hooks for the project

echo "🔧 Setting up git hooks..."

# Configure git to use our hooks directory
git config core.hooksPath .githooks

# Make hooks executable
chmod +x .githooks/*

echo "✅ Git hooks installed!"
echo ""
echo "Hooks enabled:"
echo "  - pre-push: Runs ruff lint + format check before pushing"
echo ""
echo "To disable: git config --unset core.hooksPath"

