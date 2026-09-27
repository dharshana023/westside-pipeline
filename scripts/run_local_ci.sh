#!/usr/bin/env bash
# Local CI runner — mirrors .github/workflows/ci.yml exactly, so you get the same
# pass/fail signal on your own machine before ever pushing.
#
# Usage:
#   ./scripts/run_local_ci.sh
#
# Exits non-zero on the first failing step (lint or tests), same as CI would.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== 1/3  Installing dependencies =="
python3 -m pip install -q --upgrade pip
python3 -m pip install -q -r requirements-dev.txt

echo "== 2/3  Lint (ruff) =="
python3 -m ruff check src/ tests/

echo "== 3/3  Unit tests (pytest, with coverage) =="
python3 -m pytest tests/ -v --cov=pipeline --cov-report=term-missing

echo ""
echo "All good - local CI passed. Safe to push."
