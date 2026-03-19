#!/bin/bash
# OddsShopper — Start the backend API server
# Usage: ./start.sh

set -e

cd "$(dirname "$0")/backend"

echo "🚀 Starting OddsShopper Arbitrage API..."
echo ""
echo "  API:  http://localhost:8000"
echo "  Docs: http://localhost:8000/docs"
echo "  Arb:  open ../arb.html in your browser"
echo ""
echo "  → Tip: Add your Odds API key to backend/.env to get real-time data"
echo "  → Free key at: https://the-odds-api.com"
echo ""

python3 main.py
