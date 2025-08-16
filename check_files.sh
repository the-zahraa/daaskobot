#!/usr/bin/env bash
set -e
required=(
  "backend/requirements.txt"
  "backend/app/main.py"
  "backend/app/bot_worker.py"
  "backend/app/services/db.py"
  "backend/app/handlers/start.py"
  "backend/app/models.py"
  "backend/app/services/payments.py"
  "db/schema.sql"
  "frontend-miniapp/package.json"
  "frontend-miniapp/index.html"
  "frontend-miniapp/vite.config.js"
  "frontend-miniapp/src/main.jsx"
  "frontend-miniapp/src/App.jsx"
  "frontend-miniapp/src/utils/telegram.js"
  ".gitignore"
  "backend/.env"
)

missing=()
for f in "${required[@]}"; do
  if [[ ! -f "$f" ]]; then
    missing+=("$f")
  fi
done

if [ ${#missing[@]} -eq 0 ]; then
  echo "✔ All expected files are present."
  exit 0
else
  echo "✖ Missing files:"
  for m in "${missing[@]}"; do echo " - $m"; done
  exit 2
fi
