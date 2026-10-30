#!/usr/bin/env bash
# Holt neue Versionen aus GitHub und startet den Bot neu, wenn sich etwas geändert hat.
cd "$(dirname "$0")" || exit 1
git fetch -q origin main || exit 0
[ "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)" ] && exit 0
git reset -q --hard origin/main
venv/bin/pip install -q -r requirements.txt
chmod +x *.sh
sudo systemctl restart pionex-bot
echo "$(date) aktualisiert auf $(git rev-parse --short HEAD)" >> update.log
