#!/usr/bin/env bash
# Zeigt den aktuellen Dashboard-Link (inklusive Passwort)
cd "$(dirname "$0")"
T=$(grep ^DASH_TOKEN .env | cut -d= -f2)
U=""
for i in 1 2 3 4 5 6 7 8 9 10; do
  U=$(sudo journalctl -u pionex-tunnel --no-pager 2>/dev/null | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' | tail -1)
  [ -n "$U" ] && break
  sleep 3
done
echo
echo "Dashboard-Link (auf dem Handy öffnen):"
echo "$U/?t=$T"
echo
