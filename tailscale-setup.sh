#!/usr/bin/env bash
# Richtet eine feste Dashboard-Adresse über Tailscale Funnel ein (einmalig ausführen: bash ~/pionex-bot/tailscale-setup.sh)
set -e
cd "$(dirname "$0")"
if ! command -v tailscale >/dev/null 2>&1; then
  echo "Installiere Tailscale ..."
  curl -fsSL https://tailscale.com/install.sh | sh
fi
echo
echo "Gleich erscheint ein Anmelde-Link. Öffne ihn im Browser und melde dich bei Tailscale an."
sudo tailscale up --hostname=trendbot
echo
echo "Schalte das Dashboard (Port 8000) öffentlich per HTTPS frei ..."
sudo tailscale funnel --bg 8000 || {
  echo
  echo "Funnel ist noch nicht freigeschaltet. Öffne den oben angezeigten Link (oder https://login.tailscale.com/admin/dns),"
  echo "aktiviere dort HTTPS-Zertifikate und bestätige die Funnel-Freigabe, danach dieses Skript erneut starten."
  exit 1
}
H=$(tailscale status --json | python3 -c "import json,sys; print(json.load(sys.stdin)['Self']['DNSName'].rstrip('.'))")
grep -v '^DASH_URL=' .env > .env.tmp || true
echo "DASH_URL=https://$H" >> .env.tmp
mv .env.tmp .env
chmod 600 .env
sudo systemctl restart pionex-bot
echo
echo "Fertig. Feste Adresse: https://$H"
echo "Link mit Passwort anzeigen: bash ~/pionex-bot/dashboard-link.sh"
