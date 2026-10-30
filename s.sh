#!/usr/bin/env bash
# Einmalige Einrichtung des Auto-Updates. Läuft in Oracle Cloud Shell.
ssh -o StrictHostKeyChecking=accept-new ubuntu@130.61.49.176 'cd ~/pionex-bot && git init -q -b main 2>/dev/null; git remote remove origin 2>/dev/null; git remote add origin https://github.com/paniert-sitzer0t/trend-bot.git && git fetch -q origin main && git reset -q --hard origin/main && chmod +x *.sh && echo "*/5 * * * * $HOME/pionex-bot/update.sh" | crontab - && echo FERTIG'
