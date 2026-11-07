"""Schreibt alle 30 Minuten eine Zusammenfassung (status.json) in den Branch „status“ des GitHub-Repos,
damit Claude den Stand lesen kann. Enthält keine Passwörter und keine Schlüssel. Ohne GH_TOKEN passiert nichts."""
import base64, json, os, time
import requests
import bot

REPO = bot.env("GH_REPO", "paniert-sitzer0t/trend-bot")
TOKEN = bot.env("GH_TOKEN", "")
BRANCH = "status"
API = "https://api.github.com/repos/" + REPO
H = {"Authorization": "Bearer " + TOKEN, "Accept": "application/vnd.github+json"}


def tunnel_url():
    """Aktuelle Dashboard-Adresse (ohne Passwort). Ändert sich bei jedem Tunnel-Neustart."""
    import re, subprocess, json as _j, urllib.request
    try:
        import bot
        fest = bot.env("DASH_URL", "").strip().rstrip("/")      # feste Adresse (z. B. Tailscale Funnel), ändert sich nie
        if fest.startswith("https://"):
            return fest
    except Exception:
        pass
    # 1) cloudflared meldet die Quick-Tunnel-Adresse über seinen lokalen Metrics-Server
    for port in range(20241, 20246):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/quicktunnel", timeout=2) as r:
                h = _j.loads(r.read().decode()).get("hostname")
                if h:
                    return "https://" + h
        except Exception:
            pass
    # 2) Tunnel-Protokoll seit dem letzten Systemstart
    for cmd in (["sudo", "-n", "journalctl", "-b", "-u", "pionex-tunnel", "--no-pager"],
                ["journalctl", "-b", "-u", "pionex-tunnel", "--no-pager"]):
        try:
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=20).stdout
            urls = re.findall(r"https://[a-z0-9-]+\.trycloudflare\.com", out)
            if urls:
                return urls[-1]
        except Exception:
            pass
    return None


def tunnel_ok(u):
    """Prüft vom Server aus, ob die öffentliche Adresse erreichbar ist. 4xx zählt als erreichbar."""
    import urllib.request, urllib.error
    try:
        urllib.request.urlopen(urllib.request.Request(u + "/", headers={"User-Agent": "trend-bot"}), timeout=15)
        return True
    except urllib.error.HTTPError as e:
        return e.code < 500
    except Exception:
        return False


def restart_tunnel():
    import subprocess
    try:
        r = subprocess.run(["sudo", "-n", "systemctl", "restart", "pionex-tunnel"],
                           capture_output=True, text=True, timeout=60)
        _tun["neustarts"] += 1
        _tun["letzter_neustart"] = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        _tun["fehler"] = (r.stderr or "").strip()[:200] or None
    except Exception as e:
        _tun["fehler"] = str(e)[:200]


def _opt(o):
    if not o or o.get("fehler"):
        return o
    q = {k: v for k, v in o.items() if k not in ("beste", "rueckblick")}
    q["beste"] = [{k: v for k, v in b.items() if k != "kurve"} for b in o.get("beste", [])]
    q["rueckblick"] = o.get("rueckblick")
    q["rueckblick_1x"] = o.get("rueckblick_1x")
    return q


def summary(payload):
    fw = []
    for x in payload["forward"]:
        fw.append(dict(key=x["key"], name=x["name"], pct=round(x["pct"], 2), equity=round(x["equity"], 2),
                       lev=x.get("lev", 1), liquidiert=x.get("liq"), stops=x.get("stops", 0), anpassungen=x.get("rebalances"),
                       kosten=round(x.get("costs", 0) or 0, 2),
                       positionen=[f'{p["side"]} {p["symbol"]} {p["value"]:.1f}' for p in x.get("positions", [])]))
    fw.sort(key=lambda r: r["pct"], reverse=True)
    bt = payload.get("backtest") or {}
    res = {}
    for k, v in (bt.get("results") or {}).items():
        res[k] = {f: (round(v[f], 1) if isinstance(v.get(f), float) else v.get(f))
                  for f in ("total", "cagr", "maxdd", "sharpe", "liquidated", "best_year", "stops", "rebalances", "years", "ausreisser") if f in v}
    return dict(erzeugt=time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()), dashboard_adresse=tunnel_url(), paper_ranking=fw,
                backtest=dict(start=bt.get("start"), tage=bt.get("days"), stunden=bt.get("hourly"), funding_echt=bt.get("funding_real"),
                              notiz=bt.get("note"), dauer_min=round((bt.get("duration") or 0) / 60), datenpruefung=bt.get("datenpruefung"), optimierung=_opt(bt.get("optimierung")), ergebnisse=res, fehler=bt.get("error")),
                backtest_laeuft=payload.get("running"), fortschritt=payload.get("progress"),
                server=payload.get("server"), letzter_fehler=payload.get("last_error"), tunnel=dict(_tun))


def has_token():
    return bool(TOKEN)


def publish(payload):
    r = requests.get(API + "/git/ref/heads/" + BRANCH, headers=H, timeout=20)
    if r.status_code == 404:
        sha = requests.get(API + "/git/ref/heads/main", headers=H, timeout=20).json()["object"]["sha"]
        requests.post(API + "/git/refs", headers=H, timeout=20, json={"ref": "refs/heads/" + BRANCH, "sha": sha}).raise_for_status()
    cur = requests.get(API + "/contents/status.json", headers=H, params={"ref": BRANCH}, timeout=20)
    body = {"message": "Status " + time.strftime("%H:%M", time.gmtime()), "branch": BRANCH,
            "content": base64.b64encode(json.dumps(summary(payload), ensure_ascii=False, indent=1).encode()).decode()}
    if cur.status_code == 200:
        body["sha"] = cur.json()["sha"]
    requests.put(API + "/contents/status.json", headers=H, json=body, timeout=30).raise_for_status()


def set_token(t):
    """Prüft den Token gegen das Repo, speichert ihn in .env und aktiviert die Veröffentlichung."""
    global TOKEN, H
    h = {"Authorization": "Bearer " + t, "Accept": "application/vnd.github+json"}
    r = requests.get(API, headers=h, timeout=20)
    if r.status_code != 200:
        raise ValueError("GitHub lehnt den Token ab (Status %d)" % r.status_code)
    if not (r.json().get("permissions") or {}).get("push"):
        raise ValueError("Der Token darf nicht ins Repo schreiben")
    lines = [l for l in open(".env", encoding="utf-8").read().splitlines() if not l.startswith("GH_TOKEN=")] if os.path.exists(".env") else []
    open(".env", "w", encoding="utf-8").write("\n".join(lines + ["GH_TOKEN=" + t]) + "\n")
    TOKEN, H = t, h
    return True


def run(get_payload, every=1800):
    last = 0.0
    lastcheck = 0.0
    time.sleep(60)
    while True:
        # Tunnel-Selbstheilung: alle 3 Min prüfen, nach 2 Fehlschlägen Tunnel neu starten (neue Adresse)
        if time.time() - lastcheck >= 180:
            lastcheck = time.time()
            u = tunnel_url()
            ok = bool(u) and tunnel_ok(u)
            _tun["ok"] = ok
            _tun["bad"] = 0 if ok else _tun["bad"] + 1
            if _tun["bad"] >= 2:
                _tun["bad"] = 0
                restart_tunnel()
                time.sleep(20)
                _link[0] = None
        if TOKEN:
            u = tunnel_url()
            if u and u != _link[0]:
                _force[0] = True              # Adresse neu oder geändert: sofort veröffentlichen
                _link[0] = u
        if TOKEN and (time.time() - last >= every or _force[0]):
            _force[0] = False
            try:
                publish(get_payload())
                last = time.time()
            except Exception as e:
                print("Status-Veröffentlichung Fehler:", e, flush=True)
                last = time.time() - every + 300
        time.sleep(15)


_force = [False]
_link = [None]
_tun = {"ok": None, "bad": 0, "neustarts": 0, "letzter_neustart": None, "fehler": None}
