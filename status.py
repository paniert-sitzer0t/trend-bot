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
                  for f in ("total", "cagr", "maxdd", "sharpe", "liquidated", "best_year", "stops", "rebalances", "years") if f in v}
    return dict(erzeugt=time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()), paper_ranking=fw,
                backtest=dict(start=bt.get("start"), tage=bt.get("days"), stunden=bt.get("hourly"), funding_echt=bt.get("funding_real"),
                              notiz=bt.get("note"), ergebnisse=res, fehler=bt.get("error")),
                backtest_laeuft=payload.get("running"), fortschritt=payload.get("progress"))


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
    time.sleep(60)
    while True:
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
