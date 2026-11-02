import os, threading
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
import uvicorn
import bot, json, time, strats

engine = bot.Engine()
fwd = strats.Forward(start=bot.START_BALANCE)
BT = {"running": False, "progress": ""}


def _frac(m):
    """Fortschritt 0..1 aus der Statusmeldung."""
    import re
    x = re.search(r"(\d+)/(\d+)", m or "")
    p = int(x.group(1)) / max(int(x.group(2)), 1) if x else 0.0
    if m.startswith("Tageskerzen"):
        return 0.02 + 0.18 * p
    if m.startswith("Lade Meme"):
        return 0.2
    if m.startswith("Stundenkerzen"):
        return 0.22 + 0.38 * p
    if m.startswith("Rechne"):
        return 0.6 + 0.2 * p
    if m.startswith("Suche"):
        return 0.8 + 0.2 * p
    return 0.01


def run_bt():
    if BT["running"]:
        return False
    BT.update(running=True, started=time.time(), frac=0.0, progress="Startet ...")

    def prog(m):
        BT.update(progress=m, frac=max(BT.get("frac", 0.0), _frac(m)))

    def job():
        t0 = time.time()
        try:
            r = strats.run_and_save("backtest.json", progress=prog)
            r["duration"] = time.time() - t0
            with open("backtest.json", "w") as f:
                json.dump(r, f)
        finally:
            BT.update(running=False, progress="", frac=0.0)
    threading.Thread(target=job, daemon=True).start()
    return True


def bt_eta():
    """Geschätzte Restdauer in Minuten (aus Fortschritt und Dauer des letzten Laufs)."""
    if not BT["running"]:
        return None
    el = time.time() - BT.get("started", time.time())
    fr = BT.get("frac", 0.0)
    last = (load_bt() or {}).get("duration")
    if fr >= 0.25:
        total = el / fr
    elif last:
        total = max(last, el / max(fr, 0.01) if fr > 0.05 else last)
    else:
        total = 45 * 60
    return dict(eta=max(1, round((total - el) / 60)), elapsed=round(el / 60), pct=round(fr * 100))


def load_bt():
    try:
        return json.load(open("backtest.json"))
    except Exception:
        return None
app = FastAPI()
TOKEN = bot.env("DASH_TOKEN", "")


def auth(t):
    if TOKEN and t != TOKEN:
        raise HTTPException(401, "Token falsch")


@app.get("/")
def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))


@app.get("/api/state")
def state(x_token: str = Header("")):
    auth(x_token)
    return engine.snapshot()


@app.post("/api/close/{pid}")
def close(pid: int, x_token: str = Header("")):
    auth(x_token)
    return {"ok": engine.close_position(pid)}


@app.post("/api/close_all")
def close_all(x_token: str = Header("")):
    auth(x_token)
    return {"closed": engine.close_all()}


@app.post("/api/pause/{flag}")
def pause(flag: int, x_token: str = Header("")):
    auth(x_token)
    engine.put("paused", 1 if flag else 0)
    engine.log("Bot pausiert (keine Rotation)" if flag else "Bot läuft wieder")
    return {"paused": bool(flag)}


@app.post("/api/rotate")
def rotate(x_token: str = Header("")):
    auth(x_token)
    return {"ok": engine.rotate_now()}


@app.get("/api/compare")
def compare(x_token: str = Header("")):
    auth(x_token)
    o = engine.snapshot()
    try:
        since = engine.db.execute("SELECT MIN(ts) FROM equity").fetchone()[0]
    except Exception:
        since = None
    try:
        rows_ = engine.db.execute("SELECT opened, closed FROM positions ORDER BY opened").fetchall()
        ivs = []
        for a, b_ in rows_:
            b_ = b_ or time.time()
            if ivs and a <= ivs[-1][1]:
                ivs[-1][1] = max(ivs[-1][1], b_)
            else:
                ivs.append([a, b_])
    except Exception:
        ivs = None
    orig = dict(key="ORIG", since=since, investiert=ivs, name="Original: Top-Mover mit 8-h-Rotation (Hauptbot)", curve=o["curve"], equity=o["equity"],
                pnl=o["total_pnl"], pct=o["total_pnl"] / o["start"] * 100, rebalances=o["trades"], costs=0,
                positions=[dict(symbol=p["symbol"], side=p["side"], value=p["notional"]) for p in o["open"]])
    return {"forward": fwd.snapshot() + [orig], "backtest": load_bt(), "running": BT["running"], "progress": BT["progress"], "eta": bt_eta(),
            "start": bot.START_BALANCE}


@app.get("/api/status/token")
def status_token_get(x_token: str = Header("")):
    auth(x_token)
    import status
    return {"set": status.has_token()}


@app.post("/api/status/token")
def status_token_set(body: dict, x_token: str = Header("")):
    auth(x_token)
    import status
    try:
        status.set_token(str(body.get("token", "")).strip())
    except Exception as e:
        raise HTTPException(400, str(e))
    status._force[0] = True
    return {"set": True}


@app.post("/api/backtest/run")
def backtest_run(x_token: str = Header("")):
    auth(x_token)
    return {"started": run_bt()}


if __name__ == "__main__":
    threading.Thread(target=engine.run, daemon=True).start()
    threading.Thread(target=fwd.run, daemon=True).start()
    import status
    threading.Thread(target=status.run, args=(lambda: compare(TOKEN),), daemon=True).start()
    def bt_scheduler():
        """Backtest automatisch zweimal täglich (ab 01:30 und 13:30 UTC) und beim Start, wenn er fehlt oder alt ist."""
        time.sleep(600)                                   # nach dem Start erst 10 Minuten warten
        while True:
            try:
                bt = load_bt()
                ts = (bt or {}).get("ts", 0)
                now = time.time()
                slot = now - ((now - 5400) % 43200)          # letzter Zeitpunkt 01:30 oder 13:30 UTC
                if not BT["running"] and (not bt or "error" in bt or "hourly" not in bt or ts < slot):
                    run_bt()
            except Exception as e:
                print("Backtest-Planer Fehler:", e, flush=True)
            time.sleep(300)
    threading.Thread(target=bt_scheduler, daemon=True).start()
    uvicorn.run(app, host=bot.env("HOST", "127.0.0.1"), port=int(bot.env("PORT", "8000")), log_level="warning")
