import os, threading
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
import uvicorn
import bot, json, time, strats

engine = bot.Engine()
fwd = strats.Forward(start=bot.START_BALANCE)
BT = {"running": False, "progress": ""}


def run_bt():
    if BT["running"]:
        return False
    BT["running"] = True

    def job():
        try:
            strats.run_and_save("backtest.json", progress=lambda m: BT.update(progress=m))
        finally:
            BT["running"] = False
            BT["progress"] = ""
    threading.Thread(target=job, daemon=True).start()
    return True


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
    orig = dict(key="ORIG", name="Original: Top-Mover mit 8-h-Rotation (Hauptbot)", curve=o["curve"], equity=o["equity"],
                pnl=o["total_pnl"], pct=o["total_pnl"] / o["start"] * 100, rebalances=o["trades"], costs=0,
                positions=[dict(symbol=p["symbol"], side=p["side"], value=p["notional"]) for p in o["open"]])
    return {"forward": fwd.snapshot() + [orig], "backtest": load_bt(), "running": BT["running"], "progress": BT["progress"],
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
