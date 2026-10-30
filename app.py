import os, threading
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
import uvicorn
import bot

engine = bot.Engine()
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


if __name__ == "__main__":
    threading.Thread(target=engine.run, daemon=True).start()
    uvicorn.run(app, host=bot.env("HOST", "127.0.0.1"), port=int(bot.env("PORT", "8000")), log_level="warning")
