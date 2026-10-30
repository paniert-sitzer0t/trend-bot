"""Pionex Trend-Rotation-Bot. Long und Short (Short nur simuliert), Rotation alle 8 Stunden.
Aktuell nur Paper-Modus: Pionex erlaubt per API kein Shorten."""
import os, time, sqlite3, threading, datetime as dt
import requests


def load_env(path=".env"):
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"'))


load_env()


def env(k, d):
    return os.environ.get(k, d)


START_BALANCE = float(env("START_BALANCE", "77"))
MAX_POSITIONS = int(env("MAX_POSITIONS", "3"))
ROTATION_HOURS = int(env("ROTATION_HOURS", "8"))
MIN_VOLUME_USDT = float(env("MIN_VOLUME_USDT", "1000000"))   # 24h Umsatz, Liquiditätsfilter
CANDIDATES = int(env("CANDIDATES", "12"))                    # Top-Mover, die geprüft werden
MAX_ABS_MOVE = float(env("MAX_ABS_MOVE", "0.5"))             # >50 % in 24h: zu riskant (Pump/Crash)
STOP_LOSS = float(env("STOP_LOSS", "0.08"))                  # 8 % Notbremse je Position
INVEST_FRACTION = float(env("INVEST_FRACTION", "0.95"))
FEE = float(env("FEE", "0.0005"))             # 0,05 % pro Seite
SLIPPAGE = float(env("SLIPPAGE", "0.0003"))
MIN_NOTIONAL = 10.0
POLL_SECONDS = int(env("POLL_SECONDS", "15"))
BASE = "https://api.pionex.com"
SLOT = ROTATION_HOURS * 3600


def get(path, params=None):
    r = requests.get(BASE + path, params=params, timeout=15)
    r.raise_for_status()
    j = r.json()
    if not j.get("result"):
        raise RuntimeError(f"Pionex: {j.get('code')} {j.get('message')}")
    return j["data"]


def pct(a, b):
    return (a / b - 1) if b else 0.0


def daily(symbol):
    k = get("/api/v1/market/klines", {"symbol": symbol, "interval": "1D", "limit": 32})["klines"]
    k = sorted(k, key=lambda x: int(x["time"]))
    return [float(x["close"]) for x in k], [(float(x["high"]) - float(x["low"])) / float(x["close"]) for x in k]


SCHEMA = """
CREATE TABLE IF NOT EXISTS positions(
 id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, side TEXT, entry REAL, size REAL, opened REAL,
 status TEXT, exit REAL, closed REAL, pnl REAL, reason TEXT, chg24 REAL, chg7 REAL, chg30 REAL);
CREATE TABLE IF NOT EXISTS state(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS equity(ts REAL, eq REAL);
"""


class Engine:
    def __init__(self):
        self.lock = threading.RLock()
        self.db = sqlite3.connect("trend.db", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self.prices, self.ranking, self.logs = {}, [], []
        self.last_eq = 0.0
        if self.get("cash") is None:
            self.put("cash", START_BALANCE)
        if self.get("paused") is None:
            self.put("paused", 0)
        self.log(f"Trend-Bot gestartet (PAPER), Budget {START_BALANCE:.2f} USDT, Rotation alle {ROTATION_HOURS} h")

    def get(self, k):
        r = self.db.execute("SELECT v FROM state WHERE k=?", (k,)).fetchone()
        return r["v"] if r else None

    def put(self, k, v):
        self.db.execute("INSERT OR REPLACE INTO state VALUES(?,?)", (k, str(v)))
        self.db.commit()

    def log(self, msg):
        line = f"{dt.datetime.now().strftime('%d.%m. %H:%M:%S')} {msg}"
        print(line, flush=True)
        self.logs.append(line)
        self.logs = self.logs[-80:]

    def open_positions(self):
        return [dict(r) for r in self.db.execute("SELECT * FROM positions WHERE status='open'")]

    def cash(self):
        return float(self.get("cash"))

    @property
    def paused(self):
        return self.get("paused") == "1"

    def upnl_gross(self, p, px):
        return p["size"] * (px - p["entry"]) * (1 if p["side"] == "long" else -1)

    def equity(self):
        eq = self.cash()
        for p in self.open_positions():
            px = self.prices.get(p["symbol"], p["entry"])
            eq += p["size"] * p["entry"] + self.upnl_gross(p, px)  # gebundenes Kapital plus Buchgewinn
        return eq

    # ---- Handel (nur Paper)
    def open_position(self, sym, side, notional, info):
        px = self.prices.get(sym)
        if not px:
            return False
        px = px * (1 + SLIPPAGE) if side == "long" else px * (1 - SLIPPAGE)
        notional = min(notional, self.cash() / (1 + FEE))
        if notional < MIN_NOTIONAL:
            self.log(f"{sym}: Betrag unter {MIN_NOTIONAL:.0f} USDT, übersprungen")
            return False
        size = notional / px
        self.put("cash", self.cash() - notional - notional * FEE)
        self.db.execute(
            "INSERT INTO positions(symbol,side,entry,size,opened,status,chg24,chg7,chg30) VALUES(?,?,?,?,?,?,?,?,?)",
            (sym, side, px, size, time.time(), "open", info["c24"], info["c7"], info["c30"]))
        self.db.commit()
        self.log(f"{'LONG' if side == 'long' else 'SHORT'} {sym} {notional:.2f} USDT @ {px:.6g} (24h {info['c24']*100:+.1f}%, 7d {info['c7']*100:+.1f}%, 30d {info['c30']*100:+.1f}%)")
        return True

    def close_position(self, pid, reason="manuell"):
        with self.lock:
            r = self.db.execute("SELECT * FROM positions WHERE id=? AND status='open'", (pid,)).fetchone()
            if not r:
                return False
            p = dict(r)
            px = self.prices.get(p["symbol"], p["entry"])
            px = px * (1 - SLIPPAGE) if p["side"] == "long" else px * (1 + SLIPPAGE)
            gross = self.upnl_gross(p, px)
            fees = p["size"] * p["entry"] * FEE + p["size"] * px * FEE
            pnl = gross - fees
            self.put("cash", self.cash() + p["size"] * p["entry"] + gross - p["size"] * px * FEE)
            self.db.execute("UPDATE positions SET status='closed',exit=?,closed=?,pnl=?,reason=? WHERE id=?",
                            (px, time.time(), pnl, reason, pid))
            self.db.commit()
            self.log(f"ZU {p['side'].upper()} {p['symbol']} @ {px:.6g} | {reason} | P&L {pnl:+.2f} USDT")
            return True

    def close_all(self):
        return sum(bool(self.close_position(p["id"], "manuell (alle)")) for p in self.open_positions())

    # ---- Strategie
    def analyze(self):
        t = get("/api/v1/market/tickers")["tickers"]
        movers = []
        for x in t:
            s = x["symbol"]
            if not s.endswith("_USDT") or "_PERP" in s:
                continue
            try:
                close, op, amt = float(x["close"]), float(x["open"]), float(x.get("amount", 0))
            except (KeyError, ValueError):
                continue
            if op <= 0 or amt < MIN_VOLUME_USDT:
                continue
            c24 = pct(close, op)
            if abs(c24) > MAX_ABS_MOVE:
                continue
            movers.append((abs(c24), s, c24, close))
        movers.sort(reverse=True)
        out = []
        for _, s, c24, close in movers[:CANDIDATES]:
            try:
                closes, ranges = daily(s)
            except Exception as e:
                self.log(f"{s}: Datenfehler {e}")
                continue
            if len(closes) < 31:
                continue
            c7, c30 = pct(closes[-1], closes[-8]), pct(closes[-1], closes[-31])
            votes = [c24 > 0, c7 > 0, c30 > 0]
            ups = sum(votes)
            side = "long" if ups >= 2 else "short"
            vol = max(sum(ranges[-14:]) / 14, 0.005)  # mittlere Tagesspanne
            out.append(dict(symbol=s, price=close, c24=c24, c7=c7, c30=c30, side=side,
                            agree=max(ups, 3 - ups), vol=vol))
            self.prices[s] = close
            time.sleep(0.2)
        return out

    def rotate(self):
        self.log("--- Rotation: Ranking nach 24h-Bewegung ---")
        ranking = self.analyze()
        if not ranking:
            self.log("Keine Daten, Rotation später erneut")
            return False
        # Auswahl: stärkste Bewegung zuerst, 3 von 3 Signalen vor 2 von 3 bei fast gleicher Stärke
        picks = sorted(ranking, key=lambda r: (abs(r["c24"]) * (1.25 if r["agree"] == 3 else 1.0)), reverse=True)
        eq_free = self.equity()
        n = max(1, min(MAX_POSITIONS, int(eq_free * INVEST_FRACTION // MIN_NOTIONAL)))
        picks = picks[:n]
        for r in ranking:
            r["selected"] = r in picks
        self.ranking = ranking
        want = {r["symbol"]: r for r in picks}
        # bestehende Positionen: behalten, wenn gleiche Richtung weiter gewählt (spart Gebühren)
        keep = set()
        for p in self.open_positions():
            w = want.get(p["symbol"])
            if w and w["side"] == p["side"]:
                keep.add(p["symbol"])
                self.log(f"Halte {p['side'].upper()} {p['symbol']} (Signal unverändert)")
            else:
                self.close_position(p["id"], "Rotation")
        # Gewichte: inverse Volatilität, damit jede Position ähnliches Risiko trägt
        new = [r for r in picks if r["symbol"] not in keep]
        if new:
            total_w = sum(1 / r["vol"] for r in picks)
            budget = self.equity() * INVEST_FRACTION
            held_val = sum(p["size"] * p["entry"] for p in self.open_positions())
            for r in new:
                notional = budget * (1 / r["vol"]) / total_w
                notional = min(notional, max(budget - held_val, 0))
                if self.open_position(r["symbol"], r["side"], notional, r):
                    held_val += notional
        return True

    def manage(self):
        for p in self.open_positions():
            px = self.prices.get(p["symbol"])
            if not px:
                continue
            loss = -self.upnl_gross(p, px) / (p["size"] * p["entry"])
            if loss >= STOP_LOSS:
                self.close_position(p["id"], f"Stop-Loss {STOP_LOSS*100:.0f}%")

    def refresh_prices(self):
        syms = {p["symbol"] for p in self.open_positions()}
        if not syms:
            return
        for x in get("/api/v1/market/tickers")["tickers"]:
            if x["symbol"] in syms:
                self.prices[x["symbol"]] = float(x["close"])

    def next_rotation(self):
        return (int(time.time() // SLOT) + 1) * SLOT

    def step(self):
        with self.lock:
            self.refresh_prices()
            self.manage()
            slot = int(time.time() // SLOT)
            if not self.paused and self.get("slot") != str(slot):
                if self.rotate():
                    self.put("slot", slot)
            now = time.time()
            if now - self.last_eq >= 60:
                self.last_eq = now
                self.db.execute("INSERT INTO equity VALUES(?,?)", (now, self.equity()))
                self.db.commit()

    def rotate_now(self):
        with self.lock:
            return self.rotate()

    def run(self):
        while True:
            try:
                self.step()
            except Exception as e:
                self.log(f"Fehler: {e}")
            time.sleep(POLL_SECONDS)

    def snapshot(self):
        with self.lock:
            opens = []
            for p in self.open_positions():
                px = self.prices.get(p["symbol"], p["entry"])
                p["price"] = px
                gross = self.upnl_gross(p, px)
                p["upnl"] = gross - p["size"] * p["entry"] * FEE * 2
                p["upct"] = gross / (p["size"] * p["entry"]) * 100
                p["notional"] = p["size"] * p["entry"]
                opens.append(p)
            closed = [dict(r) for r in self.db.execute(
                "SELECT * FROM positions WHERE status='closed' ORDER BY closed DESC LIMIT 50")]
            allc = [r["pnl"] for r in self.db.execute("SELECT pnl FROM positions WHERE status='closed'")]
            curve = [[r["ts"], r["eq"]] for r in self.db.execute(
                "SELECT * FROM (SELECT * FROM equity ORDER BY ts DESC LIMIT 300) ORDER BY ts")]
            eq = self.equity()
            wins = sum(1 for x in allc if x > 0)
            return dict(
                mode="paper", paused=self.paused, equity=eq, cash=self.cash(), start=START_BALANCE,
                total_pnl=eq - START_BALANCE, trades=len(allc), winrate=(wins / len(allc) * 100 if allc else 0),
                open=opens, closed=closed, ranking=self.ranking, logs=self.logs[-40:][::-1], curve=curve,
                next_rotation=self.next_rotation(), now=time.time(), rotation_hours=ROTATION_HOURS)
