"""Strategie-Vergleich: Backtest (Binance-Tagesdaten) und paralleler Vorwärtstest im Paper-Modus.
Alle Strategien teilen sich dieselbe Logik (weights), damit Backtest und Vorwärtstest vergleichbar sind."""
import json, os, sqlite3, threading, time, math
import requests

BASES = ["https://data-api.binance.vision", "https://api.binance.com"]
FEE = 0.0005            # 0,05 % pro Seite
SLIP = 0.0008           # 0,08 % pro Seite, vorsichtig für Altcoins
SHORT_FUND = 0.0003     # ca. 0,03 % pro Tag Funding auf Short-Positionen
INVEST = 0.95
NUM = 40                # Anzahl Coins im Universum
STABLE = ("USDC", "TUSD", "FDUSD", "USDP", "DAI", "BUSD", "EUR", "AEUR", "USD1", "XUSD", "PAXG", "UST", "USTC")
NAMES = {
    "A": "A: Top-Mover 24h (täglich)",
    "D": "D: Gegenbewegung zu A",
    "B": "B: Momentum 7d/30d Long+Short (wöchentlich)",
    "C": "C: Momentum nur Long mit BTC-Filter (wöchentlich)",
    "E": "E: BTC halten",
}
PERIOD = {"A": 1, "D": 1, "B": 7, "C": 7, "E": 10 ** 9}
DAY = 86400


def api(path, params=None):
    last = None
    for b in BASES:
        try:
            r = requests.get(b + path, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # nächster Host
            last = e
    raise last


def universe():
    t = api("/api/v3/ticker/24hr")
    out = []
    for x in t:
        s = x["symbol"]
        if not s.endswith("USDT"):
            continue
        base = s[:-4]
        if base in STABLE or (len(base) > 4 and base.endswith(("UP", "DOWN", "BULL", "BEAR"))):
            continue
        out.append((float(x["quoteVolume"]), s))
    out.sort(reverse=True)
    syms = [s for _, s in out[:NUM]]
    if "BTCUSDT" not in syms:
        syms.append("BTCUSDT")
    return syms


def load_history(syms, days):
    """Gibt (Anzahl Tage, {symbol: [Schlusskurs oder None]}, erster Tag als Tagesnummer) zurück."""
    end = int(time.time() // DAY)            # heutiger (noch offener) Tag wird verworfen
    start = end - days
    raw = {}
    for s in syms:
        cur, rows = start * DAY * 1000, {}
        while True:
            k = api("/api/v3/klines", {"symbol": s, "interval": "1d", "limit": 1000, "startTime": cur})
            if not k:
                break
            for x in k:
                d = int(x[0]) // 86400000
                if d < end:
                    rows[d] = float(x[4])
            nxt = int(k[-1][0]) + 86400000
            if len(k) < 1000 or nxt // 86400000 >= end:
                break
            cur = nxt
            time.sleep(0.05)
        raw[s] = rows
        time.sleep(0.05)
    closes = {s: [raw[s].get(start + i) for i in range(days)] for s in syms}
    return days, closes, start


def ret(c, i, n):
    a, b = c[i], c[i - n] if i - n >= 0 else None
    if a is None or b is None or b <= 0:
        return None
    return a / b - 1


def weights(strat, closes, i):
    """Zielgewichte (positiv = Long, negativ = Short) auf Basis der Daten bis einschließlich Tag i."""
    btc = closes["BTCUSDT"]
    if strat == "E":
        return {"BTCUSDT": INVEST}
    scores = {}
    for s, c in closes.items():
        if c[i] is None:
            continue
        r1, r7, r30 = ret(c, i, 1), ret(c, i, 7), ret(c, i, 30)
        if r1 is None or r7 is None or r30 is None:
            continue
        scores[s] = (r1, r7, r30)
    if len(scores) < 8:
        return {}
    if strat in ("A", "D"):
        top = sorted(scores, key=lambda s: abs(scores[s][0]), reverse=True)[:3]
        w = {}
        for s in top:
            ups = sum(v > 0 for v in scores[s])
            side = 1 if ups >= 2 else -1
            w[s] = side * (-1 if strat == "D" else 1) * INVEST / 3
        return w
    mom = {s: (v[1] + v[2]) / 2 for s, v in scores.items()}
    order = sorted(mom, key=mom.get, reverse=True)
    if strat == "B":
        w = {s: INVEST / 6 for s in order[:3]}
        w.update({s: -INVEST / 6 for s in order[-3:]})
        return w
    if strat == "C":
        win = [x for x in btc[max(0, i - 199):i + 1] if x is not None]
        if i < 199 or len(win) < 150 or btc[i] is None or btc[i] <= sum(win) / len(win):
            return {}
        return {s: INVEST / 3 for s in order[:3]}
    return {}


class Account:
    """Einfaches Paper-Konto mit vorzeichenbehafteten Stückzahlen (Short = negativ)."""

    def __init__(self, cash, pos=None):
        self.cash, self.pos, self.costs = cash, dict(pos or {}), 0.0

    def equity(self, px):
        return self.cash + sum(q * px.get(s, 0) for s, q in self.pos.items())

    def rebalance(self, px, w):
        eq = self.equity(px)
        for s in set(self.pos) | set(w):
            p = px.get(s)
            if not p:
                continue
            target = eq * w.get(s, 0) / p
            dq = target - self.pos.get(s, 0)
            if abs(dq * p) < 1e-9:
                continue
            cost = abs(dq * p) * (FEE + SLIP)
            self.cash -= dq * p + cost
            self.costs += cost
            if abs(target) < 1e-12:
                self.pos.pop(s, None)
            else:
                self.pos[s] = target

    def funding(self, px):
        short = sum(-q * px.get(s, 0) for s, q in self.pos.items() if q < 0)
        self.cash -= short * SHORT_FUND


def stats(curve, days_per_year=365):
    """curve: Liste von Tages-Equity-Werten."""
    c = [x for x in curve if x is not None]
    if len(c) < 3:
        return {}
    rets = [c[i] / c[i - 1] - 1 for i in range(1, len(c))]
    mean = sum(rets) / len(rets)
    sd = math.sqrt(sum((r - mean) ** 2 for r in rets) / len(rets)) or 1e-12
    peak, mdd = c[0], 0.0
    for x in c:
        peak = max(peak, x)
        mdd = min(mdd, x / peak - 1)
    yrs = len(c) / days_per_year
    return dict(total=(c[-1] / c[0] - 1) * 100, cagr=((c[-1] / c[0]) ** (1 / yrs) - 1) * 100 if c[-1] > 0 else -100,
                maxdd=mdd * 100, sharpe=mean / sd * math.sqrt(days_per_year))


def run_backtest(days=1900, progress=None, data=None):
    if data is None:
        syms = universe()
        if progress:
            progress(f"Lade {len(syms)} Coins, {days} Tage ...")
        data = load_history(syms, days)
    n, closes, start = data
    first = 31
    res = {}
    for strat in NAMES:
        acc, curve, dayidx, rebs = Account(100.0), [], [], 0
        last_reb = -10 ** 9
        for i in range(first, n - 1):
            px_i = {s: c[i] for s, c in closes.items() if c[i] is not None}
            if i - last_reb >= PERIOD[strat]:
                w = weights(strat, closes, i)
                acc.rebalance(px_i, w)
                rebs += 1
                last_reb = i
            px_n = {s: (c[i + 1] if c[i + 1] is not None else px_i.get(s, 0)) for s, c in closes.items()}
            acc.funding(px_n)
            curve.append(acc.equity(px_n))
            dayidx.append(start + i + 1)
        st = stats([100.0] + curve)
        years = {}
        for d, e in zip(dayidx, curve):
            y = time.gmtime(d * DAY).tm_year
            years.setdefault(y, [e, e])
            years[y][1] = e
        prev, out = 100.0, {}
        for y in sorted(years):
            out[str(y)] = (years[y][1] / prev - 1) * 100
            prev = years[y][1]
        st.update(years=out, costs=acc.costs, rebalances=rebs, final=curve[-1] if curve else 100.0)
        res[strat] = dict(name=NAMES[strat], **st)
    return dict(ts=time.time(), days=n, coins=len(closes), start=time.strftime("%d.%m.%Y", time.gmtime(start * DAY)),
                results=res,
                note="Universum = heutige Top-Coins nach Umsatz (Survivorship-Bias begünstigt Long-Strategien). "
                     "Keine Parameter wurden angepasst. Short mit 0,03 % Funding pro Tag, Kosten 0,13 % pro Seite.")


def run_and_save(path="backtest.json", progress=None):
    try:
        r = run_backtest(progress=progress)
    except Exception as e:
        r = dict(ts=time.time(), error=str(e))
    with open(path, "w") as f:
        json.dump(r, f)
    return r


# ---------------------------------------------------------------- Vorwärtstest
class Forward:
    """Lässt alle Strategien parallel im Paper-Modus laufen, je 77 USDT, tägliche Anpassung um 00:10 UTC."""

    def __init__(self, path="fwd.db", start=77.0):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.executescript("CREATE TABLE IF NOT EXISTS st(k TEXT PRIMARY KEY, v TEXT);"
                              "CREATE TABLE IF NOT EXISTS eq(ts REAL, k TEXT, v REAL);")
        self.lock = threading.Lock()
        self.start = start
        self.px, self.msg, self.last_eq = {}, "", 0.0
        for k in NAMES:
            if self.get(k) is None:
                self.put(k, dict(cash=start, pos={}, last_reb=0, rebs=0, costs=0.0))

    def get(self, k):
        r = self.db.execute("SELECT v FROM st WHERE k=?", (k,)).fetchone()
        return json.loads(r[0]) if r else None

    def put(self, k, v):
        self.db.execute("INSERT OR REPLACE INTO st VALUES(?,?)", (k, json.dumps(v)))
        self.db.commit()

    def prices(self):
        self.px = {x["symbol"]: float(x["price"]) for x in api("/api/v3/ticker/price")}

    def step(self):
        with self.lock:
            self.prices()
            today = int(time.time() // DAY)
            done = int((self.get("_day") or 0))
            if today > done and time.time() % DAY > 600:
                days, closes, _ = load_history(universe(), 260)
                i = days - 1
                for k in NAMES:
                    s = self.get(k)
                    acc = Account(s["cash"], s["pos"])
                    acc.costs = s["costs"]
                    if s["last_reb"] == 0 or today - s["last_reb"] >= PERIOD[k]:
                        if not (k == "E" and s["rebs"]):
                            acc.rebalance(self.px, weights(k, closes, i))
                            s["rebs"] += 1
                        s["last_reb"] = today
                    acc.funding(self.px)
                    s.update(cash=acc.cash, pos=acc.pos, costs=acc.costs)
                    self.put(k, s)
                self.put("_day", today)
            now = time.time()
            if now - self.last_eq > 600:
                self.last_eq = now
                for k in NAMES:
                    s = self.get(k)
                    self.db.execute("INSERT INTO eq VALUES(?,?,?)", (now, k, Account(s["cash"], s["pos"]).equity(self.px)))
                self.db.commit()

    def snapshot(self):
        with self.lock:
            out = []
            for k in NAMES:
                s = self.get(k)
                acc = Account(s["cash"], s["pos"])
                eq = acc.equity(self.px) if self.px else s["cash"]
                pos = [dict(symbol=a, side="long" if q > 0 else "short", value=abs(q * self.px.get(a, 0)))
                       for a, q in s["pos"].items()]
                out.append(dict(key=k, name=NAMES[k], equity=eq, pnl=eq - self.start, pct=(eq / self.start - 1) * 100,
                                positions=pos, rebalances=s["rebs"], costs=s["costs"]))
            return out

    def run(self):
        while True:
            try:
                self.step()
            except Exception as e:
                self.msg = str(e)
                print("Vorwärtstest Fehler:", e, flush=True)
            time.sleep(60)
