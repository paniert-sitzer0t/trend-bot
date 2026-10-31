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
    "F": "F: wie B, fest 35 % Einsatz (Kontrolle)",
    "G": "G: wie B, Martingale x1,1 nach Verlustwoche",
    "H": "H: wie B, Martingale x1,4 nach Verlustwoche",
    "K": "K: Long-Momentum mit Trendfilter und Risikosteuerung (wöchentlich)",
    "M": "M: Meme-Hype, Long am Folgetag (1 Tag Haltedauer)",
    "N": "N: Meme-Hype, Gegenbewegung per Short (1 Tag)",
    "E2": "E2: BTC halten mit 2x Hebel",
    "E3": "E3: BTC halten mit 3x Hebel",
    "T3": "T3: BTC 3x Hebel mit Trendfilter",
    "T5": "T5: BTC 5x Hebel mit Trendfilter",
    "K2": "K2: Strategie K mit 2x Hebel",
    "K3": "K3: Strategie K mit 3x Hebel",
    "B2": "B2: Strategie B mit 2x Hebel",
    "X10": "X10: BTC 10x Hebel, Ausbruchs-Trend",
    "X25": "X25: BTC 25x Hebel, Ausbruchs-Trend",
}
LEV = {"E2": 2, "E3": 3, "T3": 3, "T5": 5, "K2": 2, "K3": 3, "B2": 2, "X10": 10, "X25": 25}
BASEOF = {"E2": "E", "E3": "E", "T3": "TREND", "T5": "TREND", "K2": "K", "K3": "K", "B2": "B", "X10": "X", "X25": "X"}
MM = 0.005              # Erhaltungsmarge 0,5 % des Positionswerts: darunter wird liquidiert
LEV_FUND = 0.0003       # Funding und Zinsen auf den gesamten Positionswert bei Hebel, pro Tag
MEMES = ["DOGEUSDT", "SHIBUSDT", "PEPEUSDT", "FLOKIUSDT", "BONKUSDT", "WIFUSDT", "BOMEUSDT", "MEMEUSDT", "TURBOUSDT",
         "NEIROUSDT", "PNUTUSDT", "ACTUSDT", "MOODENGUSDT", "GOATUSDT", "PENGUUSDT", "TRUMPUSDT", "POPCATUSDT",
         "MEWUSDT", "DOGSUSDT", "HMSTRUSDT", "ORDIUSDT", "1000SATSUSDT", "BRETTUSDT", "FARTCOINUSDT"]
MEME_JUMP, MEME_VOL, MEME_EXPO = 0.20, 3.0, 0.20   # Tagesplus ab 20 %, Umsatz mindestens 3x Schnitt, 20 % je Coin
PERIOD = {"A": 1, "D": 1, "B": 7, "C": 7, "E": 10 ** 9, "F": 7, "G": 7, "H": 7, "K": 7, "M": 1, "N": 1,
          "E2": 10 ** 9, "E3": 10 ** 9, "T3": 1, "T5": 1, "K2": 7, "K3": 7, "B2": 7, "X10": 1, "X25": 1}
BASE = 0.35                          # Grundeinsatz der Martingale-Varianten (Anteil am Kapital)
MARTI = {"F": 1.0, "G": 1.1, "H": 1.4}
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
    """Gibt (Anzahl Tage, {symbol: [Schlusskurs oder None]}, erster Tag, {"v": Umsatz, "h": Hoch, "l": Tief je Symbol}) zurück.
    Symbole, die es nicht gibt, bekommen nur None."""
    end = int(time.time() // DAY)            # heutiger (noch offener) Tag wird verworfen
    start = end - days
    raw, rawv, rawh, rawl = {}, {}, {}, {}
    for s in syms:
        cur, rows, vrows, hrows, lrows = start * DAY * 1000, {}, {}, {}, {}
        try:
            while True:
                k = api("/api/v3/klines", {"symbol": s, "interval": "1d", "limit": 1000, "startTime": cur})
                if not k:
                    break
                for x in k:
                    d = int(x[0]) // 86400000
                    if d < end:
                        rows[d] = float(x[4])
                        vrows[d] = float(x[7])
                        hrows[d], lrows[d] = float(x[2]), float(x[3])
                nxt = int(k[-1][0]) + 86400000
                if len(k) < 1000 or nxt // 86400000 >= end:
                    break
                cur = nxt
                time.sleep(0.05)
        except Exception:
            if s == "BTCUSDT":
                raise
        raw[s], rawv[s], rawh[s], rawl[s] = rows, vrows, hrows, lrows
        time.sleep(0.05)
    closes = {s: [raw[s].get(start + i) for i in range(days)] for s in syms}
    ext = dict(v={s: [rawv[s].get(start + i) for i in range(days)] for s in syms},
               h={s: [rawh[s].get(start + i) for i in range(days)] for s in syms},
               l={s: [rawl[s].get(start + i) for i in range(days)] for s in syms})
    return days, closes, start, ext


def ret(c, i, n):
    a, b = c[i], c[i - n] if i - n >= 0 else None
    if a is None or b is None or b <= 0:
        return None
    return a / b - 1


def weights_meme(strat, ctx, i):
    """Meme-Hype: Coin springt an einem Tag um 20 % oder mehr, bei mindestens 3-fachem Umsatz. Am Folgetag
    M: Long (Hype geht weiter), N: Short (Hype kippt). Nur die 2 stärksten, je 20 % des Kapitals, 1 Tag Haltedauer."""
    if not ctx:
        return {}
    memes, memv = ctx
    hits = {}
    for s, c in memes.items():
        v = memv[s]
        if i < 21 or c[i] is None or c[i - 1] is None or v[i] is None:
            continue
        past = [x for x in v[i - 20:i] if x]
        if len(past) < 10:
            continue
        r1 = c[i] / c[i - 1] - 1
        if r1 >= MEME_JUMP and v[i] >= MEME_VOL * sum(past) / len(past):
            hits[s] = r1
    top = sorted(hits, key=hits.get, reverse=True)[:2]
    side = 1 if strat == "M" else -1
    return {s: side * MEME_EXPO for s in top}


def weights_k(closes, i):
    """K: nur Long. Nur wenn BTC über der 200-Tage-Linie liegt. Ranking nach 30d/60d-Rendite OHNE den letzten Tag
    (vermeidet den kurzfristigen Rücksetzer nach Extrembewegungen). Coin selbst muss über seiner 50-Tage-Linie liegen.
    Einsatz sinkt, wenn BTC stark schwankt (Ziel: etwa 45 % jährliche Schwankung, maximal 95 % investiert)."""
    btc = closes["BTCUSDT"]
    if i < 200 or btc[i] is None:
        return {}
    win = [x for x in btc[i - 199:i + 1] if x is not None]
    if len(win) < 150 or btc[i] <= sum(win) / len(win):
        return {}
    cand = {}
    for s, c in closes.items():
        if c[i] is None or c[i - 1] is None or c[i - 31] is None or c[i - 61] is None:
            continue
        w50 = [x for x in c[i - 49:i + 1] if x is not None]
        if len(w50) < 40 or c[i] <= sum(w50) / len(w50):
            continue
        cand[s] = ((c[i - 1] / c[i - 31] - 1) + (c[i - 1] / c[i - 61] - 1)) / 2
    top = sorted(cand, key=cand.get, reverse=True)[:3]
    if not top:
        return {}
    r = [btc[j] / btc[j - 1] - 1 for j in range(i - 29, i + 1) if btc[j] and btc[j - 1]]
    if len(r) < 20:
        return {}
    m = sum(r) / len(r)
    vol = math.sqrt(sum((x - m) ** 2 for x in r) / len(r)) * math.sqrt(365)
    expo = min(INVEST, 0.45 / max(vol, 0.05))
    return {s: expo / len(top) for s in top}


def weights(strat, closes, i, ctx=None):
    """Zielgewichte (positiv = Long, negativ = Short) auf Basis der Daten bis einschließlich Tag i."""
    btc = closes["BTCUSDT"]
    if strat in LEV:
        base = weights(BASEOF[strat], closes, i, ctx)
        return {s: v * LEV[strat] for s, v in base.items()}
    if strat == "E":
        return {"BTCUSDT": INVEST}
    if strat in ("TREND", "X"):
        n = 200 if strat == "TREND" else 50
        win = [x for x in btc[max(0, i - n + 1):i + 1] if x is not None]
        r7 = ret(btc, i, 7)
        if btc[i] is None or i < n or len(win) < n * 0.75 or btc[i] <= sum(win) / len(win):
            return {}
        if strat == "X" and (r7 is None or r7 <= 0):
            return {}
        return {"BTCUSDT": INVEST}
    if strat == "K":
        return weights_k(closes, i)
    if strat in ("M", "N"):
        return weights_meme(strat, ctx, i)
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
    if strat in ("B", "F", "G", "H"):
        w = {s: INVEST / 6 for s in order[:3]}
        w.update({s: -INVEST / 6 for s in order[-3:]})
        return w
    if strat == "C":
        win = [x for x in btc[max(0, i - 199):i + 1] if x is not None]
        if i < 199 or len(win) < 150 or btc[i] is None or btc[i] <= sum(win) / len(win):
            return {}
        return {s: INVEST / 3 for s in order[:3]}
    return {}


def sized(strat, acc, px, closes, i, ctx=None):
    """Zielgewichte inklusive Martingale-Skalierung: Einsatz wächst nach jeder Verlustperiode um den Faktor,
    gedeckelt bei 95 % des Kapitals (1x Hebel), und springt nach einer Gewinnperiode auf den Grundeinsatz zurück."""
    w = weights(strat, closes, i, ctx)
    if strat in MARTI:
        eq = acc.equity(px)
        if acc.last_eq is not None:
            acc.losses = acc.losses + 1 if eq < acc.last_eq else 0
        acc.last_eq = eq
        exposure = min(INVEST, BASE * MARTI[strat] ** acc.losses)
        w = {s: v * exposure / INVEST for s, v in w.items()}
    return w


class Account:
    """Einfaches Paper-Konto mit vorzeichenbehafteten Stückzahlen (Short = negativ)."""

    def __init__(self, cash, pos=None, losses=0, last_eq=None):
        self.cash, self.pos, self.costs = cash, dict(pos or {}), 0.0
        self.losses, self.last_eq = losses, last_eq
        self.dead, self.lev = False, False

    def gross(self, px):
        return sum(abs(q) * px.get(s, 0) for s, q in self.pos.items())

    def liquidated(self, px_worst):
        """True, wenn das Eigenkapital im ungünstigsten Kurs unter die Erhaltungsmarge fällt."""
        g = self.gross(px_worst)
        return g > 0 and self.equity(px_worst) <= MM * g

    def liquidate(self):
        self.cash, self.pos, self.dead = 0.0, {}, True

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
        if self.lev:
            self.cash -= self.gross(px) * LEV_FUND
            return
        short = sum(-q * px.get(s, 0) for s, q in self.pos.items() if q < 0)
        self.cash -= short * SHORT_FUND


def stats(curve, days_per_year=365):
    """curve: Liste von Tages-Equity-Werten."""
    c = [x for x in curve if x is not None]
    if len(c) < 3:
        return {}
    rets = [(c[i] / c[i - 1] - 1) if c[i - 1] > 0 else 0.0 for i in range(1, len(c))]
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
        if progress:
            progress("Lade Meme-Coins ...")
        mdata = load_history(MEMES, days)
        hi = dict(data[3]["h"]); hi.update(mdata[3]["h"])
        lo = dict(data[3]["l"]); lo.update(mdata[3]["l"])
        data = data[:3] + (data[3], mdata[1], mdata[3]["v"], hi, lo)
    n, closes, start, _v, memes, memv, HI, LO = (data + (None, None))[:8] if len(data) == 6 else data
    ctx = (memes, memv)
    allp = dict(closes)
    allp.update(memes)
    first = 31
    res = {}
    for strat in NAMES:
        acc, curve, dayidx, rebs = Account(100.0), [], [], 0
        acc.lev = strat in LEV
        liq = None
        last_reb = -10 ** 9
        for i in range(first, n - 1):
            px_i = {s: c[i] for s, c in allp.items() if c[i] is not None}
            if not acc.dead and i - last_reb >= PERIOD[strat]:
                w = sized(strat, acc, px_i, closes, i, ctx)
                acc.rebalance(px_i, w)
                rebs += 1
                last_reb = i
            px_n = {s: (c[i + 1] if c[i + 1] is not None else px_i.get(s, 0)) for s, c in allp.items()}
            if not acc.dead and acc.pos:
                worst = {}
                for sy, q in acc.pos.items():
                    ex = ((LO if q > 0 else HI) or {}).get(sy)
                    worst[sy] = (ex[i + 1] if ex and ex[i + 1] else None) or px_n.get(sy, 0)
                if acc.liquidated(worst):
                    acc.liquidate()
                    liq = time.strftime("%d.%m.%Y", time.gmtime((start + i + 1) * DAY))
            if not acc.dead:
                acc.funding(px_n)
                if acc.lev and acc.equity(px_n) <= 0:
                    acc.liquidate()
                    liq = time.strftime("%d.%m.%Y", time.gmtime((start + i + 1) * DAY))
            curve.append(0.0 if acc.dead else acc.equity(px_n))
            dayidx.append(start + i + 1)
        st = stats([100.0] + curve)
        years = {}
        for d, e in zip(dayidx, curve):
            y = time.gmtime(d * DAY).tm_year
            years.setdefault(y, [e, e])
            years[y][1] = e
        prev, out = 100.0, {}
        for y in sorted(years):
            out[str(y)] = (years[y][1] / prev - 1) * 100 if prev > 0 else 0.0
            prev = years[y][1]
        pts = [[d, round(e, 2)] for k2, (d, e) in enumerate(zip(dayidx, curve)) if k2 % 7 == 0]
        if curve and (len(curve) - 1) % 7:
            pts.append([dayidx[-1], round(curve[-1], 2)])
        peak_year = max(out.values()) if out else 0.0
        st.update(years=out, costs=acc.costs, rebalances=rebs, final=curve[-1] if curve else 100.0, curve=pts,
                  leverage=LEV.get(strat, 1), liquidated=liq, best_year=peak_year)
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
                days, closes, _, _x = load_history(universe(), 260)
                _d, memes, _s, mext = load_history(MEMES, 260)
                ctx = (memes, mext["v"])
                i = days - 1
                for k in NAMES:
                    s = self.get(k)
                    acc = Account(s["cash"], s["pos"], s.get("losses", 0), s.get("last_eq"))
                    acc.costs = s["costs"]
                    acc.lev = k in LEV
                    if s.get("dead"):
                        continue
                    if s["last_reb"] == 0 or today - s["last_reb"] >= PERIOD[k]:
                        if not (PERIOD[k] >= 10 ** 9 and s["rebs"]):
                            acc.rebalance(self.px, sized(k, acc, self.px, closes, i, ctx))
                            s["rebs"] += 1
                        s["last_reb"] = today
                    acc.funding(self.px)
                    s.update(cash=acc.cash, pos=acc.pos, costs=acc.costs, losses=acc.losses, last_eq=acc.last_eq)
                    self.put(k, s)
                self.put("_day", today)
            for k in NAMES:                  # Liquidationsprüfung mit dem aktuellen Kurs
                s = self.get(k)
                if s.get("dead") or not s["pos"]:
                    continue
                a = Account(s["cash"], s["pos"])
                if a.liquidated(self.px):
                    s.update(cash=0.0, pos={}, dead=True, liq_ts=time.time())
                    self.put(k, s)
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
                cv = [[r[0], r[1]] for r in self.db.execute(
                    "SELECT ts, v FROM (SELECT ts, v FROM eq WHERE k=? ORDER BY ts DESC LIMIT 500) ORDER BY ts", (k,))]
                out.append(dict(key=k, name=NAMES[k], curve=cv, equity=eq, pnl=eq - self.start, pct=(eq / self.start - 1) * 100,
                                positions=pos, rebalances=s["rebs"], costs=s["costs"], lev=LEV.get(k, 1),
                                liq=s.get("liq_ts")))
            return out

    def run(self):
        while True:
            try:
                self.step()
            except Exception as e:
                self.msg = str(e)
                print("Vorwärtstest Fehler:", e, flush=True)
            time.sleep(60)
