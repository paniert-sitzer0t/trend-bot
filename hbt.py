"""Stundenbasierter Backtest: Binance-Stundenkerzen (Cache in candles.db), echte Funding-Raten, 6-h-Rotation.
Wird von strats.run_backtest genutzt. Schwere Daten liegen als array('d') mit NaN für Lücken."""
import array, math, os, sqlite3, time
import requests
import strats as S

HOUR_MS = 3600000
FAPI = "https://fapi.binance.com"
NAN = float("nan")


def fapi(path, params):
    r = requests.get(FAPI + path, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def _db(path):
    db = sqlite3.connect(path)
    db.executescript("CREATE TABLE IF NOT EXISTS k(s TEXT, t INTEGER, c REAL, h REAL, l REAL, PRIMARY KEY(s,t));"
                     "CREATE TABLE IF NOT EXISTS f(s TEXT, t INTEGER, r REAL, PRIMARY KEY(s,t));"
                     "CREATE TABLE IF NOT EXISTS fs(s TEXT PRIMARY KEY, last INTEGER);")
    return db


def _fetch_klines(db, sym, start_ms, end_ms, pause):
    row = db.execute("SELECT MAX(t) FROM k WHERE s=?", (sym,)).fetchone()
    cur = max(start_ms, (row[0] + HOUR_MS) if row and row[0] else start_ms)
    while cur < end_ms:
        k = S.api("/api/v3/klines", {"symbol": sym, "interval": "1h", "limit": 1000, "startTime": cur})
        if not k:
            break
        db.executemany("INSERT OR REPLACE INTO k VALUES(?,?,?,?,?)",
                       [(sym, int(x[0]), float(x[4]), float(x[2]), float(x[3])) for x in k if int(x[0]) + HOUR_MS <= end_ms])
        cur = int(k[-1][0]) + HOUR_MS
        if len(k) < 1000:
            break
        time.sleep(pause)
    db.commit()


def _fetch_funding(db, sym, start_ms, end_ms, pause):
    row = db.execute("SELECT last FROM fs WHERE s=?", (sym,)).fetchone()
    cur = max(start_ms, (row[0] + 1) if row else start_ms)
    last = row[0] if row else 0
    while cur < end_ms:
        k = fapi("/fapi/v1/fundingRate", {"symbol": sym, "startTime": cur, "limit": 1000})
        if not k:
            break
        db.executemany("INSERT OR REPLACE INTO f VALUES(?,?,?)", [(sym, int(x["fundingTime"]), float(x["fundingRate"])) for x in k])
        last = int(k[-1]["fundingTime"])
        cur = last + 1
        if len(k) < 1000:
            break
        time.sleep(pause)
    db.execute("INSERT OR REPLACE INTO fs VALUES(?,?)", (sym, max(last, end_ms - 1 if cur >= end_ms else last)))
    db.commit()


def load_all(syms, days, progress=None, path="candles.db", pause=0.12):
    """Lädt Stundenkerzen und Funding (inkrementell). Gibt ein Bündel mit Arrays je Symbol zurück."""
    db = _db(path)
    end_day = int(time.time() // S.DAY)
    start_day = end_day - days
    start_ms, end_ms = start_day * S.DAY * 1000, end_day * S.DAY * 1000
    H = days * 24
    hc, hh, hl, fund = {}, {}, {}, {}
    real = True
    for n, sym in enumerate(syms):
        if progress:
            progress(f"Stundenkerzen {n + 1}/{len(syms)}: {sym}")
        try:
            _fetch_klines(db, sym, start_ms, end_ms, pause)
        except Exception:
            if sym == "BTCUSDT":
                raise
        try:
            _fetch_funding(db, sym, start_ms, end_ms, pause)
        except Exception:
            if sym == "BTCUSDT":
                real = False
        c, h, l = (array.array("f", [NAN]) * H for _ in range(3))
        got = 0
        for t, cc, hi, lo in db.execute("SELECT t,c,h,l FROM k WHERE s=? AND t>=? AND t<? ORDER BY t", (sym, start_ms, end_ms)):
            j = (t - start_ms) // HOUR_MS
            c[j], h[j], l[j] = cc, hi, lo
            got += 1
        if got:
            hc[sym], hh[sym], hl[sym] = c, h, l
        fr = {}
        for t, r in db.execute("SELECT t,r FROM f WHERE s=? AND t>=? AND t<?", (sym, start_ms, end_ms)):
            fr[(t + HOUR_MS // 2) // HOUR_MS - start_ms // HOUR_MS] = r
        if fr:
            fund[sym] = fr
    db.close()
    return dict(H=H, start=start_day, hc=hc, hh=hh, hl=hl, fund=fund, real=real and bool(fund))


def derive_funding(b):
    """Tägliche Summe der Funding-Raten je Symbol (Liste mit None, wo es keine Daten gibt)."""
    n = b["H"] // 24
    dfund = {}
    for s, fr in b["fund"].items():
        out = [None] * n
        for j, r in fr.items():
            d = j // 24
            if 0 <= d < n:
                out[d] = (out[d] or 0.0) + r
        dfund[s] = out
    return dfund


def sim_hourly(strat, b, closes, ctx, first=31, members=None):
    """Stundengenaue Simulation für Strategien mit Hebel, Trailing-Stop oder 6-h-Rotation.
    Gibt (Kurve je Tag, Tagesindizes, Konto, Anzahl Anpassungen, Liquidationsdatum) zurück."""
    hc, hh, hl, fund, H = b["hc"], b["hh"], b["hl"], b["fund"], b["H"]
    acc = S.Account(100.0)
    acc.lev = strat in S.LEV
    base = strat
    while base in S.BASEOF:
        base = S.BASEOF[base]
    isR = base.startswith("R_")
    step = 6 if isR else 24
    mult = S.LEV.get(strat, 1)
    trail = S.TRAIL[strat][1] if strat in S.TRAIL else None
    last = {}
    views = {}

    def view(d):
        if members is None:
            return closes
        v = views.get(d)
        if v is None:
            v = views[d] = {s: closes[s] for s in members[d] if s in closes}
        return v

    curve, dayidx, rebs, liq = [], [], 0, None
    last_d = -10 ** 9
    h = 24 * first + 23

    def price(s, j):
        v = hc[s][j]
        if v == v:
            last[s] = v
            return v
        return last.get(s, 0.0)

    while h < H - 1:
        d = h // 24
        if not acc.dead and (isR or d - last_d >= S.PERIOD[strat]):
            mem = members[d] if members is not None else None
            px = {s: hc[s][h] for s in (hc if mem is None else (set(mem) | set(acc.pos))) if s in hc and hc[s][h] == hc[s][h]}
            if isR:
                rets = {s: p / hc[s][h - 24] - 1 for s, p in px.items()
                        if (mem is None or s in mem) and hc[s][h - 24] == hc[s][h - 24] and hc[s][h - 24] > 0}
                w = {s: v * mult for s, v in S.rank_weights(base, rets).items()}
            else:
                w = S.sized(strat, acc, px, view(d), d, ctx)
            for s, v in px.items():
                last[s] = v
            acc.rebalance(px, w)
            rebs += 1
            last_d = d
        for j in range(h + 1, h + step + 1):
            if acc.dead:
                if j % 24 == 23:
                    curve.append(0.0); dayidx.append(b["start"] + j // 24)
                continue
            if acc.pos:
                hi = {s: (hh[s][j] if hh[s][j] == hh[s][j] else price(s, j)) for s in acc.pos}
                lo = {s: (hl[s][j] if hl[s][j] == hl[s][j] else price(s, j)) for s in acc.pos}
                if trail:
                    acc.track(hi, lo, trail)
                if acc.pos:
                    worst = {s: (lo[s] if q > 0 else hi[s]) for s, q in acc.pos.items()}
                    if acc.liquidated(worst):
                        acc.liquidate()
                        liq = time.strftime("%d.%m.%Y", time.gmtime((b["start"] * 24 + j) * 3600))
                if acc.pos and j % 8 == 0:        # Funding 00:00, 08:00, 16:00 UTC
                    px = {s: price(s, j) for s in acc.pos}
                    rates = {s: fund[s][j] for s in acc.pos if s in fund and j in fund[s]}
                    acc.funding_real(px, rates, 1 / 3)
                if acc.lev and acc.pos and acc.equity({s: price(s, j) for s in acc.pos}) <= 0:
                    acc.liquidate()
                    liq = time.strftime("%d.%m.%Y", time.gmtime((b["start"] * 24 + j) * 3600))
            if j % 24 == 23:
                curve.append(0.0 if acc.dead else acc.equity({s: price(s, j) for s in acc.pos}) if acc.pos else acc.cash)
                dayidx.append(b["start"] + j // 24)
        h += step
    return curve, dayidx, acc, rebs, liq
