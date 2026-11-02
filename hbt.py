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


def sanitize(b, closes, tol=0.05):
    """Gleicht Stundendaten mit den Tageskerzen ab. Tage, an denen der 23-Uhr-Schlusskurs mehr als 5 % vom
    Tagesschluss abweicht, werden verworfen; einzelne Ausreißer-Stunden (Sprung und sofortige Rückkehr) ebenso.
    Coins mit mehr als 2 % fehlerhaften Tagen fliegen ganz aus den Stundendaten. Gibt einen Bericht zurück."""
    hc, hh, hl = b["hc"], b["hh"], b["hl"]
    rep, dropped = {}, []
    for s in list(hc):
        c, hi, lo = hc[s], hh[s], hl[s]
        cl = closes.get(s)
        bad = good = spikes = 0
        n = len(c) // 24
        for d in range(n):
            hv = c[24 * d + 23]
            dv = cl[d] if cl is not None and d < len(cl) else None
            if hv != hv or not dv:
                continue
            if abs(hv / dv - 1) > tol:
                bad += 1
                for j in range(24 * d, 24 * d + 24):
                    c[j] = hi[j] = lo[j] = NAN
            else:
                good += 1
        for j in range(1, len(c) - 1):
            a, x, z = c[j - 1], c[j], c[j + 1]
            if a == a and x == x and z == z and a > 0 and z > 0 and x > 0:
                r1, r2 = x / a, z / a
                if (r1 > 2 or r1 < 0.5) and 0.8 < r2 < 1.25:
                    c[j] = hi[j] = lo[j] = NAN
                    spikes += 1
            if c[j] == c[j] and hi[j] == hi[j] and c[j] > 0:
                ref = max(c[j], c[j - 1]) if c[j - 1] == c[j - 1] else c[j]
                if hi[j] > 3 * ref:
                    hi[j] = ref
                ref = min(c[j], c[j - 1]) if c[j - 1] == c[j - 1] else c[j]
                if lo[j] < ref / 3:
                    lo[j] = ref
        if bad or spikes:
            rep[s] = (bad, spikes)
        if good + bad and bad > 0.02 * (good + bad) and s != "BTCUSDT":
            dropped.append(s)
            for k in ("hc", "hh", "hl"):
                b[k].pop(s, None)
    return dict(korrigiert=len(rep), entfernt=dropped, details={k: v for k, v in sorted(rep.items(), key=lambda x: -x[1][0])[:15]})


def derive_mark(b):
    """Tageshoch und -tief der Stunden-Schlusskurse (Näherung für den Mark-Preis, der Dochte glättet)."""
    n = b["H"] // 24
    mhi, mlo = {}, {}
    for s, c in b["hc"].items():
        hi, lo = [None] * n, [None] * n
        for d in range(n):
            seg = [v for v in c[24 * d:24 * d + 24] if v == v]
            if seg:
                hi[d], lo[d] = max(seg), min(seg)
        mhi[s], mlo[s] = hi, lo
    return mhi, mlo


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
    isH = base in S.HOURK
    step = 1 if isH else (6 if isR else 24)
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
    jumps = []                                   # größte Tagesgewinne zur Kontrolle auf Datenfehler
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
        if not acc.dead and (isR or isH or d - last_d >= S.PERIOD[strat]):
            mem = members[d] if members is not None else None
            px = {s: hc[s][h] for s in (hc if mem is None else (set(mem) | set(acc.pos))) if s in hc and hc[s][h] == hc[s][h]}
            for s in acc.pos:                    # Datenlücke bei gehaltenem Coin: letzter bekannter Kurs
                if s not in px:
                    px[s] = price(s, h)
            if isH:
                w = {s: v * mult for s, v in S.hour_weights(base, h, hc, b["start"]).items()}
            elif isR:
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
                    worst = {s: price(s, j) for s in acc.pos}       # Liquidation nach Mark-Preis (Stundenschluss), nicht nach Docht
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
                eq_now = 0.0 if acc.dead else acc.equity({s: price(s, j) for s in acc.pos}) if acc.pos else acc.cash
                if curve and curve[-1] > 0 and eq_now / curve[-1] > 1.5:
                    top = max(acc.pos, key=lambda s: abs(acc.pos[s]) * price(s, j)) if acc.pos else "-"
                    jumps.append((round((eq_now / curve[-1] - 1) * 100, 1),
                                  time.strftime("%d.%m.%Y", time.gmtime((b["start"] * 24 + j) * 3600)), top))
                curve.append(eq_now)
                dayidx.append(b["start"] + j // 24)
        h += step
    acc.jumps = sorted(jumps, reverse=True)[:5]
    return curve, dayidx, acc, rebs, liq


def sim_orig(b, closes, HI, LO, first=31, members=None, stop=0.08, max_pos=3, cands=12, max_move=0.5):
    """Hauptbot rückwärts gerechnet: alle 8 h die 12 stärksten 24-h-Beweger (|Bewegung| bis 50 %),
    Richtung per Mehrheit aus 24 h, 7 Tage, 30 Tage, Auswahl der Top 3 (Gleichklang zählt 1,25-fach),
    Gewichte nach inverser Tagesspanne, gleiche Richtung wird gehalten, 8 % Stop-Loss ab Einstieg.
    Universum: Coins mit Stundendaten unter den umsatzstärksten des Tages."""
    hc, hh, hl, fund, H = b["hc"], b["hh"], b["hl"], b["fund"], b["H"]
    acc = S.Account(100.0)
    entry, last = {}, {}
    curve, dayidx, rebs = [], [], 0

    def price(s, j):
        v = hc[s][j]
        if v == v:
            last[s] = v
            return v
        return last.get(s, 0.0)

    def rng(s, d):
        h, l, c = (HI or {}).get(s), (LO or {}).get(s), closes.get(s)
        if not (h and l and c):
            return None
        xs = [(h[k] - l[k]) / c[k] for k in range(max(0, d - 13), d + 1) if h[k] and l[k] and c[k]]
        return max(sum(xs) / len(xs), 0.005) if len(xs) >= 7 else None

    for j in range(24 * first, H):
        d = j // 24
        if j % 8 == 0 and j >= 720:
            mem = members[d - 1] if members is not None else list(hc)
            mv = []
            for s in mem:
                c = hc.get(s)
                if c is None:
                    continue
                p, p24, p7, p30 = c[j], c[j - 24], c[j - 168], c[j - 720]
                if not (p == p and p24 == p24 and p7 == p7 and p30 == p30) or min(p24, p7, p30) <= 0:
                    continue
                c24 = p / p24 - 1
                if abs(c24) > max_move:
                    continue
                mv.append((abs(c24), s, c24, p / p7 - 1, p / p30 - 1))
            mv.sort(reverse=True)
            rank = []
            for _, s, c24, c7, c30 in mv[:cands]:
                ups = (c24 > 0) + (c7 > 0) + (c30 > 0)
                v = rng(s, d - 1)
                if v is None:
                    continue
                rank.append((abs(c24) * (1.25 if ups in (0, 3) else 1.0), s, 1 if ups >= 2 else -1, v))
            rank.sort(reverse=True)
            picks = rank[:max_pos]
            if picks:
                px = {s: price(s, j) for s in set(acc.pos) | {r[1] for r in picks}}
                eq = acc.equity(px)
                tw = sum(1 / r[3] for r in picks)
                want = {r[1]: r for r in picks}
                w = {}
                for s, q in acc.pos.items():
                    r = want.get(s)
                    if r and (q > 0) == (r[2] > 0):
                        w[s] = q * px[s] / eq if eq > 0 else 0       # gleiche Richtung: halten, nicht umschichten
                free = S.INVEST - sum(abs(x) for x in w.values())
                new = [r for r in picks if r[1] not in w]
                nw = sum(1 / r[3] for r in new) or 1
                for r in new:
                    w[r[1]] = r[2] * min(S.INVEST * (1 / r[3]) / tw, max(free, 0) * (1 / r[3]) / nw)
                for s in list(acc.pos):
                    if s not in w:
                        entry.pop(s, None)
                acc.rebalance(px, w)
                for s in w:
                    if s not in entry and s in acc.pos:
                        entry[s] = px[s]
                rebs += 1
        if acc.pos:
            for s, q in list(acc.pos.items()):
                e = entry.get(s)
                hi = hh[s][j] if s in hh and hh[s][j] == hh[s][j] else price(s, j)
                lo = hl[s][j] if s in hl and hl[s][j] == hl[s][j] else price(s, j)
                if e and (lo <= e * (1 - stop) if q > 0 else hi >= e * (1 + stop)):
                    sp = e * (1 - stop) if q > 0 else e * (1 + stop)
                    cost = abs(q) * sp * (S.FEE + S.SLIP)
                    acc.cash += q * sp - cost
                    acc.costs += cost
                    acc.pos.pop(s)
                    entry.pop(s, None)
                    acc.stops += 1
            if acc.pos and j % 8 == 0:
                pxf = {s: price(s, j) for s in acc.pos}
                rates = {s: fund[s][j] for s in acc.pos if s in fund and j in fund[s]}
                acc.funding_real(pxf, rates, 1 / 3)
        if j % 24 == 23:
            curve.append(acc.equity({s: price(s, j) for s in acc.pos}) if acc.pos else acc.cash)
            dayidx.append(b["start"] + d)
    return curve, dayidx, acc, rebs
