"""Strategie-Suche mit Gegenprobe (Walk-Forward).

Der Bot probiert viele Varianten einer Strategie-Familie durch. Ausgewählt wird nur nach den Daten bis
Ende 2023 (Suchzeitraum). Ab 2024 (Gegenprobe) sieht die Suche nichts. Gezeigt wird, wie sich die in der
Suche besten Varianten danach geschlagen haben. So fällt auf, ob eine Variante nur die Vergangenheit
auswendig gelernt hat.
"""
import array, math, random, time
from collections import deque

import strats as S

SPLIT = "2024-01-01"           # ab hier Gegenprobe
BUDGET = 25 * 60               # maximale Rechenzeit in Sekunden
TOPK = 15                      # gespeicherte Ranglisten je Tag und Seite

SIGNALS = ([("mom", L) for L in (7, 14, 28, 56, 90)] + [("hoch", W) for W in (60, 90, 180, 365)]
           + [("risk", L) for L in (28, 90)])
SIGNAME = {"mom": "Momentum %d Tage", "hoch": "Nähe %d-Tage-Hoch", "risk": "Momentum je Risiko %d Tage"}
FILTERS = {"keiner": None, "BTC über 200-Tage-Schnitt": 200, "BTC über 100-Tage-Schnitt": 100}
MODES = {"long": "nur Long", "flip": "Long im Aufwärtstrend, Short im Abwärtstrend", "ls": "Long Top und Short Flop"}


def _rolling_max(c, W):
    out, dq = [None] * len(c), deque()
    cnt = 0
    valid = [0] * (len(c) + 1)
    for i, x in enumerate(c):
        valid[i + 1] = valid[i] + (x is not None)
        if x is not None:
            while dq and c[dq[-1]] <= x:
                dq.pop()
            dq.append(i)
        while dq and dq[0] <= i - W:
            dq.popleft()
        if x is not None and dq and valid[i + 1] - valid[max(0, i + 1 - W)] >= 0.8 * W:
            out[i] = c[dq[0]]
    return out


def _rolling_vol(c, L):
    n = len(c)
    s1, s2, k = [0.0] * (n + 1), [0.0] * (n + 1), [0] * (n + 1)
    for i in range(n):
        r = c[i] / c[i - 1] - 1 if i and c[i] is not None and c[i - 1] else None
        s1[i + 1] = s1[i] + (r or 0.0)
        s2[i + 1] = s2[i] + (r or 0.0) ** 2
        k[i + 1] = k[i] + (r is not None)
    out = [None] * n
    for i in range(L, n):
        m = k[i + 1] - k[i + 1 - L]
        if m >= 0.8 * L:
            mu = (s1[i + 1] - s1[i + 1 - L]) / m
            v = (s2[i + 1] - s2[i + 1 - L]) / m - mu * mu
            out[i] = math.sqrt(max(v, 1e-12))
    return out


def rankings(closes, members, n, first):
    """Je Signal und Tag: (Top-Liste absteigend, Flop-Liste aufsteigend) unter den Mitgliedern des Tages."""
    syms = set()
    for i in range(first, n):
        syms.update(s for s in members[i] if s in closes)
    pre = {}
    for sig, p in SIGNALS:
        if sig == "hoch":
            pre[(sig, p)] = {s: _rolling_max(closes[s], p) for s in syms}
        elif sig == "risk":
            pre[(sig, p)] = {s: _rolling_vol(closes[s], 28) for s in syms}
    out = {}
    for sig, p in SIGNALS:
        aux = pre.get((sig, p))
        top, flop = [None] * n, [None] * n
        for i in range(first, n):
            sc = []
            for s in members[i]:
                c = closes.get(s)
                if c is None or c[i] is None:
                    continue
                if sig == "hoch":
                    m = aux[s][i]
                    if m:
                        sc.append((c[i] / m, s))
                else:
                    b = c[i - p] if i - p >= 0 else None
                    if not b:
                        continue
                    r = c[i] / b - 1
                    if sig == "risk":
                        v = aux[s][i]
                        if not v:
                            continue
                        r = r / (v * math.sqrt(p))
                    sc.append((r, s))
            if len(sc) >= 10:
                sc.sort(reverse=True)
                top[i] = [s for _, s in sc[:TOPK]]
                flop[i] = [s for _, s in sc[::-1][:TOPK]]
        out[(sig, p)] = (top, flop)
    return out


def combos():
    out = []
    for sig in SIGNALS:
        for k in (3, 5, 10):
            for mode in MODES:
                for flt in FILTERS:
                    if mode == "ls" and flt != "keiner":
                        continue
                    if mode == "flip" and flt == "keiner":
                        continue
                    for per in (3, 7, 14):
                        for tr in (0.0, 0.2):
                            for lev in (1, 2):
                                out.append((sig, k, mode, flt, per, tr, lev))
    random.Random(42).shuffle(out)          # bei Zeitlimit bleibt die Stichprobe unverzerrt
    return out


def simulate(cb, R, closes, members, n, first, start, HI, LO, MHI, MLO, dfund, sma):
    sig, k, mode, flt, per, tr, lev = cb
    top, flop = R[sig]
    acc = S.Account(100.0)
    acc.lev = lev > 1
    last = {}
    curve, liq, last_reb, rebs = [], None, -10 ** 9, 0
    btc = closes["BTCUSDT"]
    fl = FILTERS[flt]
    for i in range(first, n - 1):
        if acc.dead:
            curve.append(0.0)
            continue
        if i - last_reb >= per:
            bull = True
            if fl:
                m = sma[fl][i]
                bull = bool(m and btc[i] and btc[i] > m)
            w = {}
            t, f = top[i], flop[i]
            if t:
                u = S.INVEST * lev
                if mode == "long" and bull:
                    w = {s: u / k for s in t[:k]}
                elif mode == "flip":
                    w = {s: u / k for s in t[:k]} if bull else {s: -u / k for s in f[:k]}
                elif mode == "ls":
                    w = {s: u / 2 / k for s in t[:k]}
                    for s in f[:k]:
                        w[s] = w.get(s, 0) - u / 2 / k
            px = {}
            for s in set(w) | set(acc.pos):
                c = closes.get(s)
                p = c[i] if c is not None else None
                if p is None:
                    p = last.get(s)
                if p:
                    px[s] = p
                    last[s] = p
            acc.rebalance(px, w)
            rebs += 1
            last_reb = i
        pxn = {}
        for s in acc.pos:
            p = closes[s][i + 1]
            if p is None:
                p = last.get(s, 0.0)
            else:
                last[s] = p
            pxn[s] = p
        if acc.pos and tr:
            hh = {s: ((HI or {}).get(s) or [None] * (i + 2))[i + 1] or pxn[s] for s in acc.pos}
            ll = {s: ((LO or {}).get(s) or [None] * (i + 2))[i + 1] or pxn[s] for s in acc.pos}
            acc.track(hh, ll, tr)
        if acc.pos:
            worst = {}
            for s, q in acc.pos.items():
                ex = (MLO if q > 0 else MHI).get(s) or ((LO if q > 0 else HI) or {}).get(s)
                worst[s] = (ex[i + 1] if ex and ex[i + 1] else None) or pxn.get(s, 0)
            if acc.liquidated(worst):
                acc.liquidate()
                liq = time.strftime("%d.%m.%Y", time.gmtime((start + i + 1) * S.DAY))
        if not acc.dead and acc.pos:
            if dfund:
                rt = {s: dfund[s][i + 1] for s in acc.pos if s in dfund and dfund[s][i + 1] is not None}
                acc.funding_real(pxn, rt)
            else:
                acc.funding(pxn)
            if acc.equity(pxn) <= 0:
                acc.liquidate()
                liq = time.strftime("%d.%m.%Y", time.gmtime((start + i + 1) * S.DAY))
        curve.append(0.0 if acc.dead else acc.equity(pxn))
    return curve, liq, rebs


def _seg(curve, a, b):
    """Kennzahlen eines Abschnitts (Indizes in der Kurve)."""
    c = [100.0 if a == 0 else curve[a - 1]] + curve[a:b]
    if c[0] <= 0:
        return dict(total=-100.0, cagr=-100.0, maxdd=-100.0)
    st = S.stats([x / c[0] * 100 for x in c])
    if not st:
        return dict(total=0.0, cagr=0.0, maxdd=0.0)
    return dict(total=round(st["total"], 1), cagr=round(st["cagr"], 1), maxdd=round(st["maxdd"], 1))


def _spearman(a, b):
    def rk(v):
        o = sorted(range(len(v)), key=lambda i: v[i])
        r = [0] * len(v)
        for j, i in enumerate(o):
            r[i] = j
        return r
    ra, rb = rk(a), rk(b)
    n = len(a)
    if n < 3:
        return None
    d = sum((x - y) ** 2 for x, y in zip(ra, rb))
    return round(1 - 6 * d / (n * (n * n - 1)), 2)


def label(cb):
    sig, k, mode, flt, per, tr, lev = cb
    t = f"{SIGNAME[sig[0]] % sig[1]}, {k} Coins, {MODES[mode]}"
    if flt != "keiner":
        t += f", Filter {flt}"
    t += f", alle {per} Tage"
    if tr:
        t += f", Trailing {int(tr * 100)} %"
    if lev > 1:
        t += f", {lev}x Hebel"
    return t


def search(closes, members, n, start, HI, LO, MHI, MLO, dfund, progress=None, first=31):
    t0 = time.time()
    split = int(time.mktime(time.strptime(SPLIT, "%Y-%m-%d")) // S.DAY) - start
    btc = closes["BTCUSDT"]
    sma = {L: [S._sma(btc, i, L) if i >= L else None for i in range(n)] for L in (100, 200)}
    if progress:
        progress("Suche 0/1: Ranglisten")
    R = rankings(closes, members, n, first)
    cbs = combos()
    L = n - 1 - first
    a, b = 0, split - first - 1              # Kurvenindex: Tag first+1+j
    if b < 0.4 * L or b > L - 90:             # zu wenig Daten für die feste Grenze: 70/30 teilen
        b = int(0.7 * L)
    cut = start + first + 1 + b
    rows = []
    for m, cb in enumerate(cbs):
        if time.time() - t0 > BUDGET:
            break
        if progress and m % 10 == 0:
            progress(f"Suche {m}/{len(cbs)}: Varianten")
        curve, liq, rebs = simulate(cb, R, closes, members, n, first, start, HI, LO, MHI, MLO, dfund, sma)
        tr, te, full = _seg(curve, a, b), _seg(curve, b, len(curve)), _seg(curve, 0, len(curve))
        rows.append(dict(cb=cb, train=tr, test=te, full=full, liq=liq, rebs=rebs,
                         curve=array.array("f", curve[::7])))
    # Auswahl nur ohne Hebel (der echte Bot handelt nur mit 1x); Hebel-Varianten zählen nur im Rückblick
    ok = [r for r in rows if r["cb"][6] == 1 and r["train"]["maxdd"] > -85 and r["train"]["total"] > -100]
    best = sorted(ok, key=lambda r: -r["train"]["cagr"])[:10]
    hind = max(rows, key=lambda r: r["full"]["total"]) if rows else None
    one = [r for r in rows if r["cb"][6] == 1]
    hind1 = max(one, key=lambda r: r["full"]["total"]) if one else None
    # Abgleich: dieselben Regeln wie zwei Strategien des normalen Backtests, um die Suchrechnung zu prüfen
    ref = {"MOMF": (("mom", 14), 3, "long", "BTC über 100-Tage-Schnitt", 7, 0.0, 1),
           "H180": (("hoch", 180), 5, "long", "BTC über 200-Tage-Schnitt", 7, 0.0, 1)}
    abgl = {}
    for key, cb in ref.items():
        r = next((x for x in rows if x["cb"] == cb), None)
        if r is None:
            curve, _l, _r = simulate(cb, R, closes, members, n, first, start, HI, LO, MHI, MLO, dfund, sma)
            tot = _seg(curve, 0, len(curve))["total"]
        else:
            tot = r["full"]["total"]
        abgl[key] = dict(name=label(cb), suche=tot)
    rho = _spearman([r["train"]["cagr"] for r in rows], [r["test"]["cagr"] for r in rows])

    def pub(r, curve=False):
        o = dict(name=label(r["cb"]), suche=r["train"], gegenprobe=r["test"], gesamt=r["full"],
                 liquidiert=r["liq"], anpassungen=r["rebs"], hebel=r["cb"][6])
        if curve:
            o["kurve"] = [round(x, 2) for x in r["curve"]]
        return o

    return dict(getestet=len(rows), varianten=len(cbs), sekunden=round(time.time() - t0),
                suchzeitraum=f"{time.strftime('%d.%m.%Y', time.gmtime((start + first) * S.DAY))} bis {time.strftime('%d.%m.%Y', time.gmtime((cut - 1) * S.DAY))}",
                gegenprobe=f"{time.strftime('%d.%m.%Y', time.gmtime(cut * S.DAY))} bis {time.strftime('%d.%m.%Y', time.gmtime((start + n - 1) * S.DAY))}",
                zusammenhang=rho,
                beste=[pub(r, i < 3) for i, r in enumerate(best)],
                rueckblick=pub(hind) if hind else None,
                rueckblick_1x=pub(hind1) if hind1 else None,
                abgleich=abgl,
                gegenprobe_schnitt=round(sum(r["test"]["cagr"] for r in best) / len(best), 1) if best else None)
