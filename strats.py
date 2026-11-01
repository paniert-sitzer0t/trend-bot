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
# Korb-Strategien: Rangliste nach 24-h-Veränderung, Rotation alle 6 h (Vorwärtstest), im Backtest täglich
RMODES = {"TL": "Trend: Long auf die stärksten", "TS": "Trend: Short auf die schwächsten",
          "CS": "Gegen den Trend: Short auf die stärksten", "CL": "Gegen den Trend: Long auf die schwächsten",
          "TB": "Trend: Long stärkste, Short schwächste", "CB": "Gegen den Trend: Short stärkste, Long schwächste"}
RSIZES = (3, 5, 10, 15)
RKEYS = [f"R_{m}{n}" for m in RMODES for n in RSIZES]
for _k in RKEYS:
    NAMES[_k] = f"{_k}: {RMODES[_k[2:4]]} ({_k[4:]} Coins je Seite, 6 h)"
SLOT = 6 * 3600
# Regime-Strategien: Bärenmarkt = Bitcoin unter dem 200-Tage-Durchschnitt. (Verhalten im Aufwärtstrend, im Bärenmarkt)
RG = {"RG1": ("K", "S3"), "RG2": ("BTC", "BTC"), "RG3": ("K", "BTC"), "RG4": (None, "S3"), "RG5": (None, "BTC")}
NAMES.update({
    "RG1": "RG1: Aufwärtstrend K-Long, Bärenmarkt Short auf die 3 schwächsten Coins",
    "RG2": "RG2: Bitcoin long im Aufwärtstrend, Bitcoin short im Bärenmarkt",
    "RG3": "RG3: K-Long im Aufwärtstrend, Bitcoin short im Bärenmarkt",
    "RG4": "RG4: nur Bärenmarkt, Short auf die 3 schwächsten Coins",
    "RG5": "RG5: nur Bärenmarkt, Bitcoin short",
    "RG1_2": "RG1_2: RG1 mit 2x Hebel", "RG2_3": "RG2_3: RG2 mit 3x Hebel",
})
# Neu-Listing-Strategien: Alter = Tage seit dem ersten Handelstag auf Binance. (frühestes Alter, spätestes Alter, Seite, Max-Anteil je Coin)
NL = {"NL1": (0, 2, 1, 0.35), "NL2": (0, 13, 1, 0.35), "NL3": (0, 29, 1, 0.35), "NL4": (1, 9, -1, 0.35), "NL5": (0, 6, 1, 0.6)}
NAMES.update({
    "NL1": "NL1: Neu gelistete Coins kaufen, 3 Tage halten",
    "NL2": "NL2: Neu gelistete Coins kaufen, 14 Tage halten",
    "NL3": "NL3: Neu gelistete Coins kaufen, 30 Tage halten",
    "NL4": "NL4: Neu gelistete Coins ab Tag 2 shorten (nach dem Hype), bis Tag 10",
    "NL5": "NL5: Neu gelistete Coins kaufen, 7 Tage halten, bis 60 % je Coin",
    "NL2_3": "NL2_3: NL2 mit 3x Hebel", "NL1_5": "NL1_5: NL1 mit 5x Hebel",
})
_POOL = {}
_EXT = {}                   # Zusatzdaten für die Strategien: v (Umsatz), h/l (Hoch/Tief), f (Funding je Tag), day0
# Ideen-Strategien (recherchierte Anomalien und kreative Ansätze)
IDEAS = {
    "DON": "DON: Bitcoin Donchian-Ausbruch 20/10 (Turtle), nur Long",
    "DONLS": "DONLS: Bitcoin Donchian-Ausbruch 20/10, Long und Short",
    "GX": "GX: Bitcoin Golden Cross (50 über 200 Tage), sonst bar",
    "GXLS": "GXLS: Bitcoin Golden Cross Long, Death Cross Short",
    "RSI2": "RSI2: Bitcoin Rücksetzer kaufen (RSI 2 unter 10 im Aufwärtstrend), Ausstieg über 5-Tage-Linie",
    "VT": "VT: Bitcoin mit Volatilitätssteuerung, Ziel 50 % Schwankung, bis 1,9x Hebel",
    "HALV": "HALV: Bitcoin-Halving-Zyklus, 12 Monate vor bis 18 Monate nach dem Halving long",
    "TOM": "TOM: Monatswechsel, Bitcoin nur vom 28. bis 3. halten",
    "ETHBTC": "ETHBTC: Rotation Ethereum oder Bitcoin nach ETH/BTC-Trend",
    "DM": "DM: Dual Momentum BTC/ETH/BNB, stärkster der letzten 60 Tage, sonst bar",
    "MOM28": "MOM28: Querschnitts-Momentum 28 Tage, Top 5 Long, wöchentlich",
    "MOM7": "MOM7: Querschnitts-Momentum 7 Tage, Top 5 Long, wöchentlich",
    "TSM": "TSM: Zeitreihen-Momentum je Coin (28 Tage), Long oder Short, nach Risiko gewichtet",
    "LVOL": "LVOL: Niedrig-Volatilitäts-Anomalie, 5 ruhigste Coins Long, wöchentlich",
    "MAXLS": "MAXLS: Lotterie-Effekt, Short auf Coins mit größtem Tagessprung, Long auf die ruhigsten",
    "MAXM": "MAXM: MAX-Momentum, Long auf Coins mit größtem Tagessprung der letzten 30 Tage",
    "H180": "H180: Nähe zum 180-Tage-Hoch, Top 5 Long im Aufwärtstrend",
    "VBRK": "VBRK: Volumen-Ausbruch (3x Umsatz und +5 %), 3 Tage halten",
    "ALTS": "ALTS: Altcoin-Saison-Schalter, Altcoin-Momentum oder Bitcoin",
    "FCON": "FCON: Funding-Kontra, Short auf überhitzte Longs, Long auf überhitzte Shorts",
    "FCAR": "FCAR: Funding-Carry marktneutral (Spot long, Future short), Top 5 Funding",
    "H22": "H22: Bitcoin nur 22 bis 24 Uhr UTC halten (Tageszeit-Effekt)",
    "OVN": "OVN: Bitcoin über Nacht (21 bis 14 Uhr UTC) nach 10-Tage-Hoch, Mo/Di/Fr",
    "DON_3": "DON_3: Donchian-Ausbruch mit 3x Hebel",
    "GX_3": "GX_3: Golden Cross mit 3x Hebel",
    "H180_2": "H180_2: 180-Tage-Hoch mit 2x Hebel",
    "MOM28_T20": "MOM28_T20: Momentum 28 Tage mit Trailing-Stop 20 %",
}
NAMES.update(IDEAS)
HOURK = {"H22", "OVN"}      # stundengenaue Strategien
CARRY = {"FCAR"}            # marktneutral, verdient nur Funding
MAJORS = {"BTCUSDT", "ETHUSDT"}
SLIP_MAJOR = 0.0002         # Slippage für BTC/ETH (sehr liquide), sonst SLIP
# Trailing-Stop-Varianten: Schluessel -> (Basisstrategie, Abstand zum Hoechst- bzw. Tiefstkurs, Hebel)
TRAIL = {
    "K_T8": ("K", 0.08, 1), "K_T15": ("K", 0.15, 1), "B_T8": ("B", 0.08, 1), "B_T15": ("B", 0.15, 1),
    "C_T10": ("C", 0.10, 1), "A_T5": ("A", 0.05, 1),
    "BTC_T10": ("TREND", 0.10, 1), "BTC_T20": ("TREND", 0.20, 1),
    "R_TL5_T5": ("R_TL5", 0.05, 1), "R_TB5_T5": ("R_TB5", 0.05, 1), "R_CL5_T5": ("R_CL5", 0.05, 1),
    "R_CS5_T5": ("R_CS5", 0.05, 1), "R_TL10_T8": ("R_TL10", 0.08, 1),
    "T3_T5": ("TREND", 0.05, 3), "RG1_T10": ("RG1", 0.10, 1), "NL1_T15": ("NL1", 0.15, 1), "MOM28_T20": ("MOM28", 0.20, 1), "NL2_T20": ("NL2", 0.20, 1), "RG2_T10": ("RG2", 0.10, 1), "X10_T3": ("X", 0.03, 10),
}
_BN = {"TREND": "BTC über 200-Tage-Linie", "X": "BTC Ausbruchs-Trend"}
for _k, (_b, _p, _l) in TRAIL.items():
    NAMES[_k] = f"{_k}: {_BN.get(_b) or NAMES[_b].split(': ', 1)[1]} mit Trailing-Stop {int(_p * 100)} %" + (f" und {_l}x Hebel" if _l > 1 else "")
LEV = {"E2": 2, "E3": 3, "T3": 3, "T5": 5, "K2": 2, "K3": 3, "B2": 2, "X10": 10, "X25": 25}
BASEOF = {"E2": "E", "E3": "E", "T3": "TREND", "T5": "TREND", "K2": "K", "K3": "K", "B2": "B", "X10": "X", "X25": "X"}
for _k, (_b, _p, _l) in TRAIL.items():
    BASEOF[_k] = _b
    if _l > 1:
        LEV[_k] = _l
LEV.update({"RG1_2": 2, "RG2_3": 3, "NL2_3": 3, "NL1_5": 5, "DON_3": 3, "GX_3": 3, "H180_2": 2, "VT": 2})
BASEOF.update({"RG1_2": "RG1", "RG2_3": "RG2", "NL2_3": "NL2", "NL1_5": "NL1", "DON_3": "DON", "GX_3": "GX", "H180_2": "H180", "VT": "VTB"})
EXCLUDE = {"SOXLBUSDT", "XAUTUSDT"}   # Tokenisierte Börsen- und Rohstoffprodukte, keine normalen Coins
MM = 0.005              # Erhaltungsmarge 0,5 % des Positionswerts: darunter wird liquidiert
LEV_FUND = 0.0003       # Funding und Zinsen auf den gesamten Positionswert bei Hebel, pro Tag
MEMES = ["DOGEUSDT", "SHIBUSDT", "PEPEUSDT", "FLOKIUSDT", "BONKUSDT", "WIFUSDT", "BOMEUSDT", "MEMEUSDT", "TURBOUSDT",
         "NEIROUSDT", "PNUTUSDT", "ACTUSDT", "MOODENGUSDT", "GOATUSDT", "PENGUUSDT", "TRUMPUSDT", "POPCATUSDT",
         "MEWUSDT", "DOGSUSDT", "HMSTRUSDT", "ORDIUSDT", "1000SATSUSDT", "BRETTUSDT", "FARTCOINUSDT"]
MEME_JUMP, MEME_VOL, MEME_EXPO = 0.20, 3.0, 0.20   # Tagesplus ab 20 %, Umsatz mindestens 3x Schnitt, 20 % je Coin
PERIOD = {"A": 1, "D": 1, "B": 7, "C": 7, "E": 10 ** 9, "F": 7, "G": 7, "H": 7, "K": 7, "M": 1, "N": 1,
          "E2": 10 ** 9, "E3": 10 ** 9, "T3": 1, "T5": 1, "K2": 7, "K3": 7, "B2": 7, "X10": 1, "X25": 1}
PERIOD.update({k: 1 for k in RKEYS})
PERIOD.update({"DON": 1, "DONLS": 1, "GX": 1, "GXLS": 1, "RSI2": 1, "VT": 1, "HALV": 1, "TOM": 1, "ETHBTC": 1, "DM": 7,
               "MOM28": 7, "MOM7": 7, "TSM": 7, "LVOL": 7, "MAXLS": 7, "MAXM": 7, "H180": 7, "VBRK": 1, "ALTS": 7, "FCON": 3,
               "FCAR": 1, "H22": 1, "OVN": 1, "DON_3": 1, "GX_3": 1, "H180_2": 7})
PERIOD.update({k: 1 for k in ("NL1", "NL2", "NL3", "NL4", "NL5", "NL2_3", "NL1_5")})
PERIOD.update({"RG1": 3, "RG2": 1, "RG3": 3, "RG4": 3, "RG5": 1, "RG1_2": 3, "RG2_3": 1})
for _k, (_b, _p, _l) in TRAIL.items():
    PERIOD[_k] = 1 if _b in ("TREND", "X") else PERIOD[_b]
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
        if s in EXCLUDE or base in STABLE or (len(base) > 4 and base.endswith(("UP", "DOWN", "BULL", "BEAR"))):
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


# ---------------------------------------------------------------- Ideen-Strategien
_ST = {}


def _sma(c, i, n):
    if i < n - 1:
        return None
    w = [x for x in c[i - n + 1:i + 1] if x is not None]
    return sum(w) / len(w) if len(w) >= n * 0.8 else None


def _vol(c, i, n=30):
    r = [c[j] / c[j - 1] - 1 for j in range(max(1, i - n + 1), i + 1) if c[j] is not None and c[j - 1]]
    if len(r) < n * 0.7:
        return None
    m = sum(r) / len(r)
    return math.sqrt(sum((x - m) ** 2 for x in r) / len(r)) * math.sqrt(365)


def _stateful(name, c, i, step):
    """Zustand (z. B. investiert ja/nein), Tag für Tag fortgeschrieben, nur mit Daten bis Tag i."""
    key = (name, id(c), len(c))
    st = _ST.get(key)
    if st is None:
        if len(_ST) > 64:
            _ST.clear()
        st = _ST[key] = {}
    if i in st:
        return st[i]
    j = i
    while j > 0 and (j - 1) not in st and i - j < 400:
        j -= 1
    prev = st.get(j - 1, 0)
    for k in range(j, i + 1):
        prev = st[k] = step(c, k, prev)
    return prev


def _don_step(short):
    def f(c, k, p):
        x = c[k]
        if x is None or k < 20:
            return p
        w20 = [v for v in c[k - 20:k] if v is not None]
        w10 = [v for v in c[k - 10:k] if v is not None]
        if len(w20) < 16 or not w10:
            return p
        if x > max(w20):
            return 1
        if short and x < min(w20):
            return -1
        if p == 1 and x < min(w10):
            return 0
        if p == -1 and x > max(w10):
            return 0
        return p
    return f


def _rsi(c, k, n=2):
    g = l = 0.0
    for j in range(k - n + 1, k + 1):
        if c[j] is None or c[j - 1] is None:
            return None
        d = c[j] - c[j - 1]
        g, l = g + max(d, 0), l + max(-d, 0)
    return 100.0 if l == 0 else 100 - 100 / (1 + g / l)


def _rsi_step(c, k, p):
    x = c[k]
    if x is None or k < 200:
        return p
    if p == 0:
        r, m200 = _rsi(c, k), _sma(c, k, 200)
        return 1 if r is not None and m200 and r < 10 and x > m200 else 0
    m5 = _sma(c, k, 5)
    return 0 if m5 and x > m5 else 1


def _date(i):
    return time.gmtime(((_EXT.get("day0") or 0) + i) * DAY)


def _halvings():
    import calendar
    return [calendar.timegm(t) // DAY for t in ((2016, 7, 9, 0, 0, 0), (2020, 5, 11, 0, 0, 0), (2024, 4, 20, 0, 0, 0), (2028, 4, 15, 0, 0, 0))]


def _top(scores, n, reverse=True):
    return sorted(scores, key=scores.get, reverse=reverse)[:n]


def _maxret(c, i, n=30):
    r = [c[j] / c[j - 1] - 1 for j in range(i - n + 1, i + 1) if c[j] is not None and c[j - 1]]
    return max(r) if len(r) >= n * 0.7 else None


def _btc_bull(closes, i):
    b = closes["BTCUSDT"]
    m = _sma(b, i, 200)
    return b[i] is not None and m is not None and b[i] > m


def _fund_avg(sy, i, n=3):
    f = (_EXT.get("f") or {}).get(sy)
    if not f:
        return None
    w = [x for x in f[max(0, i - n + 1):i + 1] if x is not None]
    return sum(w) / len(w) if w else None


def carry_picks(i, n=5, min_rate=0.0003):
    sc = {}
    for sy in (_EXT.get("f") or {}):
        a = _fund_avg(sy, i)
        if a is not None and a > min_rate:
            sc[sy] = a
    return _top(sc, n)


def weights_idea(strat, closes, i):
    btc = closes["BTCUSDT"]
    if btc[i] is None:
        return {}
    if strat in ("DON", "DONLS"):
        st = _stateful(strat, btc, i, _don_step(strat == "DONLS"))
        return {"BTCUSDT": st * INVEST} if st else {}
    if strat in ("GX", "GXLS"):
        a, b = _sma(btc, i, 50), _sma(btc, i, 200)
        if a is None or b is None:
            return {}
        if a > b:
            return {"BTCUSDT": INVEST}
        return {"BTCUSDT": -INVEST} if strat == "GXLS" else {}
    if strat == "RSI2":
        return {"BTCUSDT": INVEST} if _stateful("RSI2", btc, i, _rsi_step) else {}
    if strat == "VTB":
        v = _vol(btc, i, 30)
        return {"BTCUSDT": min(INVEST, 0.25 / max(v, 0.05))} if v else {}
    if strat == "HALV":
        d = (_EXT.get("day0") or 0) + i
        return {"BTCUSDT": INVEST} if any(h - 365 <= d <= h + 540 for h in _halvings()) else {}
    if strat == "TOM":
        nxt = _date(i + 1).tm_mday
        return {"BTCUSDT": INVEST} if nxt >= 28 or nxt <= 3 else {}
    if strat == "ETHBTC":
        eth = closes.get("ETHUSDT")
        if not eth or eth[i] is None:
            return {"BTCUSDT": INVEST}
        ratio = [(e / b if e is not None and b else None) for e, b in zip(eth[max(0, i - 49):i + 1], btc[max(0, i - 49):i + 1])]
        r = [x for x in ratio if x is not None]
        if len(r) < 40:
            return {"BTCUSDT": INVEST}
        return {"ETHUSDT": INVEST} if r[-1] > sum(r) / len(r) else {"BTCUSDT": INVEST}
    if strat == "DM":
        sc = {sy: ret(closes[sy], i, 60) for sy in ("BTCUSDT", "ETHUSDT", "BNBUSDT") if sy in closes}
        sc = {k: v for k, v in sc.items() if v is not None}
        if not sc:
            return {}
        best = max(sc, key=sc.get)
        return {best: INVEST} if sc[best] > 0 else {}
    if strat in ("MOM28", "MOM7"):
        n = 28 if strat == "MOM28" else 7
        sc = {sy: r for sy, c in closes.items() if (r := ret(c, i, n)) is not None}
        if len(sc) < 10:
            return {}
        return {sy: INVEST / 5 for sy in _top(sc, 5)}
    if strat == "TSM":
        w = {}
        for sy, c in closes.items():
            r, v = ret(c, i, 28), _vol(c, i, 30)
            if r is None or not v:
                continue
            w[sy] = (1 if r > 0 else -1) / v
        tot = sum(abs(x) for x in w.values())
        return {sy: INVEST * x / tot for sy, x in w.items()} if tot else {}
    if strat == "LVOL":
        sc = {sy: v for sy, c in closes.items() if c[i] is not None and (v := _vol(c, i, 30))}
        return {sy: INVEST / 5 for sy in _top(sc, 5, reverse=False)} if len(sc) >= 10 else {}
    if strat in ("MAXLS", "MAXM"):
        sc = {sy: m for sy, c in closes.items() if c[i] is not None and (m := _maxret(c, i)) is not None}
        if len(sc) < 12:
            return {}
        if strat == "MAXM":
            return {sy: INVEST / 5 for sy in _top(sc, 5)}
        w = {sy: -INVEST / 10 for sy in _top(sc, 5)}
        w.update({sy: INVEST / 10 for sy in _top(sc, 5, reverse=False)})
        return w
    if strat == "H180":
        if not _btc_bull(closes, i):
            return {}
        sc = {}
        for sy, c in closes.items():
            win = [x for x in c[max(0, i - 179):i + 1] if x is not None]
            if c[i] is not None and len(win) >= 150:
                sc[sy] = c[i] / max(win)
        return {sy: INVEST / 5 for sy in _top(sc, 5)} if len(sc) >= 10 else {}
    if strat == "VBRK":
        vol = _EXT.get("v") or {}
        picks = set()
        for sy, c in closes.items():
            v = vol.get(sy)
            if not v:
                continue
            for j in (i, i - 1, i - 2):
                base = [x for x in v[max(0, j - 30):j] if x]
                r = ret(c, j, 1)
                if len(base) >= 20 and v[j] and r is not None and v[j] > 3 * sum(base) / len(base) and r > 0.05:
                    picks.add(sy)
                    break
        if not picks:
            return {}
        each = min(INVEST / len(picks), 0.25)
        return {sy: each for sy in picks}
    if strat == "ALTS":
        if not _btc_bull(closes, i):
            return {}
        rb = ret(btc, i, 30)
        alts = {sy: r for sy, c in closes.items() if sy != "BTCUSDT" and (r := ret(c, i, 30)) is not None}
        if rb is None or len(alts) < 10:
            return {}
        med = sorted(alts.values())[len(alts) // 2]
        if med > rb:
            return {sy: INVEST / 5 for sy in _top(alts, 5)}
        return {"BTCUSDT": INVEST}
    if strat == "FCON":
        sc = {sy: a for sy in closes if closes[sy][i] is not None and (a := _fund_avg(sy, i)) is not None}
        if len(sc) < 12:
            return {}
        w = {sy: -INVEST / 10 for sy in _top(sc, 5)}
        w.update({sy: INVEST / 10 for sy in _top(sc, 5, reverse=False)})
        return w
    return {}


def hour_weights(strat, h, hc, start_day):
    """Stundenstrategien: Entscheidung am Ende der Stunde h für die nächste Stunde."""
    nxt = (h + 1) % 24
    if strat == "H22":
        return {"BTCUSDT": INVEST} if nxt in (22, 23) else {}
    if strat == "OVN":
        b = hc.get("BTCUSDT")
        if b is None:
            return {}
        for k in range(h, max(240, h - 66), -1):
            if (k + 1) % 24 != 21:
                continue
            wd = (start_day + (k + 1) // 24 + 3) % 7          # 0 = Montag
            hold = 65 if wd == 4 else 17
            if wd not in (0, 1, 4) or h - k >= hold:
                return {}
            x = b[k]
            win = [v for v in b[k - 240:k] if v == v]
            return {"BTCUSDT": INVEST} if x == x and win and x >= max(win) else {}
        return {}
    return {}


def base_of(strat):
    while strat in BASEOF:
        strat = BASEOF[strat]
    return strat


def is_nl(strat):
    while strat in BASEOF:
        strat = BASEOF[strat]
    return strat in NL


def weights_nl(strat, i):
    """Neu gelistete Coins (aus dem globalen Pool), gleich gewichtet, je Coin höchstens `cap` des Kapitals."""
    lo, hi, side, cap = NL[strat]
    closes, first = _POOL.get("closes"), _POOL.get("first")
    if not closes:
        return {}
    picks = [sy for sy, f in first.items() if f > 0 and lo <= i - f <= hi and closes[sy][i] is not None]
    if not picks:
        return {}
    each = min(INVEST / len(picks), cap)
    return {sy: side * each for sy in picks}


def weights_rg(strat, closes, i):
    """Regime: Bitcoin über dem 200-Tage-Durchschnitt = Aufwärtstrend, darunter = Bärenmarkt."""
    btc = closes["BTCUSDT"]
    if i < 200 or btc[i] is None:
        return {}
    win = [x for x in btc[i - 199:i + 1] if x is not None]
    if len(win) < 150:
        return {}
    bull_mode, bear_mode = RG[strat]
    if btc[i] > sum(win) / len(win):
        if bull_mode == "K":
            return weights_k(closes, i)
        return {"BTCUSDT": INVEST} if bull_mode == "BTC" else {}
    if bear_mode == "BTC":
        return {"BTCUSDT": -INVEST}
    cand = {}
    for sy, c in closes.items():
        if c[i] is None or c[i - 1] is None or c[i - 31] is None or c[i - 61] is None:
            continue
        w50 = [x for x in c[i - 49:i + 1] if x is not None]
        if len(w50) < 40 or c[i] >= sum(w50) / len(w50):
            continue
        cand[sy] = ((c[i - 1] / c[i - 31] - 1) + (c[i - 1] / c[i - 61] - 1)) / 2
    low = sorted(cand, key=cand.get)[:3]
    if not low:
        return {}
    r = [btc[j] / btc[j - 1] - 1 for j in range(i - 29, i + 1) if btc[j] and btc[j - 1]]
    if len(r) < 20:
        return {}
    m = sum(r) / len(r)
    vol = math.sqrt(sum((x - m) ** 2 for x in r) / len(r)) * math.sqrt(365)
    expo = min(INVEST, 0.45 / max(vol, 0.05))
    return {sy: -expo / len(low) for sy in low}


def rank_weights(strat, rets):
    """Gleichgewichtete Körbe aus einer Rangliste (sym -> Veränderung). Gesamteinsatz 95 %."""
    mode, n = strat[2:4], int(strat[4:])
    order = sorted(rets, key=rets.get, reverse=True)
    if len(order) < 2 * n + 2:
        return {}
    top, bot = order[:n], order[-n:]
    both = mode in ("TB", "CB")
    each = INVEST / n / (2 if both else 1)
    w = {}
    if mode in ("TL", "TB"):
        w.update({x: each for x in top})
    if mode in ("TS", "TB"):
        w.update({x: -each for x in bot})
    if mode in ("CS", "CB"):
        w.update({x: -each for x in top})
    if mode in ("CL", "CB"):
        w.update({x: each for x in bot})
    return w


def weights(strat, closes, i, ctx=None):
    """Zielgewichte (positiv = Long, negativ = Short) auf Basis der Daten bis einschließlich Tag i."""
    btc = closes["BTCUSDT"]
    if strat in LEV or strat in TRAIL:
        base = weights(BASEOF[strat], closes, i, ctx)
        return {s: v * LEV.get(strat, 1) for s, v in base.items()}
    if strat in RG:
        return weights_rg(strat, closes, i)
    if strat in NL:
        return weights_nl(strat, i)
    if strat in IDEAS or strat == "VTB":
        return weights_idea(strat, closes, i)
    if strat.startswith("R_"):
        rets = {s: r for s, c in closes.items() if (r := ret(c, i, 1)) is not None}
        return rank_weights(strat, rets)
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
        self.peak, self.stops, self.slip_extra = {}, 0, 0.0
        self.carry, self.cnot = set(), 0.0

    def _px(self, px, s):
        """Kurs eines gehaltenen Coins. Fehlt er (Datenlücke), gilt der letzte bekannte Kurs statt 0.
        Sonst würde ein Short ohne Kurs das Eigenkapital künstlich aufblähen und die nächste Anpassung überhebeln."""
        lp = self.__dict__.setdefault("lp", {})
        p = px.get(s)
        if p:
            lp[s] = p
            return p
        return lp.get(s, 0)

    def gross(self, px):
        return sum(abs(q) * self._px(px, s) for s, q in self.pos.items())

    def liquidated(self, px_worst):
        """True, wenn das Eigenkapital im ungünstigsten Kurs unter die Erhaltungsmarge fällt."""
        g = self.gross(px_worst)
        return g > 0 and self.equity(px_worst) <= MM * g

    def liquidate(self):
        self.cash, self.pos, self.dead = 0.0, {}, True

    def equity(self, px):
        return self.cash + sum(q * self._px(px, s) for s, q in self.pos.items())

    def rebalance(self, px, w):
        eq = self.equity(px)
        for s in set(self.pos) | set(w):
            p = px.get(s)
            if not p:
                continue
            target = eq * w.get(s, 0) / p
            old = self.pos.get(s, 0)
            if target and (not old or (old > 0) != (target > 0)):
                self.peak[s] = p
            dq = target - old
            if abs(dq * p) < 1e-9:
                continue
            cost = abs(dq * p) * (FEE + (SLIP_MAJOR if s in MAJORS else SLIP) + self.slip_extra)
            self.cash -= dq * p + cost
            self.costs += cost
            if abs(target) < 1e-12:
                self.pos.pop(s, None)
                self.peak.pop(s, None)
            else:
                self.pos[s] = target

    def track(self, hi, lo, pct):
        """Trailing-Stop: Long folgt dem Hoechstkurs, Short dem Tiefstkurs. Gibt die Anzahl Ausstiege zurueck."""
        n = 0
        for s, q in list(self.pos.items()):
            h, l = hi.get(s), lo.get(s)
            if not h or not l:
                continue
            pk = self.peak.get(s, h if q > 0 else l)
            hit = l <= pk * (1 - pct) if q > 0 else h >= pk * (1 + pct)
            if hit:
                stop = pk * (1 - pct) if q > 0 else pk * (1 + pct)
                cost = abs(q) * stop * (FEE + (SLIP_MAJOR if s in MAJORS else SLIP) + self.slip_extra)
                self.cash += q * stop - cost
                self.costs += cost
                self.pos.pop(s)
                self.peak.pop(s, None)
                n += 1
            else:
                self.peak[s] = max(pk, h) if q > 0 else min(pk, l)
        self.stops += n
        return n

    def funding_real(self, px, rates, frac=1.0):
        """Funding mit echten Raten (positiv: Long zahlt, Short erhält). Fehlt die Rate, gilt der Standardwert."""
        for s, q in list(self.pos.items()):
            if q < 0 or self.lev:
                p = px.get(s, 0)
                r = rates.get(s)
                if r is None:
                    self.cash -= abs(q) * p * (LEV_FUND if self.lev else SHORT_FUND) * frac
                else:
                    self.cash -= q * p * r

    def funding(self, px, frac=1.0):
        if self.lev:
            self.cash -= self.gross(px) * LEV_FUND * frac
            return
        short = sum(-q * px.get(s, 0) for s, q in self.pos.items() if q < 0)
        self.cash -= short * SHORT_FUND * frac


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


def mem_mb():
    """Gesamter Arbeitsspeicher des Servers in MB (für sparsame Grenzen auf kleinen Servern)."""
    try:
        for line in open("/proc/meminfo"):
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) // 1024
    except Exception:
        pass
    return 4096


def limits():
    """(Coins im Tagesdaten-Pool, Coins mit Stundendaten) je nach Arbeitsspeicher."""
    m = mem_mb()
    if m < 1500:
        return 150, 60
    if m < 3500:
        return 250, 100
    return 400, 160


def pool(min_vol=1e6, cap=None):
    """Alle USDT-Paare auf Binance (ohne Stablecoins, gehebelte und Sonderprodukte) mit Mindestumsatz heute."""
    out = []
    for x in api("/api/v3/ticker/24hr"):
        sy = x["symbol"]
        if not sy.endswith("USDT") or sy in EXCLUDE:
            continue
        base = sy[:-4]
        if base in STABLE or (len(base) > 4 and base.endswith(("UP", "DOWN", "BULL", "BEAR"))):
            continue
        v = float(x["quoteVolume"])
        if v >= min_vol:
            out.append((v, sy))
    out.sort(reverse=True)
    syms = [sy for _, sy in out[:cap or limits()[0]]]
    if "BTCUSDT" not in syms:
        syms.append("BTCUSDT")
    return syms


def rolling_members(closes, vol, n, top=NUM, window=30):
    """Je Tag die `top` Coins mit dem höchsten Umsatz der letzten `window` Tage (nur damals bekannte Daten)."""
    import array
    pre, cnt = {}, {}
    for sy in closes:
        p, k = array.array("d", [0.0]) * (n + 1), array.array("i", [0]) * (n + 1)
        v = vol.get(sy) or []
        for d in range(n):
            x = v[d] if d < len(v) else None
            p[d + 1] = p[d] + (x or 0.0)
            k[d + 1] = k[d] + (1 if x else 0)
        pre[sy], cnt[sy] = p, k
    members = [frozenset(["BTCUSDT"])] * n
    for d in range(window, n):
        sc = []
        for sy, p in pre.items():
            k = cnt[sy][d + 1] - cnt[sy][d + 1 - window]
            if k >= 20 and closes[sy][d] is not None:
                sc.append((p[d + 1] - p[d + 1 - window], sy))
        sc.sort(reverse=True)
        m = {sy for _, sy in sc[:top]}
        m.add("BTCUSDT")
        members[d] = frozenset(m)
    return members


def run_backtest(days=2700, progress=None, data=None, hourly=None):
    dfund, note_extra, members, data_ext, ext = None, "", None, False, None
    check = None
    import hbt
    if data is None:
        syms = pool()
        closes0, ext = {}, {"v": {}, "h": {}, "l": {}}
        for c0 in range(0, len(syms), 25):
            if progress:
                progress(f"Tageskerzen {min(c0 + 25, len(syms))}/{len(syms)}")
            n0, cl, start0, ex = load_history(syms[c0:c0 + 25], days)
            closes0.update(cl)
            for key in ("v", "h", "l"):
                ext[key].update(ex[key])
        data_ext = True
        members = rolling_members(closes0, ext["v"], n0)
        if progress:
            progress("Lade Meme-Coins ...")
        mdata = load_history(MEMES, days)
        hi = dict(ext["h"]); hi.update(mdata[3]["h"])
        lo = dict(ext["l"]); lo.update(mdata[3]["l"])
        data = (n0, closes0, start0, None, mdata[1], mdata[3]["v"], hi, lo)
        cnt = {}
        for m in members[31:]:
            for sy in m:
                cnt[sy] = cnt.get(sy, 0) + 1
        hsyms = sorted(sy for sy in sorted(cnt, key=cnt.get, reverse=True)[:limits()[1]] if sy in closes0) or ["BTCUSDT"]
        if "BTCUSDT" not in hsyms:
            hsyms.append("BTCUSDT")
        import gc
        gc.collect()
        try:
            import hbt
            hourly = hbt.load_all(hsyms, days, progress)
            if hourly["start"] != start0:
                raise ValueError("Zeitraum passt nicht")
            dfund = hbt.derive_funding(hourly)
            check = hbt.sanitize(hourly, closes0)
        except Exception as e:
            hourly, dfund = None, None
            note_extra = f" Stundendaten nicht verfügbar ({e}), Tagesdaten verwendet."
    n, closes, start, _v, memes, memv, HI, LO = (data + (None, None))[:8] if len(data) == 6 else data
    ctx = (memes, memv)
    _EXT.clear()
    _EXT.update(v=(ext["v"] if data_ext else ((_v or {}).get("v") or {}) if isinstance(_v, dict) else {}), h=HI or {}, l=LO or {}, f=dfund or {}, day0=start)
    MHI, MLO = (hbt.derive_mark(hourly) if hourly is not None else ({}, {}))
    _POOL["closes"] = closes
    _POOL["first"] = {sy: next((j for j, x in enumerate(c) if x is not None), 0) for sy, c in closes.items()}
    allp = dict(closes)
    allp.update(memes)
    first = 31
    res = {}
    views = {}

    def view(i):
        if members is None:
            return closes
        v = views.get(i)
        if v is None:
            v = views[i] = {s: closes[s] for s in members[i] if s in closes}
        return v

    def lastp(s, i):
        c = allp[s]
        for j in range(i, max(-1, i - 30), -1):
            if c[j] is not None:
                return c[j]
        return 0.0

    for sn, strat in enumerate(NAMES):
        if progress:
            progress(f"Rechne {sn + 1}/{len(NAMES)}: {strat}")
        acc, curve, dayidx, rebs = Account(100.0), [], [], 0
        acc.lev = strat in LEV
        liq = None
        last_reb = -10 ** 9
        nlish = is_nl(strat)
        acc.slip_extra = 0.01 if nlish else 0.0     # neue Coins: zusätzlich 1 % Slippage je Seite
        use_h = hourly is not None and not nlish and (strat in LEV or strat in TRAIL or strat in RKEYS or strat in HOURK)
        if use_h:
            import hbt
            curve, dayidx, acc, rebs, liq = hbt.sim_hourly(strat, hourly, closes, ctx, first, members)
        for i in range(first, n - 1 if not use_h else first):
            if members is None:
                px_i = {s: c[i] for s, c in allp.items() if c[i] is not None}
            else:
                px_i = {s: closes[s][i] for s in members[i] if s in closes and closes[s][i] is not None}
                for s, c in memes.items():
                    if c[i] is not None:
                        px_i[s] = c[i]
                if nlish:
                    px_i.update({s: c[i] for s, c in closes.items() if c[i] is not None})
                for s in acc.pos:
                    if s not in px_i:
                        px_i[s] = lastp(s, i)
            if strat in CARRY:
                if i - last_reb >= PERIOD[strat]:
                    picks = set(carry_picks(i))
                    eq = acc.cash
                    note = eq * INVEST / 2 / max(len(picks), 1)
                    cost = len(picks ^ acc.carry) * note * 2 * (FEE + SLIP)
                    acc.cash -= cost
                    acc.costs += cost
                    acc.carry, acc.cnot = picks, note
                    rebs += 1
                    last_reb = i
                acc.cash += sum(acc.cnot * dfund[sy][i + 1] for sy in acc.carry
                                if dfund and sy in dfund and dfund[sy][i + 1] is not None)
                curve.append(acc.cash)
                dayidx.append(start + i + 1)
                continue
            if not acc.dead and i - last_reb >= PERIOD[strat]:
                w = sized(strat, acc, px_i, view(i), i, ctx)
                acc.rebalance(px_i, w)
                rebs += 1
                last_reb = i
            px_n = {s: (allp[s][i + 1] if allp[s][i + 1] is not None else px_i.get(s) or lastp(s, i)) for s in acc.pos}
            if not acc.dead and acc.pos and strat in TRAIL:
                hh = {sy: ((HI or {}).get(sy) or [None] * (i + 2))[i + 1] or px_n.get(sy) for sy in acc.pos}
                ll = {sy: ((LO or {}).get(sy) or [None] * (i + 2))[i + 1] or px_n.get(sy) for sy in acc.pos}
                acc.track(hh, ll, TRAIL[strat][1])
            if not acc.dead and acc.pos:
                worst = {}
                for sy, q in acc.pos.items():
                    ex = (MLO if q > 0 else MHI).get(sy) or ((LO if q > 0 else HI) or {}).get(sy)   # Mark-Preis statt Docht
                    worst[sy] = (ex[i + 1] if ex and ex[i + 1] else None) or px_n.get(sy, 0)
                if acc.liquidated(worst):
                    acc.liquidate()
                    liq = time.strftime("%d.%m.%Y", time.gmtime((start + i + 1) * DAY))
            if not acc.dead:
                if dfund:
                    rt = {sy: dfund[sy][i + 1] for sy in acc.pos if sy in dfund and dfund[sy][i + 1] is not None}
                    acc.funding_real(px_n, rt)
                else:
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
        jumps, prev_e = [], 100.0
        for d, e in zip(dayidx, curve):
            if prev_e > 0 and e / prev_e > 1.5:
                jumps.append([round((e / prev_e - 1) * 100, 1), time.strftime("%d.%m.%Y", time.gmtime(d * DAY))])
            prev_e = e
        st["ausreisser"] = sorted(jumps, reverse=True)[:5] + [list(x) for x in getattr(acc, "jumps", [])[:3]]
        st.update(years=out, costs=acc.costs, rebalances=rebs, final=curve[-1] if curve else 100.0, curve=pts,
                  leverage=LEV.get(strat, 1), stops=acc.stops, liquidated=liq, best_year=peak_year)
        res[strat] = dict(name=NAMES[strat], **st)
    return dict(ts=time.time(), days=n, coins=len(closes), universe=("mitwandernd: je Tag Top %d nach 30-Tage-Umsatz" % NUM) if members else "heutige Top %d" % NUM, start=time.strftime("%d.%m.%Y", time.gmtime(start * DAY)),
                results=res,
                hourly=bool(hourly), funding_real=bool(dfund), datenpruefung=check,
                note=("Stundenkerzen: Korb-Strategien rotieren alle 6 h, Trailing-Stops und Liquidationen werden stundengenau geprüft. "
                      if hourly is not None else "Nur Tagesdaten: Korb-Strategien werden täglich statt alle 6 h gerechnet. ")
                     + ("Funding mit echten Binance-Raten (wo vorhanden, sonst 0,03 % pro Tag). " if dfund else "Funding pauschal 0,03 % pro Tag. ")
                     + ("Universum: je Tag die 40 umsatzstärksten Coins der letzten 30 Tage aus allen heute noch gelisteten Binance-USDT-Paaren (Delistete fehlen, Rest-Survivorship-Bias). " if members else "Universum = heutige Top-Coins nach Umsatz (Survivorship-Bias begünstigt Long-Strategien). ")
                     + "Keine Parameter wurden angepasst. Kosten 0,13 % pro Seite."
                     + (f" Datenprüfung: Stundendaten von {check['korrigiert']} Coins bereinigt, {len(check['entfernt'])} Coins wegen fehlerhafter Stundendaten entfernt." if check else "")
                     + note_extra)


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

    def hour_signal(self, k):
        """Live-Version der Stundenstrategien (Uhrzeit in UTC)."""
        t = time.gmtime()
        if k == "H22":
            return {"BTCUSDT": INVEST} if t.tm_hour in (22, 23) else {}
        if k == "OVN":
            st = self.get("_ovn") or {}
            now = time.time()
            if t.tm_hour == 21 and st.get("day") != int(now // DAY):
                st = {"day": int(now // DAY), "until": 0}
                if t.tm_wday in (0, 1, 4):
                    k10 = api("/api/v3/klines", {"symbol": "BTCUSDT", "interval": "1h", "limit": 241})
                    hi = max(float(x[4]) for x in k10[:-1])
                    if self.px["BTCUSDT"] >= hi:
                        st["until"] = now + (65 if t.tm_wday == 4 else 17) * 3600 - 60
                self.put("_ovn", st)
            return {"BTCUSDT": INVEST} if st.get("until", 0) > now else {}
        return {}

    def track_listings(self):
        """Merkt sich neue USDT-Paare mit dem Tag ihres ersten Auftauchens (für die Neu-Listing-Strategien)."""
        cur = sorted(sy for sy in self.px if sy.endswith("USDT") and sy not in EXCLUDE)
        seen = self.get("_seen")
        if not seen:
            self.put("_seen", cur)
            return
        new = [sy for sy in cur if sy not in seen]
        if new:
            listed = self.get("_listed") or {}
            day = int(time.time() // DAY)
            for sy in new:
                listed[sy] = day
            self.put("_listed", listed)
            self.put("_seen", sorted(set(seen) | set(new)))

    def prices(self):
        self.px = {x["symbol"]: float(x["price"]) for x in api("/api/v3/ticker/price")}

    def step(self):
        with self.lock:
            self.prices()
            today = int(time.time() // DAY)
            done = int((self.get("_day") or 0))
            if today > done and time.time() % DAY > 600:
                days, closes, day0, _x = load_history(universe(), 260)
                try:
                    fr = {x["symbol"]: float(x["lastFundingRate"]) * 3 for x in requests.get(
                        "https://fapi.binance.com/fapi/v1/premiumIndex", timeout=20).json()}
                except Exception:
                    fr = {}
                _EXT.clear()
                _EXT.update(v=_x["v"], h=_x["h"], l=_x["l"], day0=day0,
                            f={sy: [None] * (days - 1) + [r] for sy, r in fr.items() if sy in closes})
                lst = self.get("_listed") or {}
                _POOL["closes"] = {sy: [None] * (days - 1) + [self.px[sy]] for sy in lst if sy in self.px}
                _POOL["first"] = {sy: days - 1 - (today - d) for sy, d in lst.items()}
                _d, memes, _s, mext = load_history(MEMES, 260)
                ctx = (memes, mext["v"])
                i = days - 1
                for k in NAMES:
                    if base_of(k).startswith("R_") or k in HOURK:
                        continue
                    s = self.get(k)
                    if k in CARRY:
                        if s.get("carry") and s.get("cday") and s["cday"] < today:
                            s["cash"] += sum(s["cnot"] * fr.get(sy, 0.0) * (today - s["cday"]) for sy in s["carry"])
                        picks = set(carry_picks(days - 1))
                        note = s["cash"] * INVEST / 2 / max(len(picks), 1)
                        cost = len(picks ^ set(s.get("carry") or [])) * note * 2 * (FEE + SLIP)
                        s.update(cash=s["cash"] - cost, costs=s["costs"] + cost, carry=sorted(picks), cnot=note, cday=today,
                                 rebs=s["rebs"] + 1, last_reb=today)
                        self.put(k, s)
                        continue
                    acc = Account(s["cash"], s["pos"], s.get("losses", 0), s.get("last_eq"))
                    acc.costs = s["costs"]
                    acc.lev = k in LEV
                    acc.peak = dict(s.get("peak", {}))
                    acc.slip_extra = 0.01 if is_nl(k) else 0.0
                    if s.get("dead"):
                        continue
                    if s["last_reb"] == 0 or today - s["last_reb"] >= PERIOD[k]:
                        if not (PERIOD[k] >= 10 ** 9 and s["rebs"]):
                            acc.rebalance(self.px, sized(k, acc, self.px, closes, i, ctx))
                            s["rebs"] += 1
                        s["last_reb"] = today
                    acc.funding(self.px)
                    s.update(cash=acc.cash, pos=acc.pos, costs=acc.costs, losses=acc.losses, last_eq=acc.last_eq, peak=acc.peak)
                    self.put(k, s)
                self.put("_day", today)
            self.track_listings()
            slot = int(time.time() // SLOT)
            rk = [k for k in NAMES if base_of(k).startswith("R_")]
            todo = [k for k in rk if (self.get(k).get("slot") or 0) < slot]
            if todo and self.px:
                uni = set(universe())
                tk = {x["symbol"]: float(x["priceChangePercent"]) / 100 for x in api("/api/v3/ticker/24hr")
                      if x["symbol"] in uni and x["symbol"] in self.px}
                for k in todo:
                    s = self.get(k)
                    acc = Account(s["cash"], s["pos"])
                    acc.costs = s["costs"]
                    acc.peak = dict(s.get("peak", {}))
                    if s.get("dead"):
                        continue
                    if s.get("slot"):
                        acc.funding(self.px, 0.25)
                    acc.lev = k in LEV
                    acc.rebalance(self.px, {sy: v * LEV.get(k, 1) for sy, v in rank_weights(base_of(k), tk).items()})
                    s.update(cash=acc.cash, pos=acc.pos, costs=acc.costs, slot=slot, rebs=s["rebs"] + 1, peak=acc.peak)
                    self.put(k, s)
            hslot = int(time.time() // 3600)
            for k in HOURK:                  # Stundenstrategien
                s = self.get(k)
                if (s.get("hslot") or 0) >= hslot or s.get("dead") or "BTCUSDT" not in self.px:
                    continue
                w = self.hour_signal(k)
                acc = Account(s["cash"], s["pos"])
                acc.costs = s["costs"]
                acc.rebalance(self.px, w)
                s.update(cash=acc.cash, pos=acc.pos, costs=acc.costs, hslot=hslot,
                         rebs=s["rebs"] + (1 if bool(w) != bool(s["pos"]) else 0))
                self.put(k, s)
            for k in TRAIL:                  # Trailing-Stops mit dem aktuellen Kurs
                s = self.get(k)
                if s.get("dead") or not s["pos"]:
                    continue
                a = Account(s["cash"], s["pos"])
                a.costs, a.peak = s["costs"], dict(s.get("peak", {}))
                if a.track(self.px, self.px, TRAIL[k][1]) or a.peak != s.get("peak", {}):
                    s.update(cash=a.cash, pos=a.pos, costs=a.costs, peak=a.peak, stops=s.get("stops", 0) + a.stops)
                    self.put(k, s)
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
                pos += [dict(symbol=a, side="carry", value=s.get("cnot", 0)) for a in s.get("carry") or []]
                cv = [[r[0], r[1]] for r in self.db.execute(
                    "SELECT ts, v FROM (SELECT ts, v FROM eq WHERE k=? ORDER BY ts DESC LIMIT 500) ORDER BY ts", (k,))]
                out.append(dict(key=k, name=NAMES[k], curve=cv, equity=eq, pnl=eq - self.start, pct=(eq / self.start - 1) * 100,
                                positions=pos, rebalances=s["rebs"], costs=s["costs"], lev=LEV.get(k, 1),
                                liq=s.get("liq_ts"), stops=s.get("stops", 0)))
            return out

    def run(self):
        while True:
            try:
                self.step()
            except Exception as e:
                self.msg = str(e)
                print("Vorwärtstest Fehler:", e, flush=True)
            time.sleep(60)
