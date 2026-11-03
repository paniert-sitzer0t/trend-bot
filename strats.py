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
    "MOM7F": "MOM7F: Momentum 7 Tage, Top 5 Long, nur wenn Bitcoin über 200-Tage-Schnitt, wöchentlich",
    "MOM7_T15": "MOM7_T15: Momentum 7 Tage, Top 5 Long, wöchentlich mit Trailing-Stop 15 %",
    "MOM7F_T15": "MOM7F_T15: Momentum 7 Tage, Top 5 Long, Bitcoin-Filter 200 Tage und Trailing-Stop 15 %",
    "MOMF2": "MOMF2: Momentum 14 Tage, Top 3 Long, nur wenn Bitcoin über 200-Tage-Schnitt, wöchentlich",
    "MOMF_T15": "MOMF_T15: Momentum 14 Tage, Top 3, Bitcoin-Filter 100 Tage, Trailing-Stop 15 %",
    "MOMF2_T15": "MOMF2_T15: Momentum 14 Tage, Top 3, Bitcoin-Filter 200 Tage, Trailing-Stop 15 %",
    "MOMF": "MOMF: Momentum 14 Tage, Top 3 Long, nur wenn Bitcoin über 100-Tage-Schnitt, wöchentlich (aus der Strategie-Suche)",
    "H365F": "H365F: Nähe 365-Tage-Hoch, Top 3 Long, nur wenn Bitcoin über 100-Tage-Schnitt, alle 3 Tage (aus der Strategie-Suche)",
    "H180F": "H180F: Nähe 180-Tage-Hoch, Top 5 Long, nur wenn Bitcoin über 100-Tage-Schnitt, alle 3 Tage (aus der Strategie-Suche, beste ohne Hebel)",
    "H365F_2": "H365F_2: wie H365F mit 2x Hebel (die rückblickend beste Variante der Suche)",
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
# Misch-Strategien: verteilen das Kapital auf andere Strategien statt selbst Coins zu wählen
MIX = {
    "MIXALL": "MIXALL: Alle Strategien ohne Hebel zu gleichen Teilen, nie umgeschichtet",
    "MIX5": "MIX5: Jeden Monat die 5 Strategien mit dem besten Rendite-Risiko-Verhältnis der letzten 180 Tage",
    "MIX10": "MIX10: Jeden Monat die 10 Strategien mit dem besten Rendite-Risiko-Verhältnis der letzten 365 Tage",
}
MIXCFG = {"MIX5": (5, 180), "MIX10": (10, 365)}
NAMES.update(MIX)
HOURK = {"H22", "OVN"}      # stundengenaue Strategien
CARRYCFG = {"FCAR": (5, 0.0003), "FCAR3": (3, 0.0006), "FCAR10": (10, 0.0002)}   # (Anzahl Coins, Mindest-Funding je Tag)
CARRY = set(CARRYCFG)       # marktneutral, verdient nur Funding
MAJORS = {"BTCUSDT", "ETHUSDT"}
SLIP_MAJOR = 0.0002         # Slippage für BTC/ETH (sehr liquide), sonst SLIP
# Trailing-Stop-Varianten: Schluessel -> (Basisstrategie, Abstand zum Hoechst- bzw. Tiefstkurs, Hebel)
TRAIL = {
    "MOMF_T15": ("MOMF", 0.15, 1), "MOMF2_T15": ("MOMF2", 0.15, 1),
    "MOM7_T15": ("MOM7", 0.15, 1), "MOM7F_T15": ("MOM7F", 0.15, 1),
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
LEV.update({"RG1_2": 2, "RG2_3": 3, "NL2_3": 3, "NL1_5": 5, "DON_3": 3, "GX_3": 3, "H180_2": 2, "VT": 2, "H365F_2": 2})
BASEOF.update({"RG1_2": "RG1", "RG2_3": "RG2", "NL2_3": "NL2", "NL1_5": "NL1", "DON_3": "DON", "GX_3": "GX", "H180_2": "H180", "H365F_2": "H365F", "VT": "VTB"})
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
               "MOM28": 7, "MOM7": 7, "MOM7F": 7, "MOMF": 7, "MOMF2": 7, "MIXALL": 30, "MIX5": 30, "MIX10": 30, "TSM": 7, "LVOL": 7, "MAXLS": 7, "MAXM": 7, "H180": 7, "H365F": 3, "H365F_2": 3, "H180F": 3, "VBRK": 1, "ALTS": 7, "FCON": 3,
               "FCAR": 1, "H22": 1, "OVN": 1, "DON_3": 1, "GX_3": 1, "H180_2": 7})
PERIOD.update({k: 1 for k in ("NL1", "NL2", "NL3", "NL4", "NL5", "NL2_3", "NL1_5")})
PERIOD.update({"RG1": 3, "RG2": 1, "RG3": 3, "RG4": 3, "RG5": 1, "RG1_2": 3, "RG2_3": 1})
for _k, (_b, _p, _l) in TRAIL.items():
    PERIOD[_k] = 1 if _b in ("TREND", "X") else PERIOD[_b]
# Adaptive Strategien: Der Bot erkennt anhand von Bitcoin den Markt-Zustand und wechselt selbst zwischen Long, Short und bar.
#   AD2 (zwei Zustände): Bitcoin über dem Schnitt = Long auf die stärksten k Coins, darunter = Short auf die schwächsten k.
#   AD3 (drei Zustände): Long nur, wenn Bitcoin über Schnitt UND 50-Tage-Linie liegt, Short nur, wenn er unter beiden liegt, dazwischen bar.
# Rangfolge nach Momentum der letzten 14 Tage, wöchentlich neu. Varianten: Coin-Anzahl k (3, 10), Bitcoin-Schnitt (100, 200 Tage).
ADAPT = {}
for _t in (2, 3):
    for _n in (3, 10):
        for _m in (100, 200):
            ADAPT[f"AD{_t}_{_n}_{_m}"] = (_t, _n, _m)
for _k, (_t, _n, _m) in ADAPT.items():
    _z = "zwei Zustände" if _t == 2 else "drei Zustände, dazwischen bar"
    NAMES[_k] = f"{_k}: Adaptiv ({_z}): Long auf die Top {_n} im Aufwärtstrend, Short auf die schwächsten {_n} im Abwärtstrend, Bitcoin-Schnitt {_m} Tage, Momentum 14 Tage, wöchentlich"
    PERIOD[_k] = 7


def weights_adapt(strat, closes, i):
    t, k, m = ADAPT[strat]
    b = closes.get("BTCUSDT") or _POOL.get("closes", {}).get("BTCUSDT")
    sm = _sma(b, i, m) if b else None
    if not (sm and b[i]):
        return {}
    state = 1 if b[i] > sm else -1
    if t == 3:
        s50 = _sma(b, i, 50)
        if not s50 or (state == 1) != (b[i] > s50):
            return {}
    sc = {sy: r for sy, c in closes.items() if (r := ret(c, i, 14)) is not None}
    if len(sc) < 10:
        return {}
    if state == 1:
        return {sy: INVEST / k for sy in _top(sc, k)}
    return {sy: -INVEST / k for sy in _top(sc, k, reverse=False)}


# Martingale mit Faktor: Der Einsatz startet bei 35 % des Kapitals und wächst nach jeder Verlustperiode um den Faktor
# (zum Beispiel x1,2 = 20 % mehr), höchstens bis zur Stufe "max". Nach einer Gewinnperiode springt er auf 35 % zurück.
# Gedeckelt bei 100 % des Kapitals, nie Hebel. Grundregel dahinter: B (Long+Short), MOMF (Long mit BTC-Filter), AD2_3_200 (adaptiv).
MG = {}
_MGN = {"B": "Momentum Long+Short", "MOMF": "Momentum 14 Tage, Top 3 mit BTC-Filter", "AD2_3_200": "Adaptiv Long/Short"}
for _b in ("B", "MOMF", "AD2_3_200"):
    for _f in (1.2, 1.5):
        for _mx in (3, 6):
            MG[f"MG_{_b}_{int(round(_f * 10))}_{_mx}"] = (_b, _f, _mx)
for _k, (_b, _f, _mx) in MG.items():
    NAMES[_k] = f"{_k}: {_MGN[_b]} mit Martingale x{str(_f).replace('.', ',')} nach Verlustperiode, höchstens Stufe {_mx} (Einsatz 35 % bis {min(100, int(round(35 * _f ** _mx)))} %)"
    PERIOD[_k] = PERIOD[_b]


# Gleichmäßig wachsende Strategien: viele kleine Wetten, Einsatz nach Schwankung, Marktschutz.
CALM = {}
for _n in (10, 15):
    CALM[f"RPK{_n}"] = (f"Risikoparität-Korb: die {_n} größten Coins, gewichtet nach 1/Schwankung, nur wenn Bitcoin über der 200-Tage-Linie", 7)
for _k in (3, 5, 10):
    CALM[f"MZR{_k}"] = (f"Momentum mit Zielrisiko: Top {_k} nach 14-Tage-Momentum, Einsatz sinkt bei hoher Schwankung (Ziel 40 % pro Jahr), nur wenn Bitcoin über der 100-Tage-Linie", 7)
CALM["TDMB"] = ("Trend-Dimmer Bitcoin: Einsatz 0 bis 100 % in Fünftel-Stufen nach fünf Trendsignalen von Bitcoin", 3)
CALM["TDMK"] = ("Trend-Dimmer Korb: wie TDMB, aber verteilt auf die Top 5 nach 14-Tage-Momentum", 3)
for _k in (5, 10):
    CALM[f"MNL{_k}"] = (f"Marktneutral: Top {_k} Long und schwächste {_k} Short nach 14-Tage-Momentum, gleich viel (Short nur simuliert)", 7)
for _n in (10, 20):
    CALM[f"TJC{_n}"] = (f"Trendfolger je Coin: die {_n} größten Coins einzeln long, solange sie über ihrer eigenen 100-Tage-Linie liegen, nach 1/Schwankung gewichtet", 7)
for _t in (10, 15):
    CALM[f"RSB{_t}"] = (f"Rücksetzer im Aufwärtstrend: Coin fällt in 3 Tagen um über {_t} %, liegt aber über seiner 50-Tage-Linie und Bitcoin über der 100-Tage-Linie, 3 Tage halten", 1)
for _n in (10, 20):
    CALM[f"RBP{_n}"] = (f"Rebalancing-Prämie: die {_n} größten Coins zu gleichen Teilen, täglich auf Gleichstand zurück, nur wenn Bitcoin über der 100-Tage-Linie", 1)
CALM["KS1"] = ("Kern und Satelliten: 50 % Bitcoin-Trend, 30 % H180F, 20 % Risikoparität-Korb, wöchentlich auf diese Anteile zurück", 3)
CALM["KS2"] = ("Kern und Satelliten: 40 % Bitcoin-Trend, 40 % MOMF, 20 % Risikoparität-Korb, wöchentlich auf diese Anteile zurück", 3)
KSMIX = {"KS1": (("TREND", 0.5), ("H180F", 0.3), ("RPK10", 0.2)), "KS2": (("TREND", 0.4), ("MOMF", 0.4), ("RPK10", 0.2))}
BRAKE = {"BRK_H180F": ("H180F", 0.15, 0.5), "BRK_MOMF": ("MOMF", 0.15, 0.5)}     # (Basis, Rückgang vom Höchststand, Einsatzfaktor)
for _k, (_t, _p) in CALM.items():
    NAMES[_k] = f"{_k}: {_t}"
    PERIOD[_k] = _p
for _k, (_b, _d, _f) in BRAKE.items():
    NAMES[_k] = f"{_k}: {NAMES[_b].split(': ', 1)[1].split(' (')[0]} mit Verlust-Bremse: fällt das Konto {int(_d * 100)} % vom Höchststand, halbiert sich der Einsatz bis zur Erholung"
    PERIOD[_k] = PERIOD[_b]
for _k in ("FCAR3", "FCAR10"):
    NAMES[_k] = f"{_k}: Funding-Carry marktneutral, " + ("Top 3 mit Funding über 0,06 % pro Tag" if _k == "FCAR3" else "Top 10 mit Funding über 0,02 % pro Tag")
    PERIOD[_k] = 1


def _bign(closes, i, n):
    """Die n größten Coins nach durchschnittlichem Umsatz der letzten 30 Tage (nur Daten bis Tag i)."""
    vol = _EXT.get("v") or {}
    sc = {}
    for sy, c in closes.items():
        if c[i] is None:
            continue
        v = vol.get(sy)
        w = [x for x in (v[max(0, i - 29):i + 1] if v else []) if x]
        sc[sy] = sum(w) / len(w) if w else 0.0
    return _top(sc, n)


def _bull(closes, i, n):
    b = closes.get("BTCUSDT") or _POOL.get("closes", {}).get("BTCUSDT")
    m = _sma(b, i, n) if b else None
    return bool(m and b[i] and b[i] > m)


def _invvol(closes, i, syms, total):
    iv = {}
    for sy in syms:
        v = _vol(closes[sy], i, 30)
        if v:
            iv[sy] = 1.0 / max(v, 0.005)
    t = sum(iv.values())
    return {sy: total * x / t for sy, x in iv.items()} if t else {}


def weights_calm(strat, closes, i, ctx=None):
    if strat in KSMIX:
        out = {}
        for base, share in KSMIX[strat]:
            for sy, v in weights(base, closes, i, ctx).items():
                out[sy] = out.get(sy, 0.0) + v * share
        return out
    kind = strat.rstrip("0123456789")
    num = int(strat[len(kind):] or 0)
    if kind == "RPK":
        if not _bull(closes, i, 200):
            return {}
        return _invvol(closes, i, _bign(closes, i, num), INVEST)
    if kind in ("MZR", "MNL", "TDMK"):
        sc = {sy: r for sy, c in closes.items() if (r := ret(c, i, 14)) is not None}
        if len(sc) < 12:
            return {}
    if kind == "MZR":
        if not _bull(closes, i, 100):
            return {}
        picks = _top(sc, num)
        vs = [v for sy in picks if (v := _vol(closes[sy], i, 30))]
        if not vs:
            return {}
        expo = min(INVEST, 0.40 / (0.8 * (sum(vs) / len(vs)) * math.sqrt(365)))
        return {sy: expo / num for sy in picks}
    if kind == "MNL":
        w = {sy: INVEST / (2 * num) for sy in _top(sc, num)}
        w.update({sy: -INVEST / (2 * num) for sy in _top(sc, num, reverse=False)})
        return w
    if kind in ("TDMB", "TDMK"):
        b = closes.get("BTCUSDT") or _POOL.get("closes", {}).get("BTCUSDT")
        if not b or b[i] is None:
            return {}
        s50, s100, s200 = _sma(b, i, 50), _sma(b, i, 100), _sma(b, i, 200)
        r30 = ret(b, i, 30)
        pts = sum([bool(s50 and b[i] > s50), bool(s100 and b[i] > s100), bool(s200 and b[i] > s200),
                   bool(s50 and s200 and s50 > s200), bool(r30 is not None and r30 > 0)])
        if not pts:
            return {}
        f = pts / 5
        if kind == "TDMB":
            return {"BTCUSDT": INVEST * f}
        return {sy: INVEST * f / 5 for sy in _top(sc, 5)}
    if kind == "TJC":
        coins = _bign(closes, i, num)
        w = _invvol(closes, i, coins, INVEST)
        out = {}
        for sy, x in w.items():
            m = _sma(closes[sy], i, 100)
            if m and closes[sy][i] > m:
                out[sy] = x
        return out
    if kind == "RSB":
        if not _bull(closes, i, 100):
            return {}
        thr, picks = num / 100, {}
        for sy, c in closes.items():
            m = _sma(c, i, 50)
            for j in (i, i - 1, i - 2):
                r = ret(c, j, 3) if j >= 3 else None
                if r is not None and r < -thr and m and c[i] is not None and c[i] > m:
                    picks[sy] = r
                    break
        if not picks:
            return {}
        sel = _top(picks, 5, reverse=False)
        each = min(INVEST / len(sel), 0.2)
        return {sy: each for sy in sel}
    if kind == "RBP":
        if not _bull(closes, i, 100):
            return {}
        coins = _bign(closes, i, num)
        return {sy: INVEST / len(coins) for sy in coins} if len(coins) >= 5 else {}
    return {}


# Jede Strategie ohne Hebel bekommt automatisch eine Trailing-Variante (Körbe mit 6-h-Rotation 5 %, sonst 15 %).
# Ausgenommen: Strategien, die schon eine haben, Hebel- und Mischungs-Strategien, Stunden- und Carry-Strategien
# sowie Bitcoin halten (wird nie umgeschichtet, nach einem Stopp bliebe das Konto leer).
TRAIL_AUTO = {}
_has = {b for (b, p, l) in TRAIL.values() if l == 1}
for _b in list(NAMES):
    if (_b in BASEOF or _b in LEV or _b in MIX or _b in HOURK or _b in CARRY or _b in _has
            or _b not in PERIOD or PERIOD[_b] >= 10 ** 9):
        continue
    _p = 0.05 if _b.startswith("R_") else 0.15
    _k = f"{_b}_T{int(_p * 100)}"
    if _k in NAMES:
        continue
    TRAIL[_k] = (_b, _p, 1)
    TRAIL_AUTO[_k] = _b
    BASEOF[_k] = _b
    PERIOD[_k] = PERIOD[_b]
    NAMES[_k] = f"{_k}: {NAMES[_b].split(': ', 1)[1]} mit Trailing-Stop {int(_p * 100)} %"
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
    if strat == "MOM7F":
        b = closes.get("BTCUSDT") or _POOL.get("closes", {}).get("BTCUSDT")
        m = _sma(b, i, 200) if b else None
        if not (m and b[i] and b[i] > m):
            return {}
        sc = {sy: r for sy, c in closes.items() if (r := ret(c, i, 7)) is not None}
        if len(sc) < 10:
            return {}
        return {sy: INVEST / 5 for sy in _top(sc, 5)}
    if strat in ("H365F", "H180F"):
        W, K = (365, 3) if strat == "H365F" else (180, 5)
        b = closes.get("BTCUSDT") or _POOL.get("closes", {}).get("BTCUSDT")
        m = _sma(b, i, 100) if b else None
        if not (m and b[i] and b[i] > m):
            return {}
        sc = {}
        for sy, c in closes.items():
            win = [x for x in c[max(0, i - W + 1):i + 1] if x is not None]
            if c[i] is not None and len(win) >= 0.8 * W:
                sc[sy] = c[i] / max(win)
        return {sy: INVEST / K for sy in _top(sc, K)} if len(sc) >= 10 else {}
    if strat in ("MOMF", "MOMF2"):
        b = closes.get("BTCUSDT") or _POOL.get("closes", {}).get("BTCUSDT")
        m = _sma(b, i, 100 if strat == "MOMF" else 200) if b else None
        if not (m and b[i] and b[i] > m):
            return {}
        sc = {sy: r for sy, c in closes.items() if (r := ret(c, i, 14)) is not None}
        if len(sc) < 10:
            return {}
        return {sy: INVEST / 3 for sy in _top(sc, 3)}
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
    if strat in MG:
        return weights(MG[strat][0], closes, i, ctx)
    if strat in CALM:
        return weights_calm(strat, closes, i, ctx)
    if strat in BRAKE:
        return weights(BRAKE[strat][0], closes, i, ctx)
    if strat in ADAPT:
        return weights_adapt(strat, closes, i)
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
    bk = strat if strat in BRAKE else (BASEOF.get(strat) if BASEOF.get(strat) in BRAKE else None)
    if bk:
        eq = acc.equity(px)
        pk = max(acc.peak.get("_eq", eq), eq)
        acc.peak["_eq"] = pk
        on = acc.peak.get("_br", 0)
        if not on and eq < pk * (1 - BRAKE[bk][1]):
            on = 1
        elif on and eq >= pk * (1 - BRAKE[bk][1] / 2):
            on = 0
        acc.peak["_br"] = on
        if on:
            w = {s: v * BRAKE[bk][2] for s, v in w.items()}
    mk = strat if strat in MARTI else (BASEOF.get(strat) if BASEOF.get(strat) in MARTI else None)
    gk = strat if strat in MG else (BASEOF.get(strat) if BASEOF.get(strat) in MG else None)
    if mk or gk:
        eq = acc.equity(px)
        if acc.last_eq is not None:
            acc.losses = acc.losses + 1 if eq < acc.last_eq else 0
        acc.last_eq = eq
        if gk:
            exposure = min(INVEST, BASE * MG[gk][1] ** min(acc.losses, MG[gk][2]))
        else:
            exposure = min(INVEST, BASE * MARTI[mk] ** acc.losses)
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
    # Gleichmäßigkeit: wie gerade die Kurve (logarithmisch) wächst, R² der Regression, 0 bis 100. Längste Flaute: Monate unter dem früheren Höchststand.
    smooth = 0.0
    if min(c) > 0 and c[-1] > c[0]:
        ys = [math.log(x) for x in c]
        n = len(ys)
        mx_, my_ = (n - 1) / 2, sum(ys) / n
        sxx = sum((k - mx_) ** 2 for k in range(n))
        sxy = sum((k - mx_) * (y - my_) for k, y in enumerate(ys))
        syy = sum((y - my_) ** 2 for y in ys)
        smooth = max(0.0, (sxy * sxy) / (sxx * syy) * 100) if sxx and syy else 0.0
    pk, run, longest = c[0], 0, 0
    for x in c:
        if x >= pk:
            pk, run = x, 0
        else:
            run += 1
            longest = max(longest, run)
    return dict(total=(c[-1] / c[0] - 1) * 100, cagr=((c[-1] / c[0]) ** (1 / yrs) - 1) * 100 if c[-1] > 0 else -100,
                maxdd=mdd * 100, sharpe=mean / sd * math.sqrt(days_per_year), smooth=round(smooth, 1), flaute=round(longest / 30.4, 1))


def mem_available_mb():
    """Aktuell frei verfügbarer Arbeitsspeicher in MB (None, wenn nicht lesbar)."""
    try:
        for line in open("/proc/meminfo"):
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) // 1024
    except Exception:
        pass
    return None


def sysinfo():
    """Kennzahlen des Servers für das Dashboard: Arbeitsspeicher, Auslastung, Speicherplatz, Datenbankgrößen."""
    out = {}
    try:
        mem = {}
        for line in open("/proc/meminfo"):
            k, v = line.split(":", 1)
            mem[k] = int(v.split()[0]) // 1024
        out.update(ram_gesamt_mb=mem.get("MemTotal"), ram_frei_mb=mem.get("MemAvailable"), swap_mb=mem.get("SwapTotal"))
    except Exception:
        pass
    try:
        for line in open("/proc/self/status"):
            if line.startswith("VmRSS:"):
                out["prozess_mb"] = int(line.split()[1]) // 1024
    except Exception:
        pass
    try:
        out["last"] = [round(x, 2) for x in os.getloadavg()]
        out["kerne"] = os.cpu_count()
    except Exception:
        pass
    try:
        st = os.statvfs(".")
        out.update(disk_frei_gb=round(st.f_bavail * st.f_frsize / 1e9, 1), disk_gesamt_gb=round(st.f_blocks * st.f_frsize / 1e9, 1))
    except Exception:
        pass
    sizes = {}
    for f in ("fwd.db", "candles.db", "trend.db", "backtest.json"):
        try:
            sizes[f] = round(os.path.getsize(f) / 1e6, 1)
        except Exception:
            pass
    out["dateien_mb"] = sizes
    return out


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


def ev_intervals(ev, now):
    """Aus Ein-/Ausstiegs-Ereignissen [[ts, investiert], ...] die investierten Zeiträume machen."""
    out, a = [], None
    for ts, f in ev:
        if f and a is None:
            a = ts
        elif not f and a is not None:
            out.append([a, ts])
            a = None
    if a is not None:
        out.append([a, now])
    return out


def intervals(xs, flags, cap=120):
    """Zeiträume, in denen eine Strategie investiert war, als [[von, bis], ...] (gleiche Einheit wie xs).
    Kurze Lücken werden zusammengefasst, bis höchstens `cap` Zeiträume übrig sind."""
    raw, a = [], None
    for x, f in zip(xs, flags):
        if f and a is None:
            a = x
        elif not f and a is not None:
            raw.append([a, x])
            a = None
    if a is not None and xs:
        raw.append([a, xs[-1]])
    if not raw:
        return []
    step = (xs[-1] - xs[0]) / max(len(xs) - 1, 1) if len(xs) > 1 else 1
    gap = step
    out = raw
    while True:
        out = [list(raw[0])]
        for b0, b1 in raw[1:]:
            if b0 - out[-1][1] <= gap:
                out[-1][1] = b1
            else:
                out.append([b0, b1])
        if len(out) <= cap:
            return out
        gap *= 2


def mix_pool():
    """Strategien, auf die die Misch-Strategien verteilen dürfen: ohne Hebel, ohne andere Mischungen."""
    return [k for k in NAMES if k not in MIX and k not in TRAIL_AUTO and k not in MG and BASEOF.get(k) not in MG and LEV.get(k, 1) == 1]


def mix_pick(series, i, k, look):
    """Die k Strategien mit dem besten Sharpe-Wert der letzten `look` Tage bis Tag i (nur Vergangenheit)."""
    sc = {}
    for key, c in series.items():
        if i - look < 0 or not c[i] or c[i] <= 0:
            continue
        r = [c[j] / c[j - 1] - 1 for j in range(i - look + 1, i + 1) if c[j - 1] and c[j - 1] > 0 and c[j] is not None]
        if len(r) < 0.8 * look:
            continue
        m = sum(r) / len(r)
        sd = math.sqrt(sum((x - m) ** 2 for x in r) / len(r))
        if sd < 1e-6:
            continue
        sc[key] = m / sd
    return sorted(sc, key=sc.get, reverse=True)[:k]


def mix_curve(key, series, L, period=30):
    """Equity-Kurve einer Misch-Strategie aus den Tageskurven der Einzelstrategien.
    Zwischen den Anpassungen laufen die Anteile mit (wie echtes Kapital), Umschichtungen kosten Gebühren."""
    eq, w, curve, rebs, costs, last_reb = 100.0, {}, [], 0, 0.0, -10 ** 9
    cost_rate = FEE + SLIP
    for i in range(L):
        if i > 0 and w:
            r = {k: (series[k][i] / series[k][i - 1] - 1) if series[k][i - 1] and series[k][i - 1] > 0 and series[k][i] is not None else 0.0
                 for k in w}
            g = sum(w[k] * (1 + r[k]) for k in w)
            eq *= g
            w = {k: w[k] * (1 + r[k]) / g for k in w} if g > 0 else {}
        if key == "MIXALL":
            if not w and i > 0:
                live = [k for k in series if series[k][i]]
                w = {k: 1 / len(live) for k in live} if live else {}
                costs += eq * cost_rate
                eq -= eq * cost_rate
                rebs += 1
        elif i - last_reb >= period:
            k, look = MIXCFG[key]
            sel = mix_pick(series, i, k, look)
            if sel:
                nw = {x: 1 / len(sel) for x in sel}
                turn = sum(abs(nw.get(x, 0) - w.get(x, 0)) for x in set(nw) | set(w))
                c = eq * turn * cost_rate
                eq -= c
                costs += c
                w = nw
                rebs += 1
                last_reb = i
        curve.append(eq)
    return curve, rebs, costs, w


def clean_daily(closes, ext=None, up=10.0, spike=4.0):
    """Prüft Tageskerzen auf Datenfehler.
    1) Ausreißer-Tag: Kurs springt um Faktor 4 oder mehr und ist am nächsten Tag wieder zurück: Tag verwerfen.
    2) Dauerhafter Sprung um Faktor 10 oder mehr an einem Tag: typisch für Umstellung oder neu vergebenes Kürzel.
       Die Kurse davor gehören zu einem anderen Wert und werden verworfen (der Coin gilt ab da als neu).
    Starke Abstürze bleiben unangetastet, damit Verluste nicht verschwinden. Gibt die betroffenen Coins zurück."""
    rep = {}
    for sy, c in closes.items():
        n = len(c)
        for i in range(1, n - 1):
            a, x, z = c[i - 1], c[i], c[i + 1]
            if a and x and z and (x / a >= spike or a / x >= spike) and 0.67 < z / a < 1.5:
                c[i] = None
                rep.setdefault(sy, []).append("Ausreißer " + time.strftime("%d.%m.%Y", time.gmtime((int(time.time() // DAY) - n + i) * DAY)))
        prev = None
        for i in range(n):
            x = c[i]
            if x is None:
                continue
            if prev and x / prev >= up:
                for j in range(i):
                    c[j] = None
                    if ext:
                        for key in ("v", "h", "l"):
                            arr = ext.get(key, {}).get(sy)
                            if arr is not None and j < len(arr):
                                arr[j] = None
                rep.setdefault(sy, []).append("Sprung x%.0f am %s, Daten davor verworfen" % (x / prev, time.strftime("%d.%m.%Y", time.gmtime((int(time.time() // DAY) - n + i) * DAY))))
            prev = x
    return rep


def run_backtest(days=2700, progress=None, data=None, hourly=None):
    dfund, note_extra, members, data_ext, ext = None, "", None, False, None
    check = None
    daily_check = {}
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
        daily_check = clean_daily(closes0, ext)
        members = rolling_members(closes0, ext["v"], n0)
        if progress:
            progress("Lade Meme-Coins ...")
        mdata = load_history(MEMES, days)
        daily_check.update(clean_daily(mdata[1], mdata[3]))
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

    keys = [k for k in NAMES if k not in MIX] + (["ORIG"] if hourly is not None else [])
    d0 = start + first + 1
    L = n - 1 - first
    series = {}
    for sn, strat in enumerate(keys):
        if progress:
            progress(f"Rechne {sn + 1}/{len(keys)}: {strat}")
        acc, curve, dayidx, rebs = Account(100.0), [], [], 0
        inv = []
        acc.lev = strat in LEV
        liq = None
        last_reb = -10 ** 9
        nlish = is_nl(strat)
        acc.slip_extra = 0.01 if nlish else 0.0     # neue Coins: zusätzlich 1 % Slippage je Seite
        use_h = hourly is not None and not nlish and (strat in LEV or strat in RKEYS or strat in HOURK
                                                     or (strat in TRAIL and base_of(strat).startswith("R_")))
        if strat == "ORIG":
            import hbt
            use_h = True
            curve, dayidx, acc, rebs = hbt.sim_orig(hourly, closes, HI, LO, first, members)
            inv = acc.inv
        elif use_h:
            import hbt
            curve, dayidx, acc, rebs, liq = hbt.sim_hourly(strat, hourly, closes, ctx, first, members)
            inv = acc.inv
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
                    picks = set(carry_picks(i, *CARRYCFG[strat]))
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
                inv.append(bool(acc.carry))
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
            inv.append(bool(acc.pos) and not acc.dead)
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
        st["investiert"] = intervals(dayidx, inv) if len(inv) == len(dayidx) and strat not in HOURK else None
        st.update(years=out, costs=acc.costs, rebalances=rebs, final=curve[-1] if curve else 100.0, curve=pts,
                  leverage=LEV.get(strat, 1), stops=acc.stops, liquidated=liq, best_year=peak_year)
        res[strat] = dict(name=NAMES.get(strat, "Hauptbot: Top-Mover mit 8-h-Rotation"), **st)
        if strat in mix_pool():
            import array as _arr
            a = _arr.array("f", [float("nan")]) * L
            for d, e in zip(dayidx, curve):
                if 0 <= d - d0 < L:
                    a[d - d0] = e
            series[strat] = a
    ser = {k: [None if x != x else x for x in a] for k, a in series.items()}
    series.clear()
    mix_now = {}
    for strat in MIX:
        curve, rebs, costs, w = mix_curve(strat, ser, L)
        dayidx = [d0 + j for j in range(L)]
        mix_now[strat] = sorted(w, key=w.get, reverse=True)
        st = stats([100.0] + curve)
        years, prev = {}, 100.0
        for d, e in zip(dayidx, curve):
            y = time.gmtime(d * DAY).tm_year
            years.setdefault(y, [e, e])
            years[y][1] = e
        out = {}
        for y in sorted(years):
            out[str(y)] = (years[y][1] / prev - 1) * 100 if prev > 0 else 0.0
            prev = years[y][1]
        pts = [[d, round(e, 2)] for k2, (d, e) in enumerate(zip(dayidx, curve)) if k2 % 7 == 0]
        st.update(years=out, costs=costs, rebalances=rebs, final=curve[-1] if curve else 100.0, curve=pts, leverage=1,
                  stops=0, liquidated=None, best_year=max(out.values()) if out else 0.0, ausreisser=[],
                  auswahl=[[k, round(w[k] * 100, 1)] for k in mix_now[strat]][:12])
        res[strat] = dict(name=NAMES[strat], **st)
    del ser
    optim = None
    if members is not None:
        try:
            import gc, opt
            gc.collect()
            optim = opt.search(closes, members, n, start, HI, LO, MHI, MLO, dfund, progress, first)
            for key, a in (optim.get("abgleich") or {}).items():
                a["backtest"] = round(res[key]["total"], 1) if key in res else None
        except Exception as e:
            optim = dict(fehler=str(e)[:300])
    return dict(ts=time.time(), days=n, coins=len(closes), optimierung=optim, mix_auswahl=mix_now, universe=("mitwandernd: je Tag Top %d nach 30-Tage-Umsatz" % NUM) if members else "heutige Top %d" % NUM, start=time.strftime("%d.%m.%Y", time.gmtime(start * DAY)),
                results=res,
                hourly=bool(hourly), funding_real=bool(dfund), datenpruefung=dict(check or {}, tagesdaten=daily_check),
                note=("Stundenkerzen: Korb-Strategien rotieren alle 6 h, Trailing-Stops und Liquidationen werden stundengenau geprüft. "
                      if hourly is not None else "Nur Tagesdaten: Korb-Strategien werden täglich statt alle 6 h gerechnet. ")
                     + ("Funding mit echten Binance-Raten (wo vorhanden, sonst 0,03 % pro Tag). " if dfund else "Funding pauschal 0,03 % pro Tag. ")
                     + ("Universum: je Tag die 40 umsatzstärksten Coins der letzten 30 Tage aus allen heute noch gelisteten Binance-USDT-Paaren (Delistete fehlen, Rest-Survivorship-Bias). " if members else "Universum = heutige Top-Coins nach Umsatz (Survivorship-Bias begünstigt Long-Strategien). ")
                     + "Keine Parameter wurden angepasst. Kosten 0,13 % pro Seite."
                     + (f" Datenprüfung: Stundendaten von {check['korrigiert']} Coins bereinigt, {len(check['entfernt'])} Coins wegen fehlerhafter Stundendaten entfernt." if check else "")
                     + (f" Tagesdaten von {len(daily_check)} Coins korrigiert ({', '.join(sorted(daily_check)[:8])})." if daily_check else "")
                     + note_extra)


def run_and_save(path="backtest.json", progress=None):
    """Rechnet den Backtest. Bei einem Fehler bleiben die alten Ergebnisse erhalten, der Fehler wird separat gemerkt.
    Geschrieben wird über eine Zwischendatei, damit ein Absturz mitten im Schreiben nichts zerstört."""
    try:
        r = run_backtest(progress=progress)
    except Exception as e:
        r = dict(ts=time.time(), error=str(e))
        with open("backtest_error.json", "w") as f:
            json.dump(r, f)
        return r
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(r, f)
    os.replace(tmp, path)
    try:
        os.remove("backtest_error.json")
    except OSError:
        pass
    return r


# ---------------------------------------------------------------- Vorwärtstest
class Forward:
    """Lässt alle Strategien parallel im Paper-Modus laufen, je 77 USDT, tägliche Anpassung um 00:10 UTC."""

    def __init__(self, path="fwd.db", start=77.0):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript("CREATE TABLE IF NOT EXISTS st(k TEXT PRIMARY KEY, v TEXT);"
                              "CREATE TABLE IF NOT EXISTS eq(ts REAL, k TEXT, v REAL);"
                              "CREATE INDEX IF NOT EXISTS eq_k ON eq(k, ts);")
        self.lock = threading.Lock()
        self.start = start
        self.px, self.msg, self.last_eq = {}, "", 0.0
        for k in NAMES:
            s = self.get(k)
            if s is None:
                self.put(k, dict(cash=start, pos={}, last_reb=0, rebs=0, costs=0.0, created=time.time()))
            elif not s.get("created"):            # ältere Konten: Start = erster gespeicherter Kontostand
                r = self.db.execute("SELECT MIN(ts) FROM eq WHERE k=?", (k,)).fetchone()
                s["created"] = (r[0] if r and r[0] else time.time())
                self.put(k, s)

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
                days, closes, day0, _x = load_history(universe(), 400)
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
                    if base_of(k).startswith("R_") or k in HOURK or k in MIX:
                        continue
                    s = self.get(k)
                    if k in CARRY:
                        if s.get("carry") and s.get("cday") and s["cday"] < today:
                            s["cash"] += sum(s["cnot"] * fr.get(sy, 0.0) * (today - s["cday"]) for sy in s["carry"])
                        picks = set(carry_picks(days - 1, *CARRYCFG[k]))
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
                    if k.startswith("H365F") and not self.get("_fix365"):     # früher fehlten Kursdaten für das 365-Tage-Fenster: einmal sofort neu anpassen
                        s["last_reb"] = 0
                    if s["last_reb"] == 0 or today - s["last_reb"] >= PERIOD[k]:
                        if not (PERIOD[k] >= 10 ** 9 and s["rebs"]):
                            acc.rebalance(self.px, sized(k, acc, self.px, closes, i, ctx))
                            s["rebs"] += 1
                        s["last_reb"] = today
                    acc.funding(self.px)
                    s.update(cash=acc.cash, pos=acc.pos, costs=acc.costs, losses=acc.losses, last_eq=acc.last_eq, peak=acc.peak)
                    self.put(k, s)
                self.put("_day", today)
                self.put("_fix365", 1)
                self.thin_eq()
            if not self.get("_fix365") and int(self.get("_day") or 0) >= today and time.time() - getattr(self, "_fix_try", 0) > 600:
                self._fix_try = time.time()                 # einmalige Korrektur: 365-Tage-Strategien konnten wegen zu kurzer Kurshistorie nicht handeln
                try:
                    days, closes, _d0, _x = load_history(universe(), 400)
                    for k in [k for k in NAMES if k.startswith("H365F")]:
                        s = self.get(k)
                        if not s or s.get("dead"):
                            continue
                        acc = Account(s["cash"], s["pos"], s.get("losses", 0), s.get("last_eq"))
                        acc.costs, acc.lev = s["costs"], k in LEV
                        acc.peak = dict(s.get("peak", {}))
                        acc.rebalance(self.px, sized(k, acc, self.px, closes, days - 1, None))
                        s.update(cash=acc.cash, pos=acc.pos, costs=acc.costs, losses=acc.losses, last_eq=acc.last_eq, peak=acc.peak,
                                 rebs=s["rebs"] + 1, last_reb=today)
                        self.put(k, s)
                    self.put("_fix365", 1)
                except Exception as e:
                    self.msg(f"Korrektur 365-Tage-Strategien fehlgeschlagen: {e}")
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
                self.mix_step(now)
                for k in NAMES:
                    s = self.get(k)
                    self.db.execute("INSERT INTO eq VALUES(?,?,?)", (now, k, Account(s["cash"], s["pos"]).equity(self.px)))
                    flag = bool(s["pos"] or s.get("carry") or s.get("w")) and not s.get("dead")
                    if flag != s.get("inv_now", False):        # Ein- und Ausstiege für das Diagramm merken
                        ev = (s.get("ev") or [])[-199:] + [[now, flag]]
                        s.update(ev=ev, inv_now=flag)
                        self.put(k, s)
                self.db.commit()

    def thin_eq(self, keep_days=7):
        """Kontoverlauf ausdünnen: die letzten 7 Tage bleiben im 10-Minuten-Takt, älteres wird auf eine Messung pro Stunde reduziert.
        Hält die Datenbank klein und das Dashboard schnell, auch nach Monaten und mit vielen Strategien."""
        cut = time.time() - keep_days * DAY
        try:
            self.db.execute("DELETE FROM eq WHERE ts < ? AND rowid NOT IN "
                            "(SELECT MIN(rowid) FROM eq WHERE ts < ? GROUP BY k, CAST(ts / 3600 AS INTEGER))", (cut, cut))
            self.db.commit()
        except Exception as e:
            print("Ausdünnen fehlgeschlagen:", e, flush=True)

    def mix_step(self, now):
        """Misch-Strategien im Paper-Test: Wert folgt den Einzelstrategien nach Anteilen.
        Auswahl für MIX5/MIX10 kommt aus dem letzten Backtest (nur Vergangenheitsdaten), Wechsel höchstens alle 30 Tage."""
        cur = {}
        for k in mix_pool():
            st = self.get(k)
            if st:
                cur[k] = Account(st["cash"], st["pos"]).equity(self.px)
        try:
            sel = json.load(open("backtest.json")).get("mix_auswahl") or {}
        except Exception:
            sel = {}
        for m in MIX:
            s = self.get(m)
            w, last = s.get("w") or {}, s.get("last") or {}
            if w:
                r = {k: (cur[k] / last[k] - 1) if last.get(k) and k in cur else 0.0 for k in w}
                g = sum(w[k] * (1 + r[k]) for k in w)
                s["cash"] = s["cash"] * g
                w = {k: w[k] * (1 + r[k]) / g for k in w} if g > 0 else {}
            want = None
            if m == "MIXALL":
                if not w:
                    want = [k for k in cur if cur[k] > 0]
            elif sel.get(m) and (not w or now - (s.get("mix_ts") or 0) >= 30 * DAY):
                want = sel[m]
            if want:
                nw = {k: 1 / len(want) for k in want if k in cur}
                if nw and set(nw) != set(w):
                    turn = sum(abs(nw.get(k, 0) - w.get(k, 0)) for k in set(nw) | set(w))
                    c = s["cash"] * turn * (FEE + SLIP)
                    s["cash"] -= c
                    s["costs"] = s.get("costs", 0) + c
                    s["rebs"] = s.get("rebs", 0) + 1
                    w = nw
                s["mix_ts"] = now
            s["w"], s["last"] = w, {k: cur[k] for k in w if k in cur}
            self.put(m, s)

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
                if k in MIX:
                    pos = [dict(symbol=a, side="mix", value=eq * x) for a, x in sorted((s.get("w") or {}).items(), key=lambda t: -t[1])]
                cv = [[r[0], r[1]] for r in self.db.execute(
                    "SELECT ts, v FROM (SELECT ts, v FROM eq WHERE k=? ORDER BY ts DESC LIMIT 500) ORDER BY ts", (k,))]
                out.append(dict(key=k, name=NAMES[k], curve=cv, equity=eq, pnl=eq - self.start, pct=(eq / self.start - 1) * 100,
                                positions=pos, rebalances=s["rebs"], costs=s["costs"], lev=LEV.get(k, 1),
                                liq=s.get("liq_ts"), stops=s.get("stops", 0), since=s.get("created"),
                                investiert=ev_intervals(s.get("ev") or [], time.time()),
                                base=BASEOF.get(k), trail=TRAIL[k][1] if k in TRAIL else None))
            return out

    def run(self):
        while True:
            try:
                self.step()
            except Exception as e:
                self.msg = str(e)
                print("Vorwärtstest Fehler:", e, flush=True)
            time.sleep(60)
