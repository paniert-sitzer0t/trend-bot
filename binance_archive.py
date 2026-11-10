"""Point-in-time Binance Spot history from Binance Vision monthly archives.

The current exchange ticker is not used to choose historical constituents. Archive
presence determines which symbols existed, and the backtest ranks each date by
the preceding 30 calendar days of quote volume.
"""
import calendar
import concurrent.futures
import csv
import io
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone

import requests

LIST_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/"
DATA_URL = "https://data.binance.vision/"
PREFIX = "data/spot/monthly/klines/"
TIMEOUT = 45
WORKERS = 6


def _get(url, params=None):
    """Retry temporary Binance Vision/S3 failures instead of aborting a multi-hour import."""
    last_error = None
    for attempt in range(4):
        try:
            r = requests.get(url, params=params, timeout=(10, TIMEOUT))
            if r.status_code in (429, 500, 502, 503, 504):
                r.raise_for_status()
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            last_error = exc
            if attempt == 3:
                raise
            time.sleep(min(2 ** attempt, 8))
    raise last_error


def _tag(node, name):
    return next((x for x in node if x.tag.rsplit("}", 1)[-1] == name), None)


def _listing(prefix, delimiter=None):
    """List a single archive directory; a listing failure must never look empty."""
    marker, out = "", []
    while True:
        params = {"prefix": prefix, "max-keys": "1000"}
        if delimiter:
            params["delimiter"] = delimiter
        if marker:
            params["marker"] = marker
        r = _get(LIST_URL, params=params)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        if root.tag.rsplit("}", 1)[-1] != "ListBucketResult":
            raise RuntimeError("Binance Vision returned a non-S3 archive listing")
        for x in root:
            name = x.tag.rsplit("}", 1)[-1]
            if name in ("CommonPrefixes", "Contents"):
                key = _tag(x, "Prefix" if name == "CommonPrefixes" else "Key")
                if key is not None and key.text:
                    out.append(key.text)
        trunc = _tag(root, "IsTruncated")
        if trunc is None or trunc.text != "true":
            return out
        nxt = _tag(root, "NextMarker")
        marker = (nxt.text if nxt is not None else "") or (out[-1] if out else "")
        if not marker:
            raise RuntimeError("Binance Vision listing is truncated without a continuation marker")


def _months(start_day, end_day):
    a = datetime.fromtimestamp(start_day * 86400, timezone.utc)
    b = datetime.fromtimestamp((end_day - 1) * 86400, timezone.utc)
    y, m, out = a.year, a.month, []
    while (y, m) <= (b.year, b.month):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return set(out)


def _db(path):
    db = sqlite3.connect(path, timeout=60)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.executescript(
        "CREATE TABLE IF NOT EXISTS vision_daily(s TEXT, d INTEGER, c REAL, q REAL, h REAL, l REAL, PRIMARY KEY(s,d));"
        "CREATE TABLE IF NOT EXISTS vision_month(s TEXT, m TEXT, PRIMARY KEY(s,m));"
    )
    return db


def _parse_zip(symbol, month, raw):
    rows = []
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        names = [n for n in z.namelist() if n.endswith(".csv")]
        if len(names) != 1:
            raise ValueError(f"{symbol} {month}: unerwarteter Binance-Archivinhalt")
        for row in csv.reader(io.TextIOWrapper(z.open(names[0]), encoding="utf-8")):
            if len(row) < 8:
                continue
            ts = int(float(row[0]))
            ts = ts // 1_000_000 if ts >= 100_000_000_000_000 else ts // 1000
            day = ts // 86400
            close, quote, high, low = float(row[4]), float(row[7]), float(row[2]), float(row[3])
            if close > 0:
                rows.append((symbol, day, close, quote, high, low))
    return rows


def load_point_in_time(days, stable_assets, excluded_symbols, top, extra_symbols, progress=None, path="candles.db",
                       clean_daily=None, rolling_members=None):
    """Return (n, closes, start, ext, members, data_check, symbol_count, end_day).

    The end is the first day of the current UTC month because Binance publishes
    monthly archives after month end. Repeated runs reuse parsed candles in SQLite.
    """
    now = datetime.now(timezone.utc)
    end_date = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    end_day = int(end_date.timestamp() // 86400)
    start_day = end_day - days
    wanted = _months(start_day, end_day)
    db = _db(path)
    try:
        if progress:
            progress("Binance-Archiv: Symbole")
        dirs = _listing(PREFIX, delimiter="/")
        symbols = []
        for d in dirs:
            symbol = d[len(PREFIX):].strip("/")
            if not symbol.endswith("USDT"):
                continue
            base = symbol[:-4]
            if (base in stable_assets or symbol in excluded_symbols
                    or (len(base) > 4 and base.endswith(("UP", "DOWN", "BULL", "BEAR")))):
                continue
            symbols.append(symbol)
        symbols = sorted(set(symbols))
        if not symbols:
            raise RuntimeError("Binance Vision hat keine historischen USDT-Spot-Symbole geliefert")

        jobs = []
        # Keep network catalog requests concurrent; SQLite stays on this thread.
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            listings = ex.map(lambda symbol: (symbol, _listing(f"{PREFIX}{symbol}/1d/")), symbols)
            for i, (symbol, keys) in enumerate(listings, 1):
                if progress and i % 25 == 0:
                    progress(f"Binance-Archiv: Katalog {i}/{len(symbols)}")
                for key in keys:
                    m = re.search(r"-1d-(\d{4}-\d{2})\.zip$", key)
                    if m and m.group(1) in wanted:
                        exists = db.execute("SELECT 1 FROM vision_month WHERE s=? AND m=?", (symbol, m.group(1))).fetchone()
                        if not exists:
                            jobs.append((symbol, m.group(1), key))

        def fetch(job):
            symbol, month, key = job
            r = _get(DATA_URL + key)
            r.raise_for_status()
            rows = _parse_zip(symbol, month, r.content)
            return symbol, month, rows

        if progress:
            progress(f"Binance-Archiv: Kerzen 0/{len(jobs)}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for i, result in enumerate(ex.map(fetch, jobs), 1):
                symbol, month, rows = result
                db.executemany("INSERT OR REPLACE INTO vision_daily VALUES(?,?,?,?,?,?)", rows)
                db.execute("INSERT OR REPLACE INTO vision_month VALUES(?,?)", (symbol, month))
                if i % 100 == 0 or i == len(jobs):
                    db.commit()
                    if progress:
                        progress(f"Binance-Archiv: Kerzen {i}/{len(jobs)}")
        db.commit()

        records = db.execute(
            "SELECT s,d,c,q FROM vision_daily WHERE d>=? AND d<? ORDER BY s,d",
            (start_day, end_day)).fetchall()
        closes_all, vol_all = {}, {}
        for symbol, day, close, quote in records:
            c = closes_all.setdefault(symbol, [None] * days)
            v = vol_all.setdefault(symbol, [None] * days)
            j = day - start_day
            if 0 <= j < days:
                c[j], v[j] = close, quote
        if not closes_all or "BTCUSDT" not in closes_all:
            raise RuntimeError("Binance-Archiv enthält im gewählten Zeitraum keine vollständige BTCUSDT-Reihe")

        data_check = clean_daily(closes_all, {"v": vol_all}) if clean_daily else {}
        members = rolling_members(closes_all, vol_all, days, top=top) if rolling_members else None
        selected = set(extra_symbols or ())
        if members is not None:
            for row in members:
                selected.update(row)
        else:
            selected.update(closes_all)
        selected.intersection_update(closes_all)
        if "BTCUSDT" in closes_all:
            selected.add("BTCUSDT")

        closes = {s: closes_all[s] for s in selected}
        ext = {"v": {s: vol_all[s] for s in selected if s in vol_all},
               "h": {s: [None] * days for s in selected},
               "l": {s: [None] * days for s in selected}}
        rows = db.execute(
            "SELECT s,d,h,l FROM vision_daily WHERE d>=? AND d<? AND s IN (%s) ORDER BY s,d"
            % ",".join("?" for _ in selected),
            (start_day, end_day, *sorted(selected))).fetchall()
        for symbol, day, high, low in rows:
            j = day - start_day
            if 0 <= j < days and closes[symbol][j] is not None:
                ext["h"][symbol][j], ext["l"][symbol][j] = high, low
        return days, closes, start_day, ext, members, data_check, len(symbols), end_day
    finally:
        db.close()


def prefill_hourly(symbols, start_day, end_day, progress=None, path="candles.db"):
    """Load historic spot 1h archives for selected symbols, including later-delisted pairs."""
    db = sqlite3.connect(path, timeout=60)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.executescript(
        "CREATE TABLE IF NOT EXISTS k(s TEXT, t INTEGER, c REAL, h REAL, l REAL, PRIMARY KEY(s,t));"
        "CREATE TABLE IF NOT EXISTS vision_hour_month(s TEXT, m TEXT, PRIMARY KEY(s,m));"
    )
    start_ms, end_ms = start_day * 86400000, end_day * 86400000
    wanted = _months(start_day, end_day)
    jobs = []
    symbols = sorted(set(symbols))
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        listings = ex.map(lambda symbol: (symbol, _listing(f"{PREFIX}{symbol}/1h/")), symbols)
        for i, (symbol, keys) in enumerate(listings, 1):
            if progress and i % 10 == 0:
                progress(f"Binance-Stunden: Katalog {i}/{len(symbols)}")
            for key in keys:
                match = re.search(r"-1h-(\d{4}-\d{2})\.zip$", key)
                if not match or match.group(1) not in wanted:
                    continue
                month = match.group(1)
                if not db.execute("SELECT 1 FROM vision_hour_month WHERE s=? AND m=?", (symbol, month)).fetchone():
                    jobs.append((symbol, month, key))

    def fetch(job):
        symbol, month, key = job
        r = _get(DATA_URL + key)
        r.raise_for_status()
        rows = []
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            names = [n for n in z.namelist() if n.endswith(".csv")]
            if len(names) != 1:
                raise ValueError(f"{symbol} {month} 1h: unerwarteter Binance-Archivinhalt")
            for row in csv.reader(io.TextIOWrapper(z.open(names[0]), encoding="utf-8")):
                if len(row) < 5:
                    continue
                raw_ts = int(float(row[0]))
                ts = raw_ts // 1000 if raw_ts >= 100_000_000_000_000 else raw_ts
                if start_ms <= ts < end_ms:
                    close, high, low = float(row[4]), float(row[2]), float(row[3])
                    if close > 0:
                        rows.append((symbol, ts, close, high, low))
        return symbol, month, rows

    try:
        if progress:
            progress(f"Binance-Stunden: Kerzen 0/{len(jobs)}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for i, result in enumerate(ex.map(fetch, jobs), 1):
                symbol, month, rows = result
                db.executemany("INSERT OR REPLACE INTO k VALUES(?,?,?,?,?)", rows)
                db.execute("INSERT OR REPLACE INTO vision_hour_month VALUES(?,?)", (symbol, month))
                if i % 100 == 0 or i == len(jobs):
                    db.commit()
                    if progress:
                        progress(f"Binance-Stunden: Kerzen {i}/{len(jobs)}")
        db.commit()
    finally:
        db.close()
