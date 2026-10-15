"""NBA Pinnacle odds-movement tracker  v1.0
FastAPI + MongoDB Atlas + The Odds API (Pinnacle only).
Built-in lessons from the MLB tracker:
  - picks lock `line_at_pick` once and settle against it (settle_version=1 = honest data)
  - lock window is relative to each game's tip-off, not a fixed clock time
  - post-tip odds are never stored (no live-odds pollution)
  - CLV is tracked from day one; edge claim needs n>=MIN_N and Wilson LB > -110 breakeven
  - HEAD supported on "/" (UptimeRobot), credit-budget governor for the free API tier
"""
import os, math, threading, time, logging
from datetime import datetime, timedelta, timezone
import requests
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient, ASCENDING, DESCENDING

API_KEY = os.environ["ODDS_API_KEY"]            # NBA's own key
MONGO_URI = os.environ["MONGO_URI"]
CRON_SECRET = os.getenv("CRON_SECRET", "")
MONTHLY_BUDGET = int(os.getenv("MONTHLY_BUDGET", "500"))   # your plan's credits / month
SETTLE_RESERVE = int(os.getenv("SETTLE_RESERVE", "40"))    # credits kept for /scores
ALLOW_ORIGIN = os.getenv("ALLOW_ORIGIN", "*")
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "1") == "1"

BASE = "https://api.the-odds-api.com/v4/sports/basketball_nba"
MARKETS = "spreads,totals"
POLL_COST = 2                 # markets x 1 bookmaker group
SETTLE_VERSION = 1
LOCK_MAX_MIN, LOCK_MIN_MIN = 120, 45       # lock window: T-120 .. T-45 min
STEAM = {"spread": 1.0, "total": 1.5}      # initial thresholds (points vs opening)
MIN_N = 30
BREAKEVEN = 0.524

log = logging.getLogger("nba")
logging.basicConfig(level=logging.INFO)
db = MongoClient(MONGO_URI, tz_aware=True)["nba_tracker"]
events, snaps, picks, diag, meta = db.events, db.snaps, db.picks, db.diag, db.meta
snaps.create_index([("event_id", ASCENDING), ("ts", ASCENDING)])
snaps.create_index("ts", expireAfterSeconds=60 * 86400)     # keep M0 small
picks.create_index([("event_id", ASCENDING), ("market", ASCENDING)], unique=True)
diag.create_index([("event_id", ASCENDING), ("market", ASCENDING)], unique=True)

app = FastAPI(title="NBA Pinnacle Tracker")
app.add_middleware(CORSMiddleware, allow_origins=[ALLOW_ORIGIN], allow_methods=["*"], allow_headers=["*"])


def now():
    return datetime.now(timezone.utc)


# ---------- credits ----------
def meta_get(k, d=None):
    r = meta.find_one({"_id": k})
    return r["v"] if r else d


def meta_set(k, v):
    meta.update_one({"_id": k}, {"$set": {"v": v}}, upsert=True)


def note_credits(resp):
    rem = resp.headers.get("x-requests-remaining")
    if rem is not None:
        meta_set("remaining", float(rem))


def daily_allowance():
    t = now()
    nxt = (t.replace(day=1) + timedelta(days=32)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    days_left = max((nxt - t).total_seconds() / 86400, 1)
    remaining = meta_get("remaining", MONTHLY_BUDGET)
    return max(remaining - SETTLE_RESERVE, 0) / days_left


def can_spend():
    key = "spent_" + now().strftime("%Y%m%d")
    return meta_get(key, 0) + POLL_COST <= daily_allowance(), key


# ---------- odds ----------
def parse_event(ev):
    bk = next((b for b in ev.get("bookmakers", []) if b["key"] == "pinnacle"), None)
    if not bk:
        return None
    out = {"spread": None, "spread_price": None, "total": None, "over_price": None, "under_price": None}
    for m in bk["markets"]:
        if m["key"] == "spreads":
            o = next((x for x in m["outcomes"] if x["name"] == ev["home_team"]), None)
            if o:
                out["spread"], out["spread_price"] = o["point"], o["price"]
        elif m["key"] == "totals":
            for o in m["outcomes"]:
                if o["name"] == "Over":
                    out["total"], out["over_price"] = o["point"], o["price"]
                elif o["name"] == "Under":
                    out["under_price"] = o["price"]
    return out if out["spread"] is not None or out["total"] is not None else None


def poll_odds():
    r = requests.get(BASE + "/odds", timeout=20, params={
        "apiKey": API_KEY, "bookmakers": "pinnacle", "markets": MARKETS, "oddsFormat": "decimal"})
    note_credits(r)
    r.raise_for_status()
    t, n = now(), 0
    for ev in r.json():
        commence = datetime.fromisoformat(ev["commence_time"].replace("Z", "+00:00"))
        if commence <= t:                 # never store live / post-tip odds
            continue
        s = parse_event(ev)
        if not s:
            continue
        s.update({"event_id": ev["id"], "ts": t})
        snaps.insert_one(dict(s))
        events.update_one({"_id": ev["id"]}, {
            "$setOnInsert": {"open_spread": s["spread"], "open_total": s["total"], "open_ts": t},
            "$set": {"home": ev["home_team"], "away": ev["away_team"], "commence": commence}}, upsert=True)
        evd = events.find_one({"_id": ev["id"]})
        try_lock(evd, s, t)
        n += 1
    return n


# ---------- picks ----------
def signal(evd, s):
    """Return {market: (side, line, price, delta)} following the move."""
    res = {}
    if s["spread"] is not None and evd.get("open_spread") is not None:
        d = s["spread"] - evd["open_spread"]
        if abs(d) >= STEAM["spread"]:      # home line more negative = money on home
            res["spread"] = ("home" if d < 0 else "away", s["spread"], s["spread_price"], d)
    if s["total"] is not None and evd.get("open_total") is not None:
        d = s["total"] - evd["open_total"]
        if abs(d) >= STEAM["total"]:
            res["total"] = ("over" if d > 0 else "under", s["total"],
                            s["over_price"] if d > 0 else s["under_price"], d)
    return res


def set_diag(eid, market, outcome):
    diag.update_one({"event_id": eid, "market": market},
                    {"$set": {"outcome": outcome, "ts": now()}}, upsert=True)


def try_lock(evd, s, t):
    mins = (evd["commence"] - t).total_seconds() / 60
    if mins > LOCK_MAX_MIN or mins <= 0:
        return
    sig = signal(evd, s)
    for market in ("spread", "total"):
        if picks.find_one({"event_id": evd["_id"], "market": market}):
            continue
        if market in sig and mins >= LOCK_MIN_MIN:
            side, line, price, d = sig[market]
            picks.insert_one({
                "event_id": evd["_id"], "market": market, "side": side, "line_at_pick": line,
                "price_at_pick": price, "open_line": evd["open_spread" if market == "spread" else "open_total"],
                "delta": d, "locked_at": t, "commence": evd["commence"], "home": evd["home"],
                "away": evd["away"], "settled": False, "settle_version": SETTLE_VERSION})
            set_diag(evd["_id"], market, "locked")
        elif mins < LOCK_MIN_MIN:
            in_window = snaps.count_documents({"event_id": evd["_id"], "ts": {
                "$gte": evd["commence"] - timedelta(minutes=LOCK_MAX_MIN),
                "$lte": evd["commence"] - timedelta(minutes=LOCK_MIN_MIN)}})
            set_diag(evd["_id"], market, "no_signal" if in_window else "missed_window")


# ---------- settlement ----------
def diff(market, side, line, hs, as_):
    if market == "spread":
        return hs + line - as_ if side == "home" else as_ - line - hs
    tot = hs + as_
    return tot - line if side == "over" else line - tot


def res(x):
    return "push" if x == 0 else ("win" if x > 0 else "loss")


def clv(market, side, line, close):
    if close is None:
        return None
    return (line - close) if side in ("home", "under") else (close - line)


def settle():
    open_picks = list(picks.find({"settled": False, "commence": {"$lt": now() - timedelta(hours=4)}}))
    if not open_picks:
        return 0
    r = requests.get(BASE + "/scores", timeout=20, params={"apiKey": API_KEY, "daysFrom": 3})
    note_credits(r)
    r.raise_for_status()
    done = {e["id"]: e for e in r.json() if e.get("completed") and e.get("scores")}
    n = 0
    for p in open_picks:
        e = done.get(p["event_id"])
        if not e:
            continue
        sc = {x["name"]: float(x["score"]) for x in e["scores"]}
        hs, as_ = sc.get(e["home_team"]), sc.get(e["away_team"])
        if hs is None or as_ is None:
            continue
        close = snaps.find_one({"event_id": p["event_id"], "ts": {"$lt": p["commence"]}}, sort=[("ts", DESCENDING)])
        close_line = close["spread" if p["market"] == "spread" else "total"] if close else None
        m, sd = p["market"], p["side"]
        picks.update_one({"_id": p["_id"]}, {"$set": {
            "settled": True, "home_score": hs, "away_score": as_, "close_line": close_line,
            "result": res(diff(m, sd, p["line_at_pick"], hs, as_)),                 # official
            "result_vs_open": res(diff(m, sd, p["open_line"], hs, as_)) if p["open_line"] is not None else None,
            "result_vs_close": res(diff(m, sd, close_line, hs, as_)) if close_line is not None else None,
            "clv_pts": clv(m, sd, p["line_at_pick"], close_line)}})
        n += 1
    return n


# ---------- stats ----------
def wilson_lb(w, n, z=1.96):
    if n == 0:
        return 0.0
    p = w / n
    return (p + z * z / (2 * n) - z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / (1 + z * z / n)


def bucket(rows):
    w = sum(r["result"] == "win" for r in rows)
    l = sum(r["result"] == "loss" for r in rows)
    pu = sum(r["result"] == "push" for r in rows)
    n = w + l
    cl = [r["clv_pts"] for r in rows if r.get("clv_pts") is not None]
    lb = wilson_lb(w, n)
    return {"n": n, "wins": w, "losses": l, "pushes": pu,
            "win_rate": round(w / n, 3) if n else None, "wilson_lb": round(lb, 3),
            "edge_claim": bool(n >= MIN_N and lb > BREAKEVEN),
            "avg_clv_pts": round(sum(cl) / len(cl), 2) if cl else None,
            "clv_positive_rate": round(sum(c > 0 for c in cl) / len(cl), 3) if cl else None}


# ---------- scheduler ----------
def next_interval_min():
    nxt = events.find_one({"commence": {"$gt": now()}}, sort=[("commence", ASCENDING)])
    if not nxt:
        return 180
    h = (nxt["commence"] - now()).total_seconds() / 3600
    return 120 if h > 12 else 30 if h > 3 else 10


def scheduler():
    last_poll = last_settle = 0.0
    while True:
        try:
            if time.time() - last_poll >= next_interval_min() * 60:
                ok, key = can_spend()
                last_poll = time.time()
                if ok:
                    poll_odds()
                    meta_set(key, meta_get(key, 0) + POLL_COST)
            if time.time() - last_settle >= 3 * 3600:
                last_settle = time.time()
                settle()
        except Exception as e:
            log.exception("scheduler error: %s", e)
        time.sleep(60)


@app.on_event("startup")
def _start():
    if ENABLE_SCHEDULER:
        threading.Thread(target=scheduler, daemon=True).start()


# ---------- routes ----------
@app.api_route("/", methods=["GET", "HEAD"])
def root():
    return {"ok": True, "service": "nba-pinnacle-tracker", "settle_version": SETTLE_VERSION}


@app.get("/games")
def games():
    out = []
    for e in events.find({"commence": {"$gt": now() - timedelta(hours=1)}}).sort("commence", 1):
        last = snaps.find_one({"event_id": e["_id"]}, sort=[("ts", DESCENDING)], projection={"_id": 0})
        pk = list(picks.find({"event_id": e["_id"]}, {"_id": 0}))
        out.append({"id": e["_id"], "home": e["home"], "away": e["away"], "commence": e["commence"],
                    "open_spread": e.get("open_spread"), "open_total": e.get("open_total"),
                    "latest": last, "n_snaps": snaps.count_documents({"event_id": e["_id"]}), "picks": pk})
    return out


@app.get("/game/{eid}")
def game(eid: str):
    return list(snaps.find({"event_id": eid}, {"_id": 0}).sort("ts", 1))


@app.get("/picks")
def get_picks(limit: int = 100):
    return list(picks.find({}, {"_id": 0}).sort("locked_at", -1).limit(limit))


@app.get("/stats")
def stats():
    rows = list(picks.find({"settled": True, "settle_version": SETTLE_VERSION}))
    out = {"settle_version": SETTLE_VERSION, "min_n": MIN_N, "breakeven": BREAKEVEN,
           "pending": picks.count_documents({"settled": False}), "all": bucket(rows)}
    for m in ("spread", "total"):
        out[m] = bucket([r for r in rows if r["market"] == m])
        for sd in {r["side"] for r in rows if r["market"] == m}:
            out[f"{m}_{sd}"] = bucket([r for r in rows if r["market"] == m and r["side"] == sd])
    # diagnostics only (not graded edge): same picks vs open / close
    out["diagnostic_vs_close"] = bucket([{**r, "result": r.get("result_vs_close") or "push"} for r in rows])
    return out


@app.get("/diagnostics/locks")
def diag_locks(days: int = 14):
    since = now() - timedelta(days=days)
    rows = list(diag.find({"ts": {"$gte": since}}))
    c = {}
    for r in rows:
        c[r["outcome"]] = c.get(r["outcome"], 0) + 1
    days_seen = len({r["ts"].strftime("%Y%m%d") for r in rows})
    return {"days": days, "days_with_data": days_seen, "outcomes": c, "total": len(rows)}


@app.get("/budget")
def budget():
    key = "spent_" + now().strftime("%Y%m%d")
    return {"remaining": meta_get("remaining", MONTHLY_BUDGET), "spent_today": meta_get(key, 0),
            "daily_allowance": round(daily_allowance(), 1), "poll_cost": POLL_COST}


@app.api_route("/poll", methods=["GET", "POST"])
def manual_poll(x_cron_secret: str = Header(default="")):
    if not CRON_SECRET or x_cron_secret != CRON_SECRET:
        raise HTTPException(403, "forbidden")
    return {"snapshots": poll_odds(), "settled": settle()}
