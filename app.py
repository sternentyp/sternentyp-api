from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import pytz
import time
from collections import defaultdict, deque
from itertools import combinations

import swisseph as swe
from timezonefinder import TimezoneFinder
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable, GeocoderTimedOut, GeocoderServiceError

app = Flask(__name__)

# -------------------------
# EPHEMERIS
# -------------------------
swe.set_ephe_path("./ephe")

# -------------------------
# CONSTANTS
# -------------------------
ZODIAC_SIGNS = [
    "Widder", "Stier", "Zwillinge", "Krebs", "Löwe", "Jungfrau",
    "Waage", "Skorpion", "Schütze", "Steinbock", "Wassermann", "Fische"
]

# Zeichen -> Element / Modalität
SIGN_META = {
    "Widder":       {"element": "Feuer", "modalitaet": "Kardinal"},
    "Stier":        {"element": "Erde",  "modalitaet": "Fix"},
    "Zwillinge":    {"element": "Luft",  "modalitaet": "Veränderlich"},
    "Krebs":        {"element": "Wasser","modalitaet": "Kardinal"},
    "Löwe":         {"element": "Feuer", "modalitaet": "Fix"},
    "Jungfrau":     {"element": "Erde",  "modalitaet": "Veränderlich"},
    "Waage":        {"element": "Luft",  "modalitaet": "Kardinal"},
    "Skorpion":     {"element": "Wasser","modalitaet": "Fix"},
    "Schütze":      {"element": "Feuer", "modalitaet": "Veränderlich"},
    "Steinbock":    {"element": "Erde",  "modalitaet": "Kardinal"},
    "Wassermann":   {"element": "Luft",  "modalitaet": "Fix"},
    "Fische":       {"element": "Wasser","modalitaet": "Veränderlich"},
}

# Bodies (Planeten + Punkte)
BODIES = {
    "Sonne": swe.SUN,
    "Mond": swe.MOON,
    "Merkur": swe.MERCURY,
    "Venus": swe.VENUS,
    "Mars": swe.MARS,
    "Jupiter": swe.JUPITER,
    "Saturn": swe.SATURN,
    "Uranus": swe.URANUS,
    "Neptun": swe.NEPTUNE,
    "Pluto": swe.PLUTO,

    # Erweiterte Punkte
    "Chiron": swe.CHIRON,
    "Lilith": swe.MEAN_APOG,       # Black Moon Lilith (Mean Apogee)
    "Mondknoten": swe.TRUE_NODE,   # True Node
}

ASPECTS = [
    ("Konjunktion", 0.0, 8.0),
    ("Sextil", 60.0, 6.0),
    ("Quadrat", 90.0, 6.0),
    ("Trigon", 120.0, 6.0),
    ("Opposition", 180.0, 8.0),
]

# Für Aspektmuster brauchen wir zusätzlich Quincunx (150°)
PATTERN_ASPECTS = [
    ("Sextil", 60.0, 4.0),
    ("Quadrat", 90.0, 5.0),
    ("Trigon", 120.0, 5.0),
    ("Opposition", 180.0, 6.0),
    ("Quincunx", 150.0, 3.0),
]

# -------------------------
# ABUSE-SCHUTZ (LIGHT)
# -------------------------
RATE_LIMIT = 90        # max requests
RATE_WINDOW = 60       # per 60 seconds
_ip_requests = defaultdict(lambda: deque())

def get_client_ip():
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.remote_addr or "unknown"

@app.before_request
def rate_limit_guard():
    ip = get_client_ip()
    now = time.time()
    q = _ip_requests[ip]
    while q and (now - q[0]) > RATE_WINDOW:
        q.popleft()
    if len(q) >= RATE_LIMIT:
        return jsonify({"error": "Too many requests. Please slow down for a moment. 💛"}), 429
    q.append(now)
    return None

# -------------------------
# GEO CACHE (TTL)
# -------------------------
GEO_TTL_SECONDS = 7 * 24 * 3600  # 7 Tage Cache
_geo_cache = {}  # place -> (lat, lon, ts)

def geo_cache_get(place: str):
    entry = _geo_cache.get(place)
    if not entry:
        return None
    lat, lon, ts = entry
    if (time.time() - ts) > GEO_TTL_SECONDS:
        _geo_cache.pop(place, None)
        return None
    return lat, lon

def geo_cache_set(place: str, lat: float, lon: float):
    _geo_cache[place] = (lat, lon, time.time())

# -------------------------
# HELPERS
# -------------------------
def deg_to_sign(deg: float):
    deg = deg % 360.0
    sign_index = int(deg // 30) % 12
    sign_deg = deg % 30
    return {
        "zeichen": ZODIAC_SIGNS[sign_index],
        "grad": round(sign_deg, 6),
        "ecliptic_longitude": round(deg, 6),
    }

def norm360(x: float) -> float:
    x = x % 360.0
    if x < 0:
        x += 360.0
    return x

def angle_diff(a: float, b: float) -> float:
    d = abs(norm360(a) - norm360(b))
    return min(d, 360.0 - d)

def midpoint_angle(a: float, b: float) -> float:
    a = norm360(a)
    b = norm360(b)
    d = (b - a + 360.0) % 360.0
    if d > 180.0:
        d -= 360.0
    return norm360(a + d / 2.0)

def pick_pattern_aspect(lon_a: float, lon_b: float):
    d = angle_diff(lon_a, lon_b)
    for asp_name, exact, orb in PATTERN_ASPECTS:
        if abs(d - exact) <= orb:
            return asp_name, round(d, 6), round(abs(d - exact), 6)
    return None, None, None

# -------------------------
# ELEMENT / MODALITÄTEN BALANCE
# -------------------------
# Welche Bodies zählen? (du kannst hier feinjustieren)
BALANCE_BODIES = {
    "Sonne","Mond","Merkur","Venus","Mars","Jupiter","Saturn",
    "Uranus","Neptun","Pluto","Chiron","Lilith","Mondknoten","Südknoten"
}

def calc_element_modal_balance(bodies_out: dict):
    elements = {"Feuer": 0, "Erde": 0, "Luft": 0, "Wasser": 0}
    modal = {"Kardinal": 0, "Fix": 0, "Veränderlich": 0}

    details = []
    for body, data in bodies_out.items():
        if body not in BALANCE_BODIES:
            continue
        sign = data.get("zeichen")
        meta = SIGN_META.get(sign)
        if not meta:
            continue
        elements[meta["element"]] += 1
        modal[meta["modalitaet"]] += 1
        details.append({
            "body": body,
            "zeichen": sign,
            "element": meta["element"],
            "modalitaet": meta["modalitaet"]
        })

    return {
        "elements": elements,
        "modalitaeten": modal,
        "counted_bodies": sorted(list(BALANCE_BODIES)),
        "details": details
    }

# -------------------------
# STELLIUM DETECTION
# -------------------------
# Default: mind. 3 Bodies im gleichen Zeichen
STELLIUM_MIN_BODIES = 3
# Optional: enger Stellium-Check (Orb über Longitudes), 0 = aus
STELLIUM_ORB_DEG = 0.0  # z.B. 10.0 wenn du "enge Cluster" willst

# Welche Bodies zählen für Stellium?
STELLIUM_BODIES = {
    "Sonne","Mond","Merkur","Venus","Mars","Jupiter","Saturn",
    "Uranus","Neptun","Pluto","Chiron","Lilith","Mondknoten","Südknoten"
}

def calc_stelliums(bodies_out: dict, bodies_lon: dict):
    by_sign = defaultdict(list)
    for body, data in bodies_out.items():
        if body not in STELLIUM_BODIES:
            continue
        sign = data.get("zeichen")
        if sign:
            by_sign[sign].append(body)

    stelliums = []
    for sign, bodies in by_sign.items():
        if len(bodies) >= STELLIUM_MIN_BODIES:
            entry = {"zeichen": sign, "bodies": sorted(bodies), "count": len(bodies)}

            # Optional: Orb-Cluster innerhalb des Zeichens (wenn aktiviert)
            if STELLIUM_ORB_DEG and STELLIUM_ORB_DEG > 0:
                lons = sorted([(b, bodies_lon[b]) for b in bodies], key=lambda x: x[1])
                # sehr simple cluster: max-min innerhalb Zeichenbereich
                vals = [lon for _, lon in lons]
                span = max(vals) - min(vals)
                entry["orb_span_deg"] = round(span, 6)
                entry["orb_ok"] = span <= STELLIUM_ORB_DEG
            stelliums.append(entry)

    stelliums.sort(key=lambda x: x["count"], reverse=True)
    return {
        "min_bodies": STELLIUM_MIN_BODIES,
        "counted_bodies": sorted(list(STELLIUM_BODIES)),
        "orb_mode_deg": STELLIUM_ORB_DEG,
        "stelliums": stelliums
    }

# -------------------------
# ASPEKTMUSTER
# -------------------------
# Wir erkennen Muster nur auf einem Set von "wichtigen Punkten", sonst wird’s spammy.
PATTERN_BODIES = [
    "Sonne","Mond","Merkur","Venus","Mars","Jupiter","Saturn","Uranus","Neptun","Pluto"
]

def build_aspect_map(bodies_lon: dict):
    # Map: frozenset({A,B}) -> aspect_name
    amap = {}
    for a, b in combinations(PATTERN_BODIES, 2):
        if a not in bodies_lon or b not in bodies_lon:
            continue
        asp, actual, orb = pick_pattern_aspect(bodies_lon[a], bodies_lon[b])
        if asp:
            amap[frozenset([a, b])] = {
                "aspect": asp,
                "actual_angle": actual,
                "orb": orb
            }
    return amap

def has_aspect(amap, a, b, aspect_name):
    key = frozenset([a, b])
    v = amap.get(key)
    return v is not None and v["aspect"] == aspect_name

def detect_patterns(bodies_lon: dict):
    amap = build_aspect_map(bodies_lon)
    patterns = []

    # --------
    # Grand Trine (A-B trine, B-C trine, A-C trine)
    # --------
    for a, b, c in combinations(PATTERN_BODIES, 3):
        if (a in bodies_lon and b in bodies_lon and c in bodies_lon and
            has_aspect(amap, a, b, "Trigon") and
            has_aspect(amap, a, c, "Trigon") and
            has_aspect(amap, b, c, "Trigon")):
            patterns.append({
                "pattern": "Grand Trine",
                "points": [a, b, c]
            })

    # --------
    # T-Square (A-B opposition, A-C square, B-C square)
    # --------
    for a, b, c in combinations(PATTERN_BODIES, 3):
        if (a in bodies_lon and b in bodies_lon and c in bodies_lon and
            has_aspect(amap, a, b, "Opposition") and
            has_aspect(amap, a, c, "Quadrat") and
            has_aspect(amap, b, c, "Quadrat")):
            patterns.append({
                "pattern": "T-Square",
                "points": [a, b, c],
                "apex": c
            })
        # auch andere Permutationen abdecken (apex kann a/b/c sein)
        if (a in bodies_lon and b in bodies_lon and c in bodies_lon and
            has_aspect(amap, a, c, "Opposition") and
            has_aspect(amap, a, b, "Quadrat") and
            has_aspect(amap, c, b, "Quadrat")):
            patterns.append({
                "pattern": "T-Square",
                "points": [a, b, c],
                "apex": b
            })
        if (a in bodies_lon and b in bodies_lon and c in bodies_lon and
            has_aspect(amap, b, c, "Opposition") and
            has_aspect(amap, b, a, "Quadrat") and
            has_aspect(amap, c, a, "Quadrat")):
            patterns.append({
                "pattern": "T-Square",
                "points": [a, b, c],
                "apex": a
            })

    # --------
    # Mystic Rectangle (4 Punkte: 2 Oppositions, 2 Trines, 2 Sextiles)
    # Klassisch: A-C opposition, B-D opposition,
    # A-B trine, C-D trine,
    # A-D sextile, B-C sextile (oder gespiegelt)
    # --------
    for a, b, c, d in combinations(PATTERN_BODIES, 4):
        if not all(x in bodies_lon for x in [a, b, c, d]):
            continue

        # Variante 1
        if (has_aspect(amap, a, c, "Opposition") and
            has_aspect(amap, b, d, "Opposition") and
            has_aspect(amap, a, b, "Trigon") and
            has_aspect(amap, c, d, "Trigon") and
            has_aspect(amap, a, d, "Sextil") and
            has_aspect(amap, b, c, "Sextil")):
            patterns.append({
                "pattern": "Mystic Rectangle",
                "points": [a, b, c, d]
            })

        # Variante 2 (gespiegelt)
        if (has_aspect(amap, a, c, "Opposition") and
            has_aspect(amap, b, d, "Opposition") and
            has_aspect(amap, a, d, "Trigon") and
            has_aspect(amap, c, b, "Trigon") and
            has_aspect(amap, a, b, "Sextil") and
            has_aspect(amap, c, d, "Sextil")):
            patterns.append({
                "pattern": "Mystic Rectangle",
                "points": [a, b, c, d]
            })

    # --------
    # Kite: Grand Trine + 1 Opposition zu einem Vertex + 2 Sextiles
    # --------
    # Vorgehen: finde jedes Grand Trine (a,b,c). Suche d, das zu einem Vertex opposition ist
    # und zu den beiden anderen Sextil.
    grand_trines = [p for p in patterns if p["pattern"] == "Grand Trine"]
    for gt in grand_trines:
        a, b, c = gt["points"]
        for d in PATTERN_BODIES:
            if d in (a, b, c) or d not in bodies_lon:
                continue
            # d opposition zu a + sextile zu b/c
            if (has_aspect(amap, d, a, "Opposition") and
                has_aspect(amap, d, b, "Sextil") and
                has_aspect(amap, d, c, "Sextil")):
                patterns.append({"pattern": "Kite", "points": [a, b, c, d], "opposition_to": a})
            # d opposition zu b
            if (has_aspect(amap, d, b, "Opposition") and
                has_aspect(amap, d, a, "Sextil") and
                has_aspect(amap, d, c, "Sextil")):
                patterns.append({"pattern": "Kite", "points": [a, b, c, d], "opposition_to": b})
            # d opposition zu c
            if (has_aspect(amap, d, c, "Opposition") and
                has_aspect(amap, d, a, "Sextil") and
                has_aspect(amap, d, b, "Sextil")):
                patterns.append({"pattern": "Kite", "points": [a, b, c, d], "opposition_to": c})

    # --------
    # Yod: 2 Quincunx + 1 Sextil (A-B sextile, A-C quincunx, B-C quincunx)
    # C ist Apex
    # --------
    for a, b, c in combinations(PATTERN_BODIES, 3):
        if not all(x in bodies_lon for x in [a, b, c]):
            continue

        # apex = c
        if (has_aspect(amap, a, b, "Sextil") and
            has_aspect(amap, a, c, "Quincunx") and
            has_aspect(amap, b, c, "Quincunx")):
            patterns.append({"pattern": "Yod", "points": [a, b, c], "apex": c})

        # apex = b
        if (has_aspect(amap, a, c, "Sextil") and
            has_aspect(amap, a, b, "Quincunx") and
            has_aspect(amap, c, b, "Quincunx")):
            patterns.append({"pattern": "Yod", "points": [a, b, c], "apex": b})

        # apex = a
        if (has_aspect(amap, b, c, "Sextil") and
            has_aspect(amap, b, a, "Quincunx") and
            has_aspect(amap, c, a, "Quincunx")):
            patterns.append({"pattern": "Yod", "points": [a, b, c], "apex": a})

    # Dedup: gleiche Muster mit gleichen Punkten nur 1x
    seen = set()
    deduped = []
    for p in patterns:
        key = (p["pattern"], tuple(sorted(p["points"])))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(p)

    return {
        "pattern_bodies": PATTERN_BODIES,
        "aspects_used": [{"name": n, "exact": e, "orb": o} for (n, e, o) in PATTERN_ASPECTS],
        "patterns": deduped
    }

# -------------------------
# GEO + TZ (FIX: NIE WIEDER 500)
# -------------------------
def get_latlon_from_place(place_name: str):
    if not place_name:
        return None, ("Provide either (lat, lon) or place", 400)

    cached = geo_cache_get(place_name)
    if cached:
        return cached, None

    try:
        geolocator = Nominatim(user_agent="sternentyp", timeout=6)
        loc = geolocator.geocode(place_name, language="de")
        if not loc:
            return None, ("Could not geocode place. Provide lat/lon for accuracy.", 400)

        lat, lon = float(loc.latitude), float(loc.longitude)
        geo_cache_set(place_name, lat, lon)
        return (lat, lon), None

    except (GeocoderUnavailable, GeocoderTimedOut, GeocoderServiceError):
        return None, ("Geocoding service temporarily unavailable. Please provide lat/lon.", 503)
    except Exception:
        return None, ("Geocoding failed unexpectedly. Please provide lat/lon.", 503)

def infer_timezone(lat, lon):
    tf = TimezoneFinder()
    return tf.timezone_at(lat=float(lat), lng=float(lon))

def parse_input_datetime(date_str: str, time_str: str, tz_name: str):
    local_tz = pytz.timezone(tz_name)
    naive_local = datetime.fromisoformat(f"{date_str}T{time_str}:00")
    aware_local = local_tz.localize(naive_local, is_dst=None)
    return aware_local, aware_local.astimezone(pytz.UTC)

def jd_ut_from_utc(utc_dt: datetime) -> float:
    return swe.julday(
        utc_dt.year, utc_dt.month, utc_dt.day,
        utc_dt.hour + utc_dt.minute / 60.0 + utc_dt.second / 3600.0
    )

def zodiac_flags(zodiac: str):
    if zodiac == "sidereal":
        swe.set_sid_mode(swe.SIDM_FAGAN_BRADLEY, 0, 0)
        return swe.FLG_SWIEPH | swe.FLG_SIDEREAL
    return swe.FLG_SWIEPH

def calc_body_lon_meta(jd_ut: float, body_const: int, flags: int):
    xx, _ = swe.calc_ut(jd_ut, body_const, flags)
    lon = float(xx[0]) % 360.0
    speed_lon = float(xx[3])
    return lon, speed_lon

def calc_bodies(jd_ut: float, flags: int):
    out = {}
    meta = {}
    for name, p in BODIES.items():
        lon, speed_lon = calc_body_lon_meta(jd_ut, p, flags)
        out[name] = lon
        meta[name] = {"speed_lon": round(speed_lon, 6), "retrograd": speed_lon < 0}
    return out, meta

def calc_houses(jd_ut: float, lat: float, lon: float, house_system: str):
    hsys = str(house_system)[0].encode("ascii")
    houses, ascmc = swe.houses(jd_ut, float(lat), float(lon), hsys)
    cusp_list = list(houses[1:13]) if len(houses) == 13 else list(houses[0:12])
    asc = ascmc[0]
    mc = ascmc[1]
    houses_out = {f"haus_{i}": cusp_list[i - 1] % 360.0 for i in range(1, 13)}
    return houses_out, asc % 360.0, mc % 360.0

def planet_house(planet_lon: float, houses_out: dict):
    cusps = [houses_out[f"haus_{i}"] for i in range(1, 13)]
    base = cusps[0]
    adj_cusps = [norm360(c - base) for c in cusps]
    pl = norm360(planet_lon - base)

    for i in range(12):
        start = adj_cusps[i]
        end = adj_cusps[(i + 1) % 12]
        if i < 11:
            if start <= pl < end:
                return i + 1
        else:
            if pl >= start or pl < adj_cusps[0]:
                return 12
    return 12

def aspects_between(set_a: dict, set_b: dict):
    events = []
    for name_a, lon_a in set_a.items():
        for name_b, lon_b in set_b.items():
            if name_a == name_b and set_a is set_b:
                continue
            d = angle_diff(lon_a, lon_b)
            for asp_name, exact, orb in ASPECTS:
                orb_limit = orb
                if name_a in ("Sonne", "Mond") or name_b in ("Sonne", "Mond"):
                    orb_limit = max(orb_limit, 8.0)
                if abs(d - exact) <= orb_limit:
                    events.append({
                        "aspect": asp_name,
                        "exact_angle": exact,
                        "actual_angle": round(d, 6),
                        "orb": round(abs(d - exact), 6),
                        "orb_limit": float(orb_limit),
                        "body_1": name_a,
                        "body_2": name_b
                    })
                    break
    events.sort(key=lambda x: x["orb"])
    return events

# -------------------------
# CORE: BUILD CHART
# -------------------------
def build_chart(payload: dict):
    date_str = payload.get("date")
    time_str = payload.get("time")
    place = payload.get("place")
    lat = payload.get("lat")
    lon = payload.get("lon")
    tz_name = payload.get("timezone")
    house_system = payload.get("house_system", "P")
    zodiac = payload.get("zodiac", "tropical")

    if not date_str or not time_str:
        return None, ("Missing required fields: date, time", 400)

    if lat is None or lon is None:
        if not place:
            return None, ("Provide either (lat, lon) or place", 400)
        ll, geo_err = get_latlon_from_place(place)
        if geo_err:
            return None, geo_err
        lat, lon = ll

    if not tz_name:
        tz_name = infer_timezone(float(lat), float(lon))
        if not tz_name:
            return None, ("Could not infer timezone, please provide timezone", 400)

    _, utc_dt = parse_input_datetime(date_str, time_str, tz_name)
    jd_ut = jd_ut_from_utc(utc_dt)
    flags = zodiac_flags(zodiac)

    houses_out, asc, mc = calc_houses(jd_ut, float(lat), float(lon), house_system)
    bodies_lon, bodies_meta = calc_bodies(jd_ut, flags)

    # Südknoten automatisch
    if "Mondknoten" in bodies_lon:
        bodies_lon["Südknoten"] = norm360(bodies_lon["Mondknoten"] + 180.0)
        bodies_meta["Südknoten"] = {
            "speed_lon": bodies_meta.get("Mondknoten", {}).get("speed_lon", 0.0),
            "retrograd": bodies_meta.get("Mondknoten", {}).get("retrograd", False)
        }

    bodies_out = {k: deg_to_sign(v) for k, v in bodies_lon.items()}
    houses_fmt = {k: deg_to_sign(v) for k, v in houses_out.items()}

    planet_houses = {k: planet_house(v, houses_out) for k, v in bodies_lon.items()}

    aspects = aspects_between(bodies_lon, bodies_lon)
    dedup = []
    seen = set()
    for a in aspects:
        pair = tuple(sorted([a["body_1"], a["body_2"]])) + (a["aspect"],)
        if pair in seen:
            continue
        seen.add(pair)
        dedup.append(a)
    aspects = dedup

    # NEW: Element/Modal Balance
    balance = calc_element_modal_balance(bodies_out)

    # NEW: Stelliums
    stelliums = calc_stelliums(bodies_out, bodies_lon)

    # NEW: Aspect Patterns
    patterns = detect_patterns(bodies_lon)

    result = {
        "input": {
            "date": date_str,
            "time": time_str,
            "place": place,
            "lat": float(lat),
            "lon": float(lon),
            "timezone": tz_name,
            "house_system": house_system,
            "zodiac": zodiac
        },
        "utc": utc_dt.isoformat(),
        "jd_ut": float(jd_ut),
        "ascendant": deg_to_sign(asc),
        "mc": deg_to_sign(mc),
        "houses": houses_fmt,
        "bodies": bodies_out,
        "bodies_meta": bodies_meta,
        "planet_houses": planet_houses,
        "aspects": aspects,

        # --- NEW OUTPUTS ---
        "balance": balance,
        "stelliums": stelliums,
        "aspect_patterns": patterns
    }
    return result, None

# -------------------------
# ROUTES
# -------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

@app.route("/chart", methods=["POST"])
def chart():
    payload = request.json or {}
    chart_result, err = build_chart(payload)
    if err:
        msg, code = err
        return jsonify({"error": msg}), code
    return jsonify(chart_result)

@app.route("/transits", methods=["POST"])
def transits():
    payload = request.json or {}
    natal = payload.get("natal")
    if not natal:
        return jsonify({"error": "Missing required field: natal"}), 400

    start_date = payload.get("start_date")
    end_date = payload.get("end_date")
    step_hours = int(payload.get("step_hours", 6))

    if not start_date or not end_date:
        return jsonify({"error": "Missing required fields: start_date, end_date"}), 400

    natal_result, natal_err = build_chart(natal)
    if natal_err:
        msg, code = natal_err
        return jsonify({"error": f"Natal error: {msg}"}), code

    zodiac = natal.get("zodiac", "tropical")
    flags = zodiac_flags(zodiac)

    start_dt_utc = datetime.fromisoformat(start_date + "T00:00:00").replace(tzinfo=pytz.UTC)
    end_dt_utc = datetime.fromisoformat(end_date + "T23:59:59").replace(tzinfo=pytz.UTC)

    natal_lons = {k: natal_result["bodies"][k]["ecliptic_longitude"] for k in natal_result["bodies"].keys()}
    natal_points = dict(natal_lons)
    natal_points["Aszendent"] = natal_result["ascendant"]["ecliptic_longitude"]
    natal_points["MC"] = natal_result["mc"]["ecliptic_longitude"]

    transit_bodies = payload.get("transit_bodies")
    if not transit_bodies:
        transit_bodies = list(BODIES.keys())

    best = {}
    t = start_dt_utc
    while t <= end_dt_utc:
        jd_ut = jd_ut_from_utc(t)

        trans_lons = {}
        for name in transit_bodies:
            if name not in BODIES:
                continue
            lon, _speed = calc_body_lon_meta(jd_ut, BODIES[name], flags)
            trans_lons[name] = lon

        for tr_name, tr_lon in trans_lons.items():
            for nat_name, nat_lon in natal_points.items():
                d = angle_diff(tr_lon, nat_lon)
                for asp_name, exact, orb in ASPECTS:
                    orb_limit = orb
                    if nat_name in ("Sonne", "Mond", "Aszendent", "MC") or tr_name in ("Sonne", "Mond"):
                        orb_limit = max(orb_limit, 8.0)
                    delta = abs(d - exact)
                    if delta <= orb_limit:
                        key = (tr_name, nat_name, asp_name)
                        cur = best.get(key)
                        if cur is None or delta < cur["orb"]:
                            best[key] = {
                                "type": "transit_aspect",
                                "transit_body": tr_name,
                                "natal_point": nat_name,
                                "aspect": asp_name,
                                "exact_angle": exact,
                                "actual_angle": round(d, 6),
                                "orb": round(delta, 6),
                                "orb_limit": float(orb_limit),
                                "peak_utc": t.isoformat()
                            }
                        break
        t += timedelta(hours=step_hours)

    events = list(best.values())
    events.sort(key=lambda x: x["orb"])

    hard = {"Quadrat", "Opposition", "Konjunktion"}
    heavy = {"Saturn", "Uranus", "Pluto"}
    personal = {"Sonne", "Mond", "Aszendent", "MC", "Merkur", "Venus", "Mars"}
    tension_hits = [e for e in events if e["aspect"] in hard and e["transit_body"] in heavy and e["natal_point"] in personal]
    tension_score = min(100, len(tension_hits) * 12)

    out = {
        "natal": {
            "ascendant": natal_result["ascendant"],
            "mc": natal_result["mc"],
            "bodies": natal_result["bodies"],
            "bodies_meta": natal_result.get("bodies_meta", {}),
            "balance": natal_result.get("balance", {}),
            "stelliums": natal_result.get("stelliums", {}),
            "aspect_patterns": natal_result.get("aspect_patterns", {})
        },
        "window": {
            "start_date": start_date,
            "end_date": end_date,
            "step_hours": step_hours
        },
        "events": events[:200],
        "tension_score": tension_score,
        "tension_highlights": tension_hits[:25]
    }
    return jsonify(out)

@app.route("/synastry", methods=["POST"])
def synastry():
    payload = request.json or {}
    person_a = payload.get("person_a")
    person_b = payload.get("person_b")
    if not person_a or not person_b:
        return jsonify({"error": "Missing required fields: person_a, person_b"}), 400

    a_chart, a_err = build_chart(person_a)
    if a_err:
        msg, code = a_err
        return jsonify({"error": f"Person A error: {msg}"}), code

    b_chart, b_err = build_chart(person_b)
    if b_err:
        msg, code = b_err
        return jsonify({"error": f"Person B error: {msg}"}), code

    a_lons = {k: a_chart["bodies"][k]["ecliptic_longitude"] for k in a_chart["bodies"].keys()}
    b_lons = {k: b_chart["bodies"][k]["ecliptic_longitude"] for k in b_chart["bodies"].keys()}

    a_points = dict(a_lons)
    a_points["Aszendent"] = a_chart["ascendant"]["ecliptic_longitude"]
    a_points["MC"] = a_chart["mc"]["ecliptic_longitude"]

    b_points = dict(b_lons)
    b_points["Aszendent"] = b_chart["ascendant"]["ecliptic_longitude"]
    b_points["MC"] = b_chart["mc"]["ecliptic_longitude"]

    syn_aspects = []
    for a_name, a_lon in a_points.items():
        for b_name, b_lon in b_points.items():
            d = angle_diff(a_lon, b_lon)
            for asp_name, exact, orb in ASPECTS:
                orb_limit = orb
                if a_name in ("Sonne", "Mond", "Aszendent", "MC") or b_name in ("Sonne", "Mond", "Aszendent", "MC"):
                    orb_limit = max(orb_limit, 8.0)
                delta = abs(d - exact)
                if delta <= orb_limit:
                    syn_aspects.append({
                        "aspect": asp_name,
                        "exact_angle": exact,
                        "actual_angle": round(d, 6),
                        "orb": round(delta, 6),
                        "orb_limit": float(orb_limit),
                        "from_a": a_name,
                        "to_b": b_name
                    })
                    break
    syn_aspects.sort(key=lambda x: x["orb"])

    a_houses_raw = {k: a_chart["houses"][k]["ecliptic_longitude"] for k in a_chart["houses"].keys()}
    overlays = {}
    for b_name, b_lon in b_lons.items():
        overlays[b_name] = planet_house(b_lon, a_houses_raw)

    out = {
        "person_a": {
            "ascendant": a_chart["ascendant"],
            "mc": a_chart["mc"],
            "bodies": a_chart["bodies"],
            "bodies_meta": a_chart.get("bodies_meta", {}),
            "balance": a_chart.get("balance", {}),
            "stelliums": a_chart.get("stelliums", {}),
            "aspect_patterns": a_chart.get("aspect_patterns", {})
        },
        "person_b": {
            "ascendant": b_chart["ascendant"],
            "mc": b_chart["mc"],
            "bodies": b_chart["bodies"],
            "bodies_meta": b_chart.get("bodies_meta", {}),
            "balance": b_chart.get("balance", {}),
            "stelliums": b_chart.get("stelliums", {}),
            "aspect_patterns": b_chart.get("aspect_patterns", {})
        },
        "synastry_aspects": syn_aspects[:200],
        "b_planets_in_a_houses": overlays
    }
    return jsonify(out)

@app.route("/composite", methods=["POST"])
def composite():
    payload = request.json or {}
    person_a = payload.get("person_a")
    person_b = payload.get("person_b")
    if not person_a or not person_b:
        return jsonify({"error": "Missing required fields: person_a, person_b"}), 400

    a_chart, a_err = build_chart(person_a)
    if a_err:
        msg, code = a_err
        return jsonify({"error": f"Person A error: {msg}"}), code

    b_chart, b_err = build_chart(person_b)
    if b_err:
        msg, code = b_err
        return jsonify({"error": f"Person B error: {msg}"}), code

    comp_lons = {}
    for k in a_chart["bodies"].keys():
        a_lon = a_chart["bodies"][k]["ecliptic_longitude"]
        b_lon = b_chart["bodies"][k]["ecliptic_longitude"]
        comp_lons[k] = midpoint_angle(a_lon, b_lon)

    comp_asc = midpoint_angle(
        a_chart["ascendant"]["ecliptic_longitude"],
        b_chart["ascendant"]["ecliptic_longitude"]
    )
    comp_mc = midpoint_angle(
        a_chart["mc"]["ecliptic_longitude"],
        b_chart["mc"]["ecliptic_longitude"]
    )

    comp_aspects = aspects_between(comp_lons, comp_lons)
    dedup = []
    seen = set()
    for a in comp_aspects:
        pair = tuple(sorted([a["body_1"], a["body_2"]])) + (a["aspect"],)
        if pair in seen:
            continue
        seen.add(pair)
        dedup.append(a)

    out = {
        "composite": {
            "ascendant": deg_to_sign(comp_asc),
            "mc": deg_to_sign(comp_mc),
            "bodies": {k: deg_to_sign(v) for k, v in comp_lons.items()},
            "aspects": dedup[:200]
        },
        "note": "Composite is calculated via midpoints of longitudes (bodies + Asc/MC). Houses are not computed here."
    }
    return jsonify(out)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)