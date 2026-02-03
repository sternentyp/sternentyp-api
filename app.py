from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import pytz
import time
from collections import defaultdict, deque

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
}

ASPECTS = [
    ("Konjunktion", 0.0, 8.0),
    ("Sextil", 60.0, 6.0),
    ("Quadrat", 90.0, 6.0),
    ("Trigon", 120.0, 6.0),
    ("Opposition", 180.0, 8.0),
]

# -------------------------
# ABUSE-SCHUTZ (LIGHT)
# -------------------------
RATE_LIMIT = 90        # max requests
RATE_WINDOW = 60       # per 60 seconds
_ip_requests = defaultdict(lambda: deque())

def get_client_ip():
    # Render/Proxy liefert oft X-Forwarded-For (ggf. Liste)
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
    """Midpoint on circle (0..360)"""
    a = norm360(a)
    b = norm360(b)
    d = (b - a + 360.0) % 360.0
    if d > 180.0:
        d -= 360.0
    return norm360(a + d / 2.0)

# -------------------------
# GEO + TZ (FIX: NIE WIEDER 500)
# -------------------------
def get_latlon_from_place(place_name: str):
    """
    Robust: Timeout + Exceptions -> gibt (None, (msg, code)) statt Exception/500
    """
    if not place_name:
        return None, ("Provide either (lat, lon) or place", 400)

    cached = geo_cache_get(place_name)
    if cached:
        return cached, None

    try:
        geolocator = Nominatim(
            user_agent="sternentyp",
            timeout=6
        )
        loc = geolocator.geocode(place_name, language="de")
        if not loc:
            return None, ("Could not geocode place. Provide lat/lon for accuracy.", 400)

        lat, lon = float(loc.latitude), float(loc.longitude)
        geo_cache_set(place_name, lat, lon)
        return (lat, lon), None

    except (GeocoderUnavailable, GeocoderTimedOut, GeocoderServiceError):
        # -> 503 statt 500
        return None, (
            "Geocoding service temporarily unavailable. Please provide lat/lon.",
            503
        )
    except Exception:
        return None, (
            "Geocoding failed unexpectedly. Please provide lat/lon.",
            503
        )

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

def calc_bodies(jd_ut: float, flags: int):
    out = {}
    for name, p in BODIES.items():
        lonlat, _ = swe.calc_ut(jd_ut, p, flags)
        out[name] = lonlat[0] % 360.0
    return out

def calc_houses(jd_ut: float, lat: float, lon: float, house_system: str):
    hsys = str(house_system)[0].encode("ascii")
    houses, ascmc = swe.houses(jd_ut, float(lat), float(lon), hsys)
    if len(houses) == 13:
        cusp_list = list(houses[1:13])
    else:
        cusp_list = list(houses[0:12])
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

    # geocode if needed (ROBUST)
    if lat is None or lon is None:
        if not place:
            return None, ("Provide either (lat, lon) or place", 400)
        ll, geo_err = get_latlon_from_place(place)
        if geo_err:
            return None, geo_err
        lat, lon = ll

    # infer tz if missing
    if not tz_name:
        tz_name = infer_timezone(float(lat), float(lon))
        if not tz_name:
            return None, ("Could not infer timezone, please provide timezone", 400)

    _, utc_dt = parse_input_datetime(date_str, time_str, tz_name)
    jd_ut = jd_ut_from_utc(utc_dt)
    flags = zodiac_flags(zodiac)

    houses_out, asc, mc = calc_houses(jd_ut, float(lat), float(lon), house_system)
    bodies_lon = calc_bodies(jd_ut, flags)

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
        "planet_houses": planet_houses,
        "aspects": aspects
    }
    return result, None

# -------------------------
# ROUTES
# -------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

# FREE: Natal Chart
@app.route("/chart", methods=["POST"])
def chart():
    payload = request.json or {}
    chart_result, err = build_chart(payload)
    if err:
        msg, code = err
        return jsonify({"error": msg}), code
    return jsonify(chart_result)

# FREE: Transits for a period
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
            lonlat, _ = swe.calc_ut(jd_ut, BODIES[name], flags)
            trans_lons[name] = lonlat[0] % 360.0

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
            "bodies": natal_result["bodies"]
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

# FREE: Synastry (A vs B)
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
            "bodies": a_chart["bodies"]
        },
        "person_b": {
            "ascendant": b_chart["ascendant"],
            "mc": b_chart["mc"],
            "bodies": b_chart["bodies"]
        },
        "synastry_aspects": syn_aspects[:200],
        "b_planets_in_a_houses": overlays
    }
    return jsonify(out)

# OPTIONAL: Composite (Midpoints)
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

    # midpoints for planets
    comp_lons = {}
    for k in a_chart["bodies"].keys():
        a_lon = a_chart["bodies"][k]["ecliptic_longitude"]
        b_lon = b_chart["bodies"][k]["ecliptic_longitude"]
        comp_lons[k] = midpoint_angle(a_lon, b_lon)

    # midpoint angles
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
        "note": "Composite is calculated via midpoints of longitudes (planets + Asc/MC). Houses are not computed here."
    }
    return jsonify(out)

# -------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
