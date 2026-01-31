from flask import Flask, request, jsonify
from datetime import datetime
import os
import pytz
import swisseph as swe
from timezonefinder import TimezoneFinder
from geopy.geocoders import Nominatim

app = Flask(__name__)

# Ephemeriden-Pfad (Ordner muss im Projekt liegen!)
swe.set_ephe_path("./ephe")

ZODIAC_SIGNS = [
    "Widder", "Stier", "Zwillinge", "Krebs", "Löwe", "Jungfrau",
    "Waage", "Skorpion", "Schütze", "Steinbock", "Wassermann", "Fische"
]

# Hauptplaneten
PLANETS = {
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

# Zusätzliche Punkte / Körper
EXTRAS = {
    "Chiron": swe.CHIRON,
    "Lilith": swe.MEAN_APOG,        # Mittel-Apogäum (Black Moon Lilith)
    "Mondknoten": swe.MEAN_NODE,    # Mean North Node
}

# Aspektdefinitionen (Grad)
ASPECTS = [
    {"name": "Konjunktion", "angle": 0},
    {"name": "Sextil", "angle": 60},
    {"name": "Quadrat", "angle": 90},
    {"name": "Trigon", "angle": 120},
    {"name": "Opposition", "angle": 180},
]


def deg_to_sign(deg: float):
    """Ecliptic longitude (0..360) -> Zeichen + Grad im Zeichen"""
    deg = deg % 360.0
    sign_index = int(deg // 30) % 12
    sign_deg = deg % 30
    return {
        "zeichen": ZODIAC_SIGNS[sign_index],
        "grad": round(sign_deg, 6),
        "ecliptic_longitude": round(deg, 6),
    }


def get_latlon_from_place(place_name: str):
    """Place -> (lat, lon) via Nominatim (kann Rate-Limits haben)"""
    geolocator = Nominatim(user_agent="sternentyp")
    loc = geolocator.geocode(place_name, language="de")
    if not loc:
        return None
    return float(loc.latitude), float(loc.longitude)


def infer_timezone(lat: float, lon: float):
    """lat/lon -> timezone name"""
    tf = TimezoneFinder()
    return tf.timezone_at(lat=lat, lng=lon)


def planet_house(ecl_lon: float, cusp_list: list[float]) -> int:
    """
    Bestimmt in welchem Haus (1..12) ein Punkt liegt.
    cusp_list: 12 Cusps als ekliptikale Längen (0..360) für Haus 1..12.
    """
    lon = ecl_lon % 360.0
    cusps = [c % 360.0 for c in cusp_list]

    for i in range(12):
        start = cusps[i]
        end = cusps[(i + 1) % 12]

        if start <= end:
            if start <= lon < end:
                return i + 1
        else:
            if lon >= start or lon < end:
                return i + 1

    return 12


def angle_diff(a: float, b: float) -> float:
    """Kleinster Winkelabstand zwischen zwei Längen (0..180)."""
    d = abs((a - b) % 360.0)
    return d if d <= 180.0 else 360.0 - d


def orb_limit(body_a: str, body_b: str) -> float:
    """
    Orbs:
    - Sonne/Mond: 8°
    - andere: 6°
    """
    if body_a in ("Sonne", "Mond") or body_b in ("Sonne", "Mond"):
        return 8.0
    return 6.0


def compute_aspects(positions_deg: dict) -> list:
    """
    positions_deg: {"Sonne": 270.1, "Mond": 67.3, ...}
    """
    names = list(positions_deg.keys())
    aspects_found = []

    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            a = names[i]
            b = names[j]
            lon1 = positions_deg[a] % 360.0
            lon2 = positions_deg[b] % 360.0

            diff = angle_diff(lon1, lon2)
            max_orb = orb_limit(a, b)

            best = None
            for asp in ASPECTS:
                target = float(asp["angle"])
                delta = abs(diff - target)
                if delta <= max_orb:
                    if best is None or delta < best["orb"]:
                        best = {
                            "body_1": a,
                            "body_2": b,
                            "aspect": asp["name"],
                            "exact_angle": target,
                            "actual_angle": round(diff, 6),
                            "orb": round(delta, 6),
                            "orb_limit": max_orb,
                        }

            if best:
                aspects_found.append(best)

    aspects_found.sort(key=lambda x: x["orb"])
    return aspects_found


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/chart", methods=["POST"])
def chart():
    data = request.json or {}

    date_str = data.get("date")
    time_str = data.get("time")
    place = data.get("place")
    lat = data.get("lat")
    lon = data.get("lon")
    tz_name = data.get("timezone")

    house_system = data.get("house_system", "P")
    zodiac = data.get("zodiac", "tropical")

    if not date_str or not time_str:
        return jsonify({"error": "Missing required fields: date, time"}), 400

    # Koordinaten
    if lat is None or lon is None:
        if not place:
            return jsonify({"error": "Provide either (lat, lon) or place"}), 400
        ll = get_latlon_from_place(place)
        if not ll:
            return jsonify({
                "error": "Could not geocode place",
                "hint": "Bitte gib lat/lon mit an (Geburtsort unklar oder Rate-Limit)."
            }), 400
        lat, lon = ll
    else:
        lat, lon = float(lat), float(lon)

    # Zeitzone
    if not tz_name:
        tz_name = infer_timezone(lat, lon)
        if not tz_name:
            return jsonify({"error": "Could not infer timezone, please provide timezone"}), 400

    try:
        local_tz = pytz.timezone(tz_name)
    except Exception:
        return jsonify({"error": f"Invalid timezone: {tz_name}"}), 400

    try:
        naive_local = datetime.fromisoformat(f"{date_str}T{time_str}:00")
    except Exception:
        return jsonify({"error": "Invalid date/time format. Use date YYYY-MM-DD and time HH:MM"}), 400

    try:
        aware_local = local_tz.localize(naive_local, is_dst=None)
    except Exception as e:
        return jsonify({
            "error": "Ambiguous or invalid local time (DST issue). Provide timezone explicitly or adjust time.",
            "details": str(e)
        }), 400

    utc_dt = aware_local.astimezone(pytz.UTC)

    # Julian Day
    jd_ut = swe.julday(
        utc_dt.year, utc_dt.month, utc_dt.day,
        utc_dt.hour + utc_dt.minute / 60.0 + utc_dt.second / 3600.0
    )

    # Flags
    if zodiac == "sidereal":
        swe.set_sid_mode(swe.SIDM_FAGAN_BRADLEY, 0, 0)
        flags = swe.FLG_SWIEPH | swe.FLG_SIDEREAL
    else:
        flags = swe.FLG_SWIEPH

    # Häuser
    hsys = str(house_system)[0].encode("ascii")
    try:
        houses, ascmc = swe.houses(jd_ut, lat, lon, hsys)
    except Exception as e:
        return jsonify({"error": "houses() failed", "details": str(e)}), 500

    asc = ascmc[0] if len(ascmc) > 0 else None
    mc = ascmc[1] if len(ascmc) > 1 else None
    if asc is None or mc is None:
        return jsonify({"error": "Could not compute ascendant/mc"}), 500

    if len(houses) == 13:
        cusp_list = list(houses[1:13])
    elif len(houses) == 12:
        cusp_list = list(houses[0:12])
    else:
        return jsonify({"error": f"Unexpected houses/cusps length: {len(houses)}"}), 500

    houses_out = {f"haus_{i}": deg_to_sign(cusp_list[i - 1]) for i in range(1, 13)}

    # Positionen sammeln (Planeten + Extras)
    bodies_out = {}
    body_houses = {}
    positions_deg = {}

    # Planeten
    for name, code in PLANETS.items():
        pos, _ = swe.calc_ut(jd_ut, code, flags)
        lon_ecl = float(pos[0]) % 360.0
        positions_deg[name] = lon_ecl
        bodies_out[name] = deg_to_sign(lon_ecl)
        body_houses[name] = planet_house(lon_ecl, cusp_list)

    # Extras
    for name, code in EXTRAS.items():
        pos, _ = swe.calc_ut(jd_ut, code, flags)
        lon_ecl = float(pos[0]) % 360.0
        positions_deg[name] = lon_ecl
        bodies_out[name] = deg_to_sign(lon_ecl)
        body_houses[name] = planet_house(lon_ecl, cusp_list)

    # Aspekte über alle Bodies (Planeten + Extras)
    aspects = compute_aspects(positions_deg)

    result = {
        "input": {
            "date": date_str,
            "time": time_str,
            "place": place,
            "lat": lat,
            "lon": lon,
            "timezone": tz_name,
            "house_system": str(house_system)[0],
            "zodiac": zodiac,
        },
        "utc": utc_dt.isoformat(),
        "jd_ut": jd_ut,
        "ascendant": deg_to_sign(asc),
        "mc": deg_to_sign(mc),
        "houses": houses_out,
        "bodies": bodies_out,
        "body_houses": body_houses,
        "aspects": aspects,
        "notes": {
            "lilith_type": "MEAN_APOG (Black Moon Lilith mean)",
            "node_type": "MEAN_NODE (North Node mean)",
        }
    }

    return jsonify(result)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
