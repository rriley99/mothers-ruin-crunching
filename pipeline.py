"""
Mother's Ruin Trip Planner — Data Pipeline
Parses raw Google Flights text dumps, enriches with timezone-aware datetimes,
computes all valid multi-city itineraries, and loads everything into SQLite.

Usage:
    python pipeline.py [--flights-dir flights] [--db flights.db]
"""

import argparse
import csv
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta
from itertools import combinations, permutations, product
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FLIGHT_DATE = "2026-05-10"
ALL_CITIES = ["NYC", "BNA", "CHS", "ORD", "AUS"]
REQUIRED_CITY = "CHS"
NUM_CITIES = 4

# Airport → city code (for NYC's three airports)
AIRPORT_TO_CITY: dict[str, str] = {
    "AUS": "AUS", "BNA": "BNA", "CHS": "CHS", "ORD": "ORD",
    "JFK": "NYC", "LGA": "NYC", "EWR": "NYC",
}

AIRPORT_TZ: dict[str, str] = {
    "AUS": "America/Chicago",
    "BNA": "America/Chicago",
    "CHS": "America/New_York",
    "MDW": "America/Chicago",
    "ORD": "America/Chicago",
    "JFK": "America/New_York",
    "LGA": "America/New_York",
    "EWR": "America/New_York",
}

# City → canonical airport code for timezone lookup (fallback)
CITY_TZ: dict[str, str] = {
    "AUS": "America/Chicago",
    "BNA": "America/Chicago",
    "CHS": "America/New_York",
    "NYC": "America/New_York",
    "ORD": "America/Chicago",
}


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _airport_tz(code: str) -> str:
    tz = AIRPORT_TZ.get(code)
    if tz:
        return tz
    # Fallback: try city code
    city = code[:3]
    return CITY_TZ.get(city, "America/Chicago")


def _parse_price(s: str) -> int:
    return int(s.replace("$", "").replace(",", "").strip())


def _parse_duration_minutes(s: str) -> int:
    h = int(m.group(1)) if (m := re.search(r"(\d+)\s*hr", s)) else 0
    mins = int(m.group(1)) if (m := re.search(r"(\d+)\s*min", s)) else 0
    return h * 60 + mins


def _parse_local_time(time_str: str, airport: str) -> datetime:
    """Parse a local time string like '6:40 AM' or '12:35 AM+1' into a
    timezone-aware datetime on FLIGHT_DATE."""
    cleaned = time_str.strip()
    extra_days = 0
    if "+1" in cleaned:
        extra_days = 1
        cleaned = cleaned.replace("+1", "").strip()

    naive = datetime.strptime(f"{FLIGHT_DATE} {cleaned}", "%Y-%m-%d %I:%M %p")
    if extra_days:
        naive += timedelta(days=extra_days)

    tz = ZoneInfo(_airport_tz(airport))
    return naive.replace(tzinfo=tz)


# ---------------------------------------------------------------------------
# Flight file parser
# ---------------------------------------------------------------------------

def parse_flight_file(filepath: str) -> list[dict]:
    """Parse a single flights/*.txt file and return enriched flight dicts."""
    name = os.path.basename(filepath).replace(".txt", "")
    origin_city, dest_city = name.split("_")
    origin_city = origin_city.upper()
    dest_city = dest_city.upper()

    with open(filepath, encoding="utf-8") as f:
        text = f.read()

    blocks = re.split(r"\n\n+", text.strip())
    flights = []

    for block in blocks:
        lines = [ln.strip() for ln in block.strip().splitlines() if ln.strip()]
        if not lines or lines[0].startswith(("All flights", "Prices include")):
            continue

        try:
            dep_str = lines[0].rstrip(" \u2013\u2014-").strip()
            arr_str = lines[1].strip()

            airline_raw = lines[2]
            op_match = re.match(r"(.+?)Operated by (.+)", airline_raw)
            if op_match:
                airline = op_match.group(1).strip()
                operated_by = op_match.group(2).strip()
            else:
                airline = airline_raw.strip()
                operated_by = ""

            duration = lines[3]

            route_str = lines[4]
            airports = re.split(r"[–\-]", route_str)
            origin_airport = airports[0].strip()
            dest_airport = airports[1].strip() if len(airports) > 1 else dest_city

            stops = lines[5]

            rest = lines[6:]
            co2 = emissions_pct = price_raw = ""
            i = 0
            if i < len(rest) and "CO2" in rest[i]:
                co2 = rest[i]; i += 1
            if i < len(rest) and "emissions" in rest[i].lower():
                emissions_pct = rest[i]; i += 1
            while i < len(rest) and not rest[i].startswith("$"):
                i += 1
            if i < len(rest):
                price_raw = rest[i]

            if not price_raw:
                continue

            dep_dt = _parse_local_time(dep_str, origin_airport)
            arr_dt = _parse_local_time(arr_str, dest_airport)

            flights.append({
                "origin_city": origin_city,
                "dest_city": dest_city,
                "origin_airport": origin_airport,
                "dest_airport": dest_airport,
                "departure": dep_str,
                "arrival": arr_str,
                "departure_tz": dep_dt.isoformat(),
                "arrival_tz": arr_dt.isoformat(),
                "departure_dt": dep_dt,       # in-memory only
                "arrival_dt": arr_dt,          # in-memory only
                "airline": airline,
                "operated_by": operated_by,
                "duration": duration,
                "duration_minutes": _parse_duration_minutes(duration),
                "stops": stops,
                "co2": co2,
                "emissions_vs_avg": emissions_pct,
                "price_raw": price_raw,
                "price": _parse_price(price_raw),
            })

        except (IndexError, ValueError) as e:
            print(f"  Warning: skipping block in {filepath}: {lines[:3]} ({e})")

    return flights


def parse_json_file(filepath: str) -> list[dict]:
    """Parse a single flights/*.json file (from scrape_flights.py) into enriched flight dicts."""
    name = os.path.basename(filepath).replace(".json", "")
    origin_city, dest_city = name.split("_")
    origin_city = origin_city.upper()
    dest_city = dest_city.upper()

    with open(filepath, encoding="utf-8") as f:
        raw = json.load(f)

    flights = []
    for r in raw:
        try:
            origin_airport = r["from"]
            dest_airport = r["to"]

            # Strip " on Sun, May 10" suffix that fast-flights appends
            dep_time = re.sub(r"\s+on\s+.+$", "", r["departure"].strip())
            arr_time = re.sub(r"\s+on\s+.+$", "", r["arrival"].strip())
            if r.get("arrival_time_ahead") == "+1":
                arr_time += "+1"

            dep_dt = _parse_local_time(dep_time, origin_airport)
            arr_dt = _parse_local_time(arr_time, dest_airport)

            stops_int = r["stops"]
            if isinstance(stops_int, int):
                stops_str = "Nonstop" if stops_int == 0 else f"{stops_int} stop{'s' if stops_int > 1 else ''}"
            else:
                stops_str = str(stops_int)

            flights.append({
                "origin_city": AIRPORT_TO_CITY.get(origin_airport, origin_city),
                "dest_city": AIRPORT_TO_CITY.get(dest_airport, dest_city),
                "origin_airport": origin_airport,
                "dest_airport": dest_airport,
                "departure": dep_time,
                "arrival": arr_time,
                "departure_tz": dep_dt.isoformat(),
                "arrival_tz": arr_dt.isoformat(),
                "departure_dt": dep_dt,
                "arrival_dt": arr_dt,
                "airline": r["airline"],
                "operated_by": "",
                "duration": r["duration"],
                "duration_minutes": _parse_duration_minutes(r["duration"]),
                "stops": stops_str,
                "co2": "",
                "emissions_vs_avg": "",
                "price_raw": r["price"],
                "price": _parse_price(r["price"]),
            })
        except (KeyError, ValueError) as e:
            print(f"  Warning: skipping record in {filepath}: {e}")

    return flights


def parse_all_flights(flights_dir: str) -> list[dict]:
    all_flights: list[dict] = []

    json_files = sorted(f for f in os.listdir(flights_dir) if f.endswith(".json"))
    txt_files = sorted(f for f in os.listdir(flights_dir) if f.endswith(".txt"))

    # Prefer JSON (live scrape) over txt (manual dump); fall back to txt if no JSON exists
    json_stems = {f[:-5] for f in json_files}
    txt_fallbacks = [f for f in txt_files if f[:-4] not in json_stems]

    sources = [(f, "json") for f in json_files] + [(f, "txt") for f in txt_fallbacks]
    print(f"Parsing {len(sources)} flight files ({len(json_files)} JSON, {len(txt_fallbacks)} txt fallbacks)...")

    for fname, fmt in sources:
        fpath = os.path.join(flights_dir, fname)
        flights = parse_json_file(fpath) if fmt == "json" else parse_flight_file(fpath)
        print(f"  {fname}: {len(flights)} flights")
        all_flights.extend(flights)

    return all_flights


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

DB_COLS_FLIGHTS = [
    "origin_city", "dest_city", "origin_airport", "dest_airport",
    "departure", "arrival", "departure_tz", "arrival_tz",
    "airline", "operated_by", "duration", "duration_minutes",
    "stops", "co2", "emissions_vs_avg", "price_raw", "price",
]

DB_COLS_ITINERARIES = [
    "route", "total_price", "total_flight_time", "total_flight_minutes",
    "layover1_minutes", "layover2_minutes",
    "leg1_flight", "leg1_airline", "leg1_departure", "leg1_arrival",
    "leg1_price", "leg1_duration",
    "leg2_flight", "leg2_airline", "leg2_departure", "leg2_arrival",
    "leg2_price", "leg2_duration",
    "leg3_flight", "leg3_airline", "leg3_departure", "leg3_arrival",
    "leg3_price", "leg3_duration",
]


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        DROP TABLE IF EXISTS flights;
        CREATE TABLE flights (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            origin_city      TEXT,
            dest_city        TEXT,
            origin_airport   TEXT,
            dest_airport     TEXT,
            departure        TEXT,
            arrival          TEXT,
            departure_tz     TEXT,
            arrival_tz       TEXT,
            airline          TEXT,
            operated_by      TEXT,
            duration         TEXT,
            duration_minutes INTEGER,
            stops            TEXT,
            co2              TEXT,
            emissions_vs_avg TEXT,
            price_raw        TEXT,
            price            INTEGER
        );

        DROP TABLE IF EXISTS itineraries;
        CREATE TABLE itineraries (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            route                 TEXT,
            total_price           INTEGER,
            total_flight_time     TEXT,
            total_flight_minutes  INTEGER,
            layover1_minutes      INTEGER,
            layover2_minutes      INTEGER,
            leg1_flight           TEXT,
            leg1_airline          TEXT,
            leg1_departure        TEXT,
            leg1_arrival          TEXT,
            leg1_price            INTEGER,
            leg1_duration         TEXT,
            leg2_flight           TEXT,
            leg2_airline          TEXT,
            leg2_departure        TEXT,
            leg2_arrival          TEXT,
            leg2_price            INTEGER,
            leg2_duration         TEXT,
            leg3_flight           TEXT,
            leg3_airline          TEXT,
            leg3_departure        TEXT,
            leg3_arrival          TEXT,
            leg3_price            INTEGER,
            leg3_duration         TEXT
        );
    """)


def load_flights(conn: sqlite3.Connection, flights: list[dict]) -> None:
    placeholders = ", ".join("?" for _ in DB_COLS_FLIGHTS)
    cols = ", ".join(DB_COLS_FLIGHTS)
    conn.executemany(
        f"INSERT INTO flights ({cols}) VALUES ({placeholders})",
        [[f[c] for c in DB_COLS_FLIGHTS] for f in flights],
    )
    print(f"  Loaded {len(flights)} flights into DB")


# ---------------------------------------------------------------------------
# Itinerary computation
# ---------------------------------------------------------------------------

def compute_itineraries(all_flights: list[dict]) -> list[dict]:
    """Enumerate all valid N-city itineraries that include REQUIRED_CITY."""
    by_route: dict[tuple, list[dict]] = {}
    for f in all_flights:
        key = (f["origin_city"], f["dest_city"])
        by_route.setdefault(key, []).append(f)

    others = [c for c in ALL_CITIES if c != REQUIRED_CITY]
    city_sets = [
        frozenset([REQUIRED_CITY] + list(combo))
        for combo in combinations(others, NUM_CITIES - 1)
    ]

    itineraries: list[dict] = []
    for city_set in city_sets:
        for perm in permutations(city_set):
            legs = [(perm[i], perm[i + 1]) for i in range(len(perm) - 1)]
            leg_options = []
            valid = True
            for leg in legs:
                opts = by_route.get(leg, [])
                if not opts:
                    valid = False
                    break
                leg_options.append(opts)
            if not valid:
                continue

            for combo in product(*leg_options):
                # Validate sequential timing
                feasible = True
                for j in range(1, len(combo)):
                    if combo[j]["departure_dt"] <= combo[j - 1]["arrival_dt"]:
                        feasible = False
                        break
                if not feasible:
                    continue

                total_price = sum(f["price"] for f in combo)
                total_minutes = sum(f["duration_minutes"] for f in combo)

                layover1 = int(
                    (combo[1]["departure_dt"] - combo[0]["arrival_dt"]).total_seconds() / 60
                )
                layover2 = int(
                    (combo[2]["departure_dt"] - combo[1]["arrival_dt"]).total_seconds() / 60
                )

                row: dict = {
                    "route": " \u2192 ".join(perm),
                    "total_price": total_price,
                    "total_flight_time": f"{total_minutes // 60}h {total_minutes % 60}m",
                    "total_flight_minutes": total_minutes,
                    "layover1_minutes": layover1,
                    "layover2_minutes": layover2,
                }
                for i, flight in enumerate(combo, 1):
                    row[f"leg{i}_flight"] = f"{flight['origin_airport']}\u2192{flight['dest_airport']}"
                    row[f"leg{i}_airline"] = flight["airline"]
                    row[f"leg{i}_departure"] = flight["departure_tz"]
                    row[f"leg{i}_arrival"] = flight["arrival_tz"]
                    row[f"leg{i}_price"] = flight["price"]
                    row[f"leg{i}_duration"] = flight["duration"]

                itineraries.append(row)

    itineraries.sort(key=lambda x: x["total_price"])
    return itineraries


def load_itineraries(conn: sqlite3.Connection, itineraries: list[dict]) -> None:
    placeholders = ", ".join("?" for _ in DB_COLS_ITINERARIES)
    cols = ", ".join(DB_COLS_ITINERARIES)
    conn.executemany(
        f"INSERT INTO itineraries ({cols}) VALUES ({placeholders})",
        [[row[c] for c in DB_COLS_ITINERARIES] for row in itineraries],
    )
    print(f"  Loaded {len(itineraries)} itineraries into DB")


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def export_flights_csv(flights: list[dict], out_path: str) -> None:
    cols = [c for c in DB_COLS_FLIGHTS if c not in ("departure_dt", "arrival_dt")]
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows([{c: row[c] for c in cols} for row in flights])
    print(f"  Wrote {len(flights)} rows to {out_path}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_pipeline(flights_dir: str = "flights", db_file: str = "flights.db") -> dict:
    print("\n=== Mother's Ruin Trip Planner — Pipeline ===\n")

    # 1. Parse
    all_flights = parse_all_flights(flights_dir)
    print(f"\nTotal flights parsed: {len(all_flights)}")

    # 2. Compute itineraries
    print("\nComputing itineraries...")
    itineraries = compute_itineraries(all_flights)
    print(f"Total valid itineraries: {len(itineraries)}")
    if itineraries:
        print(f"Price range: ${itineraries[0]['total_price']} – ${itineraries[-1]['total_price']}")

    # 3. Load to DB
    print(f"\nLoading to {db_file}...")
    conn = sqlite3.connect(db_file)
    try:
        init_db(conn)
        load_flights(conn, all_flights)
        load_itineraries(conn, itineraries)
        conn.commit()
    finally:
        conn.close()

    # 4. Export flights CSV
    export_flights_csv(all_flights, "flights.csv")

    print("\nPipeline complete.")
    return {
        "flights": len(all_flights),
        "itineraries": len(itineraries),
        "min_price": itineraries[0]["total_price"] if itineraries else 0,
        "max_price": itineraries[-1]["total_price"] if itineraries else 0,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mother's Ruin flight pipeline")
    parser.add_argument("--flights-dir", default="flights")
    parser.add_argument("--db", default="flights.db")
    args = parser.parse_args()
    run_pipeline(flights_dir=args.flights_dir, db_file=args.db)
