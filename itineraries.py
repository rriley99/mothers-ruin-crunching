import csv
import re
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo
from itertools import permutations, combinations, product

FLIGHTS_CSV = "flights.csv"
OUTPUT_CSV = "itineraries.csv"
DB_FILE = "flights.db"
ALL_CITIES = ["NYC", "BNA", "CHS", "ORD", "AUS"]
REQUIRED = "CHS"
NUM_CITIES = 4
FLIGHT_DATE = "2026-05-10"

AIRPORT_TZ = {
    "AUS": "America/Chicago",
    "BNA": "America/Chicago",
    "CHS": "America/New_York",
    "ORD": "America/Chicago",
    "JFK": "America/New_York",
    "LGA": "America/New_York",
    "EWR": "America/New_York",
}

def parse_price(price_str):
    return int(price_str.replace("$", "").replace(",", ""))

def parse_duration_minutes(dur_str):
    hours = minutes = 0
    h_match = re.search(r"(\d+)\s*hr", dur_str)
    m_match = re.search(r"(\d+)\s*min", dur_str)
    if h_match:
        hours = int(h_match.group(1))
    if m_match:
        minutes = int(m_match.group(1))
    return hours * 60 + minutes

def parse_time(time_str, airport_code):
    """Convert '6:40 AM' or '12:35 AM+1' to a tz-aware datetime and a local time string."""
    plus_day = 0
    cleaned = time_str.strip()
    if "+1" in cleaned:
        plus_day = 1
        cleaned = cleaned.replace("+1", "").strip()
    dt = datetime.strptime(f"{FLIGHT_DATE} {cleaned}", "%Y-%m-%d %I:%M %p")
    if plus_day:
        dt = dt.replace(day=dt.day + plus_day)
    tz = ZoneInfo(AIRPORT_TZ[airport_code])
    dt = dt.replace(tzinfo=tz)
    local_str = dt.strftime("%-I:%M %p")
    return dt, local_str

# Load flights, keyed by (origin_city, dest_city)
flights_by_route = {}
with open(FLIGHTS_CSV) as f:
    reader = csv.DictReader(f)
    for row in reader:
        row["departure_dt"], row["departure_local"] = parse_time(row["departure"], row["origin_airport"])
        row["arrival_dt"], row["arrival_local"] = parse_time(row["arrival"], row["dest_airport"])
        key = (row["origin_city"], row["dest_city"])
        flights_by_route.setdefault(key, []).append(row)

# Generate all 4-city sets that include CHS
others = [c for c in ALL_CITIES if c != REQUIRED]
city_sets = [frozenset([REQUIRED] + list(combo)) for combo in combinations(others, NUM_CITIES - 1)]

print(f"City sets: {len(city_sets)}")

# For each city set, generate all orderings, then all flight combos per ordering
itineraries = []
for city_set in city_sets:
    for perm in permutations(city_set):
        # Build legs: perm[0]->perm[1], perm[1]->perm[2], perm[2]->perm[3]
        legs = [(perm[i], perm[i + 1]) for i in range(len(perm) - 1)]

        # Get available flights for each leg
        leg_flights = []
        valid = True
        for leg in legs:
            available = flights_by_route.get(leg, [])
            if not available:
                valid = False
                break
            leg_flights.append(available)

        if not valid:
            continue

        # Enumerate all combinations of flights across legs
        for combo in product(*leg_flights):
            # Validate: each leg must depart after the previous leg arrives
            feasible = True
            for j in range(1, len(combo)):
                prev_arrival = combo[j - 1]["arrival_dt"]
                curr_departure = combo[j]["departure_dt"]
                if curr_departure <= prev_arrival:
                    feasible = False
                    break
            if not feasible:
                continue

            total_price = sum(parse_price(f["price"]) for f in combo)
            total_minutes = sum(parse_duration_minutes(f["duration"]) for f in combo)
            total_dur_hr = total_minutes // 60
            total_dur_min = total_minutes % 60

            row = {
                "route": " → ".join(perm),
                "total_price": total_price,
                "total_flight_time": f"{total_dur_hr}h {total_dur_min}m",
                "total_flight_minutes": total_minutes,
            }

            for i, flight in enumerate(combo, 1):
                row[f"leg{i}_flight"] = f"{flight['origin_airport']}→{flight['dest_airport']}"
                row[f"leg{i}_airline"] = flight["airline"]
                row[f"leg{i}_departure"] = flight["departure_local"]
                row[f"leg{i}_arrival"] = flight["arrival_local"]
                row[f"leg{i}_price"] = flight["price"]
                row[f"leg{i}_duration"] = flight["duration"]

            itineraries.append(row)

# Sort by total price
itineraries.sort(key=lambda x: x["total_price"])

fieldnames = [
    "route", "total_price", "total_flight_time", "total_flight_minutes",
    "leg1_flight", "leg1_airline", "leg1_departure", "leg1_arrival", "leg1_price", "leg1_duration",
    "leg2_flight", "leg2_airline", "leg2_departure", "leg2_arrival", "leg2_price", "leg2_duration",
    "leg3_flight", "leg3_airline", "leg3_departure", "leg3_arrival", "leg3_price", "leg3_duration",
]

with open(OUTPUT_CSV, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(itineraries)

# Write to SQLite
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

# Flights table (raw edges)
cur.execute("DROP TABLE IF EXISTS flights")
cur.execute("""
    CREATE TABLE flights (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        origin_city TEXT,
        dest_city TEXT,
        origin_airport TEXT,
        dest_airport TEXT,
        departure TEXT,
        arrival TEXT,
        airline TEXT,
        operated_by TEXT,
        duration TEXT,
        duration_minutes INTEGER,
        stops TEXT,
        co2 TEXT,
        emissions_vs_avg TEXT,
        price_raw TEXT,
        price INTEGER
    )
""")
for key, flight_list in flights_by_route.items():
    for row in flight_list:
        cur.execute("""
            INSERT INTO flights (origin_city, dest_city, origin_airport, dest_airport,
                departure, arrival, departure_tz, arrival_tz,
                airline, operated_by, duration, duration_minutes,
                stops, co2, emissions_vs_avg, price_raw, price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["origin_city"], row["dest_city"], row["origin_airport"], row["dest_airport"],
            row["departure"], row["arrival"], row["departure_iso"], row["arrival_iso"],
            row["airline"], row["operated_by"],
            row["duration"], parse_duration_minutes(row["duration"]),
            row["stops"], row["co2"], row["emissions_vs_avg"],
            row["price"], parse_price(row["price"]),
        ))

# Itineraries table
cur.execute("DROP TABLE IF EXISTS itineraries")
cur.execute("""
    CREATE TABLE itineraries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        route TEXT,
        total_price INTEGER,
        total_flight_time TEXT,
        total_flight_minutes INTEGER,
        leg1_flight TEXT,
        leg1_airline TEXT,
        leg1_departure TEXT,
        leg1_arrival TEXT,
        leg1_price TEXT,
        leg1_duration TEXT,
        leg2_flight TEXT,
        leg2_airline TEXT,
        leg2_departure TEXT,
        leg2_arrival TEXT,
        leg2_price TEXT,
        leg2_duration TEXT,
        leg3_flight TEXT,
        leg3_airline TEXT,
        leg3_departure TEXT,
        leg3_arrival TEXT,
        leg3_price TEXT,
        leg3_duration TEXT
    )
""")
cur.executemany("""
    INSERT INTO itineraries (route, total_price, total_flight_time, total_flight_minutes,
        leg1_flight, leg1_airline, leg1_departure, leg1_arrival, leg1_price, leg1_duration,
        leg2_flight, leg2_airline, leg2_departure, leg2_arrival, leg2_price, leg2_duration,
        leg3_flight, leg3_airline, leg3_departure, leg3_arrival, leg3_price, leg3_duration)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", [
    (r["route"], r["total_price"], r["total_flight_time"], r["total_flight_minutes"],
     r["leg1_flight"], r["leg1_airline"], r["leg1_departure"], r["leg1_arrival"], r["leg1_price"], r["leg1_duration"],
     r["leg2_flight"], r["leg2_airline"], r["leg2_departure"], r["leg2_arrival"], r["leg2_price"], r["leg2_duration"],
     r["leg3_flight"], r["leg3_airline"], r["leg3_departure"], r["leg3_arrival"], r["leg3_price"], r["leg3_duration"])
    for r in itineraries
])

conn.commit()
conn.close()

print(f"Generated {len(itineraries)} itineraries across {len(city_sets)} city sets")
print(f"Written to: {OUTPUT_CSV}, {DB_FILE}")
print(f"Price range: ${itineraries[0]['total_price']} – ${itineraries[-1]['total_price']}")
