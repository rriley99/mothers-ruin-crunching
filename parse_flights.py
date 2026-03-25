import csv
import re
import os

FLIGHTS_DIR = "flights"
OUTPUT_FILE = "flights.csv"

def parse_flight_file(filepath):
    filename = os.path.basename(filepath).replace(".txt", "")
    origin_city, dest_city = filename.split("_")
    origin_city = origin_city.upper()
    dest_city = dest_city.upper()

    with open(filepath) as f:
        text = f.read()

    # Split into blocks separated by blank lines
    blocks = re.split(r"\n\n+", text.strip())

    flights = []
    for block in blocks:
        lines = [l.strip() for l in block.strip().splitlines() if l.strip()]

        # Skip header blocks
        if lines[0].startswith("All flights") or lines[0].startswith("Prices include"):
            continue

        # Parse the flight block
        try:
            # Line 0: departure time (e.g. "7:05 AM –" or "7:05 AM –")
            # Line 1: arrival time (e.g. "10:10 AM")
            dep_time = lines[0].rstrip(" –\u2013\u2014-").strip()
            arr_time = lines[1].strip()

            # Line 2: airline
            airline_raw = lines[2]
            # Extract base airline and operated-by info
            operated_match = re.match(r"(\w+)Operated by (.+)", airline_raw)
            if operated_match:
                airline = operated_match.group(1)
                operated_by = operated_match.group(2)
            else:
                airline = airline_raw
                operated_by = ""

            # Line 3: duration
            duration = lines[3]

            # Line 4: route (e.g. ORD–AUS)
            route = lines[4]
            # Extract actual origin/dest airports from route
            airports = re.split(r"[–\-]", route)
            origin_airport = airports[0].strip()
            dest_airport = airports[1].strip() if len(airports) > 1 else ""

            # Line 5: stops
            stops = lines[5]

            # Remaining lines: CO2, optional emissions%, two numbers, price
            rest = lines[6:]
            co2 = ""
            emissions_pct = ""
            price = ""

            i = 0
            # CO2 line
            if i < len(rest) and "CO2" in rest[i]:
                co2 = rest[i]
                i += 1

            # Optional emissions percentage line
            if i < len(rest) and "emissions" in rest[i].lower():
                emissions_pct = rest[i]
                i += 1

            # Skip the two mystery numbers
            while i < len(rest) and not rest[i].startswith("$"):
                i += 1

            # Price
            if i < len(rest) and rest[i].startswith("$"):
                price = rest[i]

            flights.append({
                "origin_city": origin_city,
                "dest_city": dest_city,
                "origin_airport": origin_airport,
                "dest_airport": dest_airport,
                "departure": dep_time,
                "arrival": arr_time,
                "airline": airline,
                "operated_by": operated_by,
                "duration": duration,
                "stops": stops,
                "co2": co2,
                "emissions_vs_avg": emissions_pct,
                "price": price,
            })
        except (IndexError, ValueError) as e:
            print(f"Warning: Could not parse block in {filepath}: {lines[:3]}... ({e})")

    return flights


all_flights = []
for fname in sorted(os.listdir(FLIGHTS_DIR)):
    if fname.endswith(".txt"):
        fpath = os.path.join(FLIGHTS_DIR, fname)
        flights = parse_flight_file(fpath)
        all_flights.extend(flights)

fieldnames = [
    "origin_city", "dest_city", "origin_airport", "dest_airport",
    "departure", "arrival", "airline", "operated_by",
    "duration", "stops", "co2", "emissions_vs_avg", "price",
]

with open(OUTPUT_FILE, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_flights)

print(f"Parsed {len(all_flights)} flights from {len(os.listdir(FLIGHTS_DIR))} files into {OUTPUT_FILE}")
