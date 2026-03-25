# Mother's Ruin Trip Planner

A flight itinerary explorer for a 4-city trip on **May 10, 2026** (Mother's Day weekend).
Parses raw Google Flights text exports, computes every valid multi-city routing,
and surfaces them through a dark-themed interactive dashboard.

Cities in play: **Austin (AUS) · Nashville (BNA) · Charleston (CHS) · Chicago (ORD) · New York (NYC)**

---

## Quick Start

```bash
# 1. Clone and enter the repo
git clone <repo-url>
cd mothers-ruin-crunching

# 2. Create a virtual environment and install dependencies
python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. Start the server
uvicorn app:app --reload

# 4. Open the dashboard
open http://localhost:8000
```

A pre-built `flights.db` is included so the dashboard works immediately.

---

## Project Layout

```
mothers-ruin-crunching/
├── flights/          # Raw Google Flights text exports (one file per route)
├── static/
│   └── index.html   # Single-page dashboard (no build step)
├── pipeline.py      # Parse raw files → SQLite (run to refresh data)
├── app.py           # FastAPI backend + API endpoints
├── flights.db       # SQLite database (pre-built)
├── flights.csv      # CSV snapshot of parsed flights
└── requirements.txt
```

---

## Refreshing the Data

If you update any files in `flights/`, rebuild the database:

```bash
python pipeline.py
# or with explicit paths:
python pipeline.py --flights-dir flights --db flights.db
```

You can also click **Refresh Data** in the dashboard header to trigger the pipeline
in the background without leaving the browser.

---

## Adding New Flight Data

Each file in `flights/` is named `<origin>_<dest>.txt` (e.g. `aus_bna.txt`) and contains
the raw copy-paste output from Google Flights for that route on the trip date.

To add a new route:
1. Search Google Flights for the route and date
2. Copy all flight results and paste into a new file: `flights/<origin>_<dest>.txt`
3. Run `python pipeline.py` to rebuild

---

## Dashboard Features

| Feature | Description |
|---|---|
| **Stat cards** | Total itineraries, cheapest option, average price, unique routes |
| **Route chart** | Horizontal bar chart of minimum price per route ordering |
| **Filters** | Price range, start/end city, must-include city, departure time, min layover, airlines |
| **Sort** | By total price, total duration, or route name |
| **Details panel** | Click any row to expand a proportional flight timeline with layover comfort indicators |
| **Load More** | Paginated results (50 at a time) |

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/stats` | Summary counts and price range |
| `GET` | `/api/routes` | Min/avg/max price per route |
| `GET` | `/api/airlines` | All airlines found in the data |
| `GET` | `/api/itineraries` | Filterable, sortable, paginated itinerary list |
| `POST` | `/api/pipeline/run` | Trigger a background data refresh |
| `GET` | `/api/pipeline/status` | Check pipeline run status |

### Itinerary query parameters

| Param | Default | Description |
|---|---|---|
| `min_price` | `0` | Minimum total price |
| `max_price` | `9999` | Maximum total price |
| `start_city` | — | First city code (e.g. `BNA`) |
| `end_city` | — | Last city code (e.g. `CHS`) |
| `must_include` | — | City that must appear somewhere in the route |
| `departs_after` | `0` | Earliest departure hour (local time, 0–23) |
| `min_layover` | `0` | Minimum layover at each connection (minutes) |
| `airlines` | all | Repeatable; restricts all legs to listed airlines |
| `sort` | `price` | `price`, `duration`, or `route` |
| `limit` | `50` | Results per page (max 200) |
| `offset` | `0` | Pagination offset |

---

## Requirements

- Python 3.11+
- See `requirements.txt` for packages (`fastapi`, `uvicorn`)
- All other dependencies (`sqlite3`, `zoneinfo`, `csv`, etc.) are stdlib
