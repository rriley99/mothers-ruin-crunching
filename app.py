"""
Mother's Ruin Trip Planner — FastAPI Backend

Serves the dashboard and provides REST API endpoints over the flights SQLite DB.

Usage:
    uvicorn app:app --reload
"""

import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

DB_FILE = "flights.db"

app = FastAPI(title="Mother's Ruin Trip Planner", version="1.0.0")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def _layover_minutes(dep_iso: str, arr_iso: str) -> int:
    """Compute minutes between two ISO 8601 datetimes."""
    try:
        d = datetime.fromisoformat(dep_iso)
        a = datetime.fromisoformat(arr_iso)
        return int((d - a).total_seconds() / 60)
    except Exception:
        return 0


def _parse_city_min(city_min: list[str]) -> dict[str, int]:
    """Parse ['CHS:60', 'NYC:90'] into {'CHS': 60, 'NYC': 90}."""
    result = {}
    for s in city_min:
        if ":" in s:
            city, mins = s.split(":", 1)
            try:
                result[city.upper()] = int(mins)
            except ValueError:
                pass
    return result


def _layover_cities(route: str) -> tuple[str, str]:
    """Extract the two layover cities from 'A → B → C → D'."""
    parts = [p.strip() for p in route.split("→")]
    return (parts[1] if len(parts) > 1 else "", parts[2] if len(parts) > 2 else "")


def _leg_price_int(val) -> int:
    """Coerce leg price to int whether it's '$129' or 129."""
    if isinstance(val, int):
        return val
    try:
        return int(str(val).replace("$", "").replace(",", "").strip())
    except (ValueError, AttributeError):
        return 0


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
    return column in cols


# ---------------------------------------------------------------------------
# Pipeline integration
# ---------------------------------------------------------------------------

_pipeline_status: dict = {"running": False, "last_run": None, "result": None, "error": None}
_pipeline_lock = threading.Lock()


def _run_pipeline_bg():
    from pipeline import run_pipeline
    with _pipeline_lock:
        _pipeline_status["running"] = True
        _pipeline_status["error"] = None
    try:
        result = run_pipeline()
        with _pipeline_lock:
            _pipeline_status["result"] = result
            _pipeline_status["last_run"] = datetime.now().isoformat()
    except Exception as e:
        with _pipeline_lock:
            _pipeline_status["error"] = str(e)
    finally:
        with _pipeline_lock:
            _pipeline_status["running"] = False


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.get("/api/stats")
def get_stats():
    with get_db() as conn:
        row = conn.execute("""
            SELECT
                COUNT(*)                    AS total_itineraries,
                MIN(total_price)            AS min_price,
                MAX(total_price)            AS max_price,
                CAST(AVG(total_price) AS INTEGER) AS avg_price,
                COUNT(DISTINCT route)       AS total_routes
            FROM itineraries
        """).fetchone()
        airlines = conn.execute("""
            SELECT DISTINCT airline FROM (
                SELECT leg1_airline AS airline FROM itineraries
                UNION SELECT leg2_airline FROM itineraries
                UNION SELECT leg3_airline FROM itineraries
            ) ORDER BY airline
        """).fetchall()

        return {
            **dict(row),
            "airlines": [r[0] for r in airlines],
            "flight_date": "2026-05-10",
        }


@app.get("/api/routes")
def get_routes():
    with get_db() as conn:
        rows = conn.execute("""
            SELECT
                route,
                COUNT(*)                         AS count,
                MIN(total_price)                 AS min_price,
                CAST(AVG(total_price) AS INTEGER) AS avg_price,
                MAX(total_price)                 AS max_price
            FROM itineraries
            GROUP BY route
            ORDER BY min_price
        """).fetchall()
        return [dict(r) for r in rows]


@app.get("/api/airlines")
def get_airlines():
    with get_db() as conn:
        rows = conn.execute("""
            SELECT DISTINCT airline FROM (
                SELECT leg1_airline AS airline FROM itineraries
                UNION SELECT leg2_airline FROM itineraries
                UNION SELECT leg3_airline FROM itineraries
            ) ORDER BY airline
        """).fetchall()
        return [r[0] for r in rows]


@app.get("/api/itineraries")
def get_itineraries(
    min_price: int = Query(default=0, ge=0),
    max_price: int = Query(default=9999, ge=0),
    start_city: Optional[str] = Query(default=None, max_length=10),
    end_city: Optional[str] = Query(default=None, max_length=10),
    must_include: list[str] = Query(default=[]),
    airlines: list[str] = Query(default=[]),
    city_min: list[str] = Query(default=[]),
    departs_after: int = Query(default=0, ge=0, le=23),
    sort: str = Query(default="price", pattern="^(price|duration|route)$"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    conditions = ["total_price BETWEEN ? AND ?"]
    params: list = [min_price, max_price]

    if start_city:
        conditions.append("route LIKE ?")
        params.append(f"{start_city.upper()} \u2192%")

    if end_city:
        conditions.append("route LIKE ?")
        params.append(f"% \u2192 {end_city.upper()}")

    for city in must_include:
        c = city.upper()
        conditions.append("(route LIKE ? OR route LIKE ? OR route LIKE ?)")
        params += [f"{c} \u2192%", f"% \u2192 {c}", f"% \u2192 {c} \u2192%"]

    if departs_after > 0:
        # Extract hour from ISO string: "2026-05-10T09:00:00-05:00" → char 12..13 = "09"
        conditions.append("CAST(SUBSTR(leg1_departure, 12, 2) AS INTEGER) >= ?")
        params.append(departs_after)

    if airlines:
        ph = ",".join("?" for _ in airlines)
        conditions.append(
            f"leg1_airline IN ({ph}) AND leg2_airline IN ({ph}) AND leg3_airline IN ({ph})"
        )
        params += airlines * 3

    sort_col = {"price": "total_price", "duration": "total_flight_minutes", "route": "route"}.get(
        sort, "total_price"
    )
    where = " AND ".join(conditions)

    city_mins = _parse_city_min(city_min)
    needs_layover_filter = any(v > 0 for v in city_mins.values())

    with get_db() as conn:
        has_layover_cols = _has_column(conn, "itineraries", "layover1_minutes")

        if needs_layover_filter:
            # Per-city thresholds require Python-side filtering
            all_rows = conn.execute(
                f"SELECT * FROM itineraries WHERE {where} ORDER BY {sort_col}",
                params,
            ).fetchall()

            filtered = []
            for row in all_rows:
                r = dict(row)
                if has_layover_cols:
                    l1 = r["layover1_minutes"]
                    l2 = r["layover2_minutes"]
                else:
                    l1 = _layover_minutes(r["leg2_departure"], r["leg1_arrival"])
                    l2 = _layover_minutes(r["leg3_departure"], r["leg2_arrival"])
                    r["layover1_minutes"] = l1
                    r["layover2_minutes"] = l2

                city1, city2 = _layover_cities(r["route"])
                if l1 >= city_mins.get(city1, 0) and l2 >= city_mins.get(city2, 0):
                    filtered.append(r)

            total = len(filtered)
            page = filtered[offset: offset + limit]
        else:
            total = conn.execute(
                f"SELECT COUNT(*) FROM itineraries WHERE {where}", params
            ).fetchone()[0]

            raw = conn.execute(
                f"SELECT * FROM itineraries WHERE {where} ORDER BY {sort_col} LIMIT ? OFFSET ?",
                params + [limit, offset],
            ).fetchall()
            page = []
            for row in raw:
                r = dict(row)
                if not has_layover_cols:
                    r["layover1_minutes"] = _layover_minutes(r["leg2_departure"], r["leg1_arrival"])
                    r["layover2_minutes"] = _layover_minutes(r["leg3_departure"], r["leg2_arrival"])
                page.append(r)

        result = []
        for r in page:
            result.append({
                "id": r["id"],
                "route": r["route"],
                "total_price": r["total_price"],
                "total_flight_time": r["total_flight_time"],
                "total_flight_minutes": r["total_flight_minutes"],
                "layover1_minutes": r.get("layover1_minutes", 0),
                "layover2_minutes": r.get("layover2_minutes", 0),
                "legs": [
                    {
                        "flight": r[f"leg{i}_flight"],
                        "airline": r[f"leg{i}_airline"],
                        "departure": r[f"leg{i}_departure"],
                        "arrival": r[f"leg{i}_arrival"],
                        "price": _leg_price_int(r[f"leg{i}_price"]),
                        "duration": r[f"leg{i}_duration"],
                    }
                    for i in range(1, 4)
                ],
            })

        return {"total": total, "offset": offset, "limit": limit, "itineraries": result}


@app.post("/api/pipeline/run")
def trigger_pipeline(background_tasks: BackgroundTasks):
    if _pipeline_status["running"]:
        return JSONResponse({"error": "Pipeline already running"}, status_code=409)
    background_tasks.add_task(_run_pipeline_bg)
    return {"status": "started"}


@app.get("/api/pipeline/status")
def pipeline_status():
    return dict(_pipeline_status)


# ---------------------------------------------------------------------------
# Static files — must be mounted LAST so API routes take priority
# ---------------------------------------------------------------------------

if os.path.isdir("static"):
    app.mount("/", StaticFiles(directory="static", html=True), name="static")
