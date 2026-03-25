WITH RECURSIVE itinerary AS (
    SELECT
        f.origin_city || ' → ' || f.dest_city AS route,
        CAST(f.id AS TEXT) AS flight_ids,
        ',' || f.origin_city || ',' || f.dest_city || ',' AS visited,
        f.dest_city AS last_city,
        f.arrival_tz AS last_arrival,
        f.price AS total_price,
        f.duration_minutes AS total_minutes,
        1 AS legs,
        f.departure_tz AS leg1_dep_tz,
        f.arrival_tz AS leg1_arr_tz,
        f.origin_airport || '→' || f.dest_airport AS leg1_flight,
        f.airline AS leg1_airline,
        f.price AS leg1_price,
        '' AS leg2_dep_tz, '' AS leg2_arr_tz, '' AS leg2_flight, '' AS leg2_airline, 0 AS leg2_price,
        '' AS leg3_dep_tz, '' AS leg3_arr_tz, '' AS leg3_flight, '' AS leg3_airline, 0 AS leg3_price
    FROM flights f

    UNION ALL

    SELECT
        it.route || ' → ' || f.dest_city,
        it.flight_ids || ' → ' || CAST(f.id AS TEXT),
        it.visited || f.dest_city || ',',
        f.dest_city,
        f.arrival_tz,
        it.total_price + f.price,
        it.total_minutes + f.duration_minutes,
        it.legs + 1,
        it.leg1_dep_tz, it.leg1_arr_tz, it.leg1_flight, it.leg1_airline, it.leg1_price,
        CASE WHEN it.legs = 1 THEN f.departure_tz ELSE it.leg2_dep_tz END,
        CASE WHEN it.legs = 1 THEN f.arrival_tz ELSE it.leg2_arr_tz END,
        CASE WHEN it.legs = 1 THEN f.origin_airport || '→' || f.dest_airport ELSE it.leg2_flight END,
        CASE WHEN it.legs = 1 THEN f.airline ELSE it.leg2_airline END,
        CASE WHEN it.legs = 1 THEN f.price ELSE it.leg2_price END,
        CASE WHEN it.legs = 2 THEN f.departure_tz ELSE it.leg3_dep_tz END,
        CASE WHEN it.legs = 2 THEN f.arrival_tz ELSE it.leg3_arr_tz END,
        CASE WHEN it.legs = 2 THEN f.origin_airport || '→' || f.dest_airport ELSE it.leg3_flight END,
        CASE WHEN it.legs = 2 THEN f.airline ELSE it.leg3_airline END,
        CASE WHEN it.legs = 2 THEN f.price ELSE it.leg3_price END
    FROM itinerary it
    JOIN flights f ON f.origin_city = it.last_city
    WHERE it.legs < 3
      AND INSTR(it.visited, ',' || f.dest_city || ',') = 0
      AND f.departure_tz > it.last_arrival
)
SELECT route, flight_ids, total_price,
       leg1_airline || ' → ' || leg2_airline || ' → ' || leg3_airline AS airlines,
       (leg1_airline != leg2_airline) + (leg2_airline != leg3_airline) + (leg1_airline != leg3_airline) AS distinct_pairs,
       leg1_flight, TIME(leg1_dep_tz) AS leg1_dep, TIME(leg1_arr_tz) AS leg1_arr,
       (strftime('%s', leg2_dep_tz) - strftime('%s', leg1_arr_tz)) / 60 AS layover1_minutes,
       leg2_flight, TIME(leg2_dep_tz) AS leg2_dep, TIME(leg2_arr_tz) AS leg2_arr,
       (strftime('%s', leg3_dep_tz) - strftime('%s', leg2_arr_tz)) / 60 AS layover2_minutes,
       leg3_flight, TIME(leg3_dep_tz) AS leg3_dep, TIME(leg3_arr_tz) AS leg3_arr
FROM itinerary
WHERE legs = 3 -- hits 4 locations
  AND INSTR(visited, ',CHS,') > 0 -- hits charleston
  AND TIME(leg1_dep_tz) >= '09:00:00' -- departs after 9am local
  AND layover1_minutes > 90
  AND layover2_minutes > 90
  AND (route LIKE 'BNA → %' OR route LIKE '% → BNA')
ORDER BY total_price;
