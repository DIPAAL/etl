INSERT INTO fact_cell (
    cell_x, cell_y, ship_id, ship_junk_id,
    entry_date_id, entry_time_id,
    exit_date_id, exit_time_id,
    direction_id, nav_status_id, trajectory_sub_id,
    sog, delta_heading, draught, delta_cog, st_bounding_box
)
SELECT
    cell_x,
    cell_y,
    ship_id,
    ship_junk_id,
    (EXTRACT(YEAR FROM startTime) * 10000) + (EXTRACT(MONTH FROM startTime) * 100) + (EXTRACT(DAY FROM startTime)) AS entry_date_id,
    (EXTRACT(HOUR FROM startTime) * 10000) + (EXTRACT(MINUTE FROM startTime) * 100) + (EXTRACT(SECOND FROM startTime)) AS entry_time_id,
    (EXTRACT(YEAR FROM endTime) * 10000) + (EXTRACT(MONTH FROM endTime) * 100) + (EXTRACT(DAY FROM endTime)) AS exit_date_id,
    (EXTRACT(HOUR FROM endTime) * 10000) + (EXTRACT(MINUTE FROM endTime) * 100) + (EXTRACT(SECOND FROM endTime)) AS exit_time_id,
    (SELECT direction_id FROM dim_direction dd WHERE dd.from = entry_direction AND dd.to = exit_direction) AS direction_id,
    nav_status_id,
    trajectory_sub_id,
    length(crossing) / GREATEST(durationSeconds, 1) * 1.94 sog, -- 1 m/s = 1.94 knots. Min 1 second to avoid division by zero
    -- if delta_heading is null, then set as -1, else use calculate_delta
    CASE WHEN heading IS NULL THEN -1 ELSE calculate_delta((SELECT ARRAY_AGG(LOWER(head)) FROM UNNEST(GETVALUES(heading)) as head)) END delta_heading,
    draught,
    delta_cog,
    stbox(cell_geom, period(startTime, endTime)) st_bounding_box
FROM (
        SELECT
            get_lowest_json_key(start_edges) entry_direction,
            get_lowest_json_key(end_edges) exit_direction,
            crossing,
            cell_x,
            cell_y,
            cell_geom,
            ship_id,
            ship_junk_id,
            nav_status_id,
            trajectory_sub_id,
            startValue(atPeriod(draught, period(startTime, endTime, true, true))) draught,
            atPeriod(heading, period(startTime, endTime, true, true)) heading,
            startTime,
            endTime,
            delta_cog,
            (EXTRACT(EPOCH FROM (endTime - startTime))) durationSeconds
        FROM (
            SELECT
                -- Construct the JSON objects existing of direction key, and distance to the cell edge
                JSON_BUILD_OBJECT(
                    'South', ST_Distance(startValue(crossing), south),
                    'North', ST_Distance(startValue(crossing), north),
                    'East', ST_Distance(startValue(crossing), east),
                    'West', ST_Distance(startValue(crossing), west),
                    'Unknown', threshold_distance_to_cell_edge
                    ) AS start_edges,
                JSON_BUILD_OBJECT(
                    'South', ST_Distance(endValue(crossing), south),
                    'North', ST_Distance(endValue(crossing), north),
                    'East', ST_Distance(endValue(crossing), east),
                    'West', ST_Distance(endValue(crossing), west),
                    'Unknown', threshold_distance_to_cell_edge
                    ) AS end_edges,
                crossing,
                cell_x,
                cell_y,
                cell_geom,
                ship_id,
                ship_junk_id,
                nav_status_id,
                trajectory_sub_id,
                draught,
                heading,
                ( -- Calculate the Delta COG
                    SELECT SUM(ABS(LOWER(delta))) FROM UNNEST(GETVALUES(DEGREES(AZIMUTH(crossing)))) AS delta
                ) AS delta_cog,
                -- Truncate the entry and exit timestamp to second. Add almost a second to exit value, to be inclusive.
                date_trunc('second', startTimestamp(crossing)) startTime,
                date_trunc('second', endTimestamp(crossing) + INTERVAL '1000000 microseconds') endTime
            FROM (
                SELECT
                    unnest(sequences(atGeometry(fdt.trajectory, fdt.cell_geom))) crossing,
                    -- Create the 4 lines representing the cell edges
                    ST_SetSRID(ST_MakeLine(
                        ST_MakePoint(ST_XMin(fdt.cell_geom), ST_YMin(fdt.cell_geom)),
                        ST_MakePoint(ST_XMax(fdt.cell_geom), ST_YMin(fdt.cell_geom))
                    ), 3034) south,
                    ST_SetSRID(ST_MakeLine(
                        ST_MakePoint(ST_XMin(fdt.cell_geom), ST_YMin(fdt.cell_geom)),
                        ST_MakePoint(ST_XMin(fdt.cell_geom), ST_YMax(fdt.cell_geom))
                    ), 3034) west,
                    ST_SetSRID(ST_MakeLine(
                        ST_MakePoint(ST_XMax(fdt.cell_geom), ST_YMax(fdt.cell_geom)),
                        ST_MakePoint(ST_XMax(fdt.cell_geom), ST_YMin(fdt.cell_geom))
                    ), 3034) east,
                    ST_SetSRID(ST_MakeLine(
                        ST_MakePoint(ST_XMax(fdt.cell_geom), ST_YMax(fdt.cell_geom)),
                        ST_MakePoint(ST_XMin(fdt.cell_geom), ST_YMax(fdt.cell_geom))
                    ), 3034) north,
                    0.2 threshold_distance_to_cell_edge,
                    fdt.cell_x cell_x,
                    fdt.cell_y cell_y,
                    fdt.cell_geom cell_geom,
                    fdt.ship_id ship_id,
                    fdt.ship_junk_id ship_junk_id,
                    fdt.nav_status_id nav_status_id,
                    fdt.trajectory_sub_id trajectory_sub_id,
                    fdt.draught draught,
                    fdt.heading heading
                FROM (
                    SELECT
                        ST_X ((point).point) cell_x,
                        ST_Y ((point).point) cell_y,
                        ST_MakeEnvelope(ST_X((point).point), ST_Y((point).point), ST_X((point).point) + 50, ST_Y((point).point) + 50, 3034) cell_geom,
                        st.ship_id ship_id,
						st.trajectory trajectory,
                        st.ship_junk_id ship_junk_id,
                        st.nav_status_id nav_status_id,
                        st.trajectory_sub_id trajectory_sub_id,
                        st.draught draught,
                        st.heading heading
                    FROM
                        ( SELECT
                            ft.*,
                            spaceSplit(transform(setSRID(dt.trajectory,4326),3034), 50) point,
                            transform(setSRID(dt.trajectory, 4326), 3034) trajectory,
                            setSrid(dt.trajectory,4326)::geometry tg,
                            dt.heading heading,
                            dt.draught draught
                        FROM fact_trajectory ft
                        JOIN dim_trajectory dt ON ft.trajectory_sub_id = dt.trajectory_sub_id AND ft.start_date_id = dt.date_id
                        WHERE duration > INTERVAL '1 second' AND ft.start_date_id = %s
                    ) st
                ) fdt
            ) ci
        ) cid
    ) cif
ON CONFLICT DO NOTHING;
