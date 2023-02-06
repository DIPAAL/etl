INSERT INTO fact_cell (
    cell_x, cell_y, ship_id,
    entry_date_id, entry_time_id,
    exit_date_id, exit_time_id,
    direction_id, nav_status_id, trajectory_sub_id,
    sog, delta_heading, draught, delta_cog, st_bounding_box
)
SELECT
    cell_x,
    cell_y,
    ship_id,
    (EXTRACT(YEAR FROM startTime) * 10000) + (EXTRACT(MONTH FROM startTime) * 100) + (EXTRACT(DAY FROM startTime)) AS entry_date_id,
    (EXTRACT(HOUR FROM startTime) * 10000) + (EXTRACT(MINUTE FROM startTime) * 100) + (EXTRACT(SECOND FROM startTime)) AS entry_time_id,
    (EXTRACT(YEAR FROM endTime) * 10000) + (EXTRACT(MONTH FROM endTime) * 100) + (EXTRACT(DAY FROM endTime)) AS exit_date_id,
    (EXTRACT(HOUR FROM endTime) * 10000) + (EXTRACT(MINUTE FROM endTime) * 100) + (EXTRACT(SECOND FROM endTime)) AS exit_time_id,
    (SELECT direction_id FROM dim_direction dd WHERE dd.from = entry_direction AND dd.to = exit_direction) AS direction_id,
    nav_status_id,
    trajectory_sub_id,
    length(crossing) / GREATEST (durationSeconds, 1) * 1.94 sog, -- 1 m/s = 1.94 knots. Min 1 second to avoid division by zero
    -- if delta_heading is null, then set as -1, else use calculate_delta
    -- upper_bound=360, as heading goes from 0 -> 359
    CASE WHEN heading IS NULL THEN
        -1
    ELSE
        calculate_delta_upperbounded ((
            SELECT
                ARRAY_AGG(LOWER(head))
            FROM UNNEST(GETVALUES (heading)) AS head), 360)
    END delta_heading,
    draught,
    delta_cog,
    stbox (cell_geom, crossing_period) st_bounding_box
FROM (
    SELECT
        get_lowest_json_key (start_edges) entry_direction,
        get_lowest_json_key (end_edges) exit_direction,
        crossing,
        cell_x,
        cell_y,
        cell_geom,
        ship_id,
        nav_status_id,
        trajectory_sub_id,
        startValue (atPeriod (draught, crossing_period)) draught,
        atPeriod (heading, crossing_period) heading,
        startTime,
        endTime,
        delta_cog,
        crossing_period,
        (EXTRACT(EPOCH FROM (endTime - startTime))) durationSeconds
    FROM (
        SELECT
            *,
            period (cid.startTime, cid.endTime, TRUE, TRUE) crossing_period
        FROM (
            SELECT
                -- Construct the JSON objects existing of direction key, and distance to the cell edge
                JSON_BUILD_OBJECT(
                    'South', ST_Distance (startValue (crossing), south),
                    'North', ST_Distance (startValue (crossing), north),
                    'East', ST_Distance (startValue (crossing), east),
                    'West', ST_Distance (startValue (crossing), west),
                    'Unknown', threshold_distance_to_cell_edge
                    ) AS start_edges,
                JSON_BUILD_OBJECT(
                    'South', ST_Distance (endValue (crossing), south),
                    'North', ST_Distance (endValue (crossing), north),
                    'East', ST_Distance (endValue (crossing), east),
                    'West', ST_Distance (endValue (crossing), west),
                    'Unknown', threshold_distance_to_cell_edge
                    ) AS end_edges,
                crossing,
                cell_x,
                cell_y,
                cell_geom,
                ship_id,
                nav_status_id,
                trajectory_sub_id,
                draught,
                heading,
                ( -- Calculate the Delta COG
                    calculate_delta_upperbounded ((
                        SELECT
                            ARRAY_AGG(LOWER(delta))
                        FROM UNNEST(GETVALUES (DEGREES(AZIMUTH (crossing)))) AS delta), 360)) AS delta_cog,
                -- Truncate the entry and exit timestamp to second.
                date_trunc('second', startTimestamp (crossing)) startTime,
                date_trunc('second', endTimestamp (crossing)) endTime
            FROM (
                SELECT
                    unnest(sequences (atGeometry (fdt.trajectory, dc.geom))) crossing,
                    -- Create the 4 lines representing the cell edges
                    ST_SetSRID (
                        ST_MakeLine (
                            ST_MakePoint (ST_XMin (dc.geom), ST_YMin (dc.geom)),
                            ST_MakePoint (ST_XMax (dc.geom), ST_YMin (dc.geom))
                            ), 3034) south,
                    ST_SetSRID (
                        ST_MakeLine (
                            ST_MakePoint (ST_XMin (dc.geom), ST_YMin (dc.geom)),
                            ST_MakePoint (ST_XMin (dc.geom), ST_YMax (dc.geom))
                            ), 3034) west,
                    ST_SetSRID (
                        ST_MakeLine (
                            ST_MakePoint (ST_XMax (dc.geom), ST_YMax (dc.geom)),
                            ST_MakePoint (ST_XMax (dc.geom), ST_YMin (dc.geom))
                            ), 3034) east,
                    ST_SetSRID (
                        ST_MakeLine (
                            ST_MakePoint (ST_XMax (dc.geom), ST_YMax (dc.geom)),
                            ST_MakePoint (ST_XMin (dc.geom), ST_YMax (dc.geom))
                            ), 3034) north,
                    0.2 threshold_distance_to_cell_edge,
                    dc.x cell_x,
                    dc.y cell_y,
                    dc.geom cell_geom,
                    fdt.ship_id ship_id,
                    fdt.nav_status_id nav_status_id,
                    fdt.trajectory_sub_id trajectory_sub_id,
                    fdt.draught draught,
                    fdt.heading heading
                FROM (
                    SELECT
                        ft.*,
                        -- Split the trajectory into cells of 5000m x 5000m. This makes it much faster to join to cell dimension.
                        (spaceSplit (transform (setSRID (dt.trajectory, 4326), 3034), 5000)).tpoint point,
                        transform (dt.trajectory, 3034) trajectory,
                        dt.heading heading,
                        dt.draught draught
                    FROM
                        fact_trajectory ft
                        JOIN dim_trajectory dt ON ft.trajectory_sub_id = dt.trajectory_sub_id
                            AND ft.start_date_id = dt.date_id
                    WHERE
                        duration > INTERVAL '1 second'
                        AND ft.start_date_id = %s) fdt
                    JOIN dim_cell_50m dc ON ST_Intersects (dc.geom, fdt.point::geometry)
                ) ci
            ) cid
        ) ct
    ) cif
ON CONFLICT DO NOTHING;
