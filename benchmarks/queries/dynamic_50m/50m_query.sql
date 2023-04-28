-- Benchmarking query for evaluating the performance of lazy loading 50m cells.
-- This benchmark query is based on the "fact_cell_rollup.sql" query.
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
    infer_stopped,
    trajectory_sub_id,
    length(crossing) / GREATEST (durationSeconds, 1) * 1.94 sog,
    CASE WHEN heading IS NULL THEN
        -1
    ELSE
        calculate_delta_upperbounded ((
            SELECT
                ARRAY_AGG(head)
            FROM UNNEST(GETVALUES (heading)) AS head), 360)
    END delta_heading,
    draught,
    delta_cog,
    stbox (cell_geom, crossing_period) st_bounding_box,
    partition_id
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
        infer_stopped,
        trajectory_sub_id,
        minValue(atTime (draught, crossing_period)) draught,
        atTime (heading, crossing_period) heading,
        startTime,
        endTime,
        delta_cog,
        crossing_period,
        (EXTRACT(EPOCH FROM (endTime - startTime))) durationSeconds,
        partition_id
    FROM (
        SELECT
            *,
            span (cid.startTime, cid.endTime, TRUE, TRUE) crossing_period
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
                infer_stopped,
                trajectory_sub_id,
                draught,
                heading,
                (
                    calculate_delta_upperbounded (
                        (
                            SELECT ARRAY_AGG(delta)
                            FROM UNNEST(GETVALUES(DEGREES(AZIMUTH(crossing)))) AS delta
                        ),
                        360)
                ) AS delta_cog,
                -- Truncate the entry and exit timestamp to second.
                date_trunc('second', startTimestamp (crossing)) startTime,
                date_trunc('second', endTimestamp (crossing)) endTime,
                partition_id
            FROM (
                SELECT
                    unnest(sequences (atGeometry (
                        -- BENCHMARK: Temporal restriction on the trajectory
                        atstbox(
                            fdt.trajectory,
                            stbox(
                                    span(
                                            timestamp_from_date_time_id(:start_date, :start_time),
                                            timestamp_from_date_time_id(:end_date, :end_time), TRUE, TRUE
                                        )
                                )
                            ),
                        dc.geom))) crossing,
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
                    fdt.infer_stopped infer_stopped,
                    fdt.trajectory_sub_id trajectory_sub_id,
                    fdt.draught draught,
                    fdt.heading heading,
                    fdt.partition_id
                FROM staging.split_trajectories fdt
                JOIN staging.cell_{CELL_SIZE}m dc ON (ST_Crosses(dc.geom, fdt.trajectory::geometry) OR ST_Contains(dc.geom, fdt.trajectory::geometry))
                    -- BENCHMARK: Spatial and temporal bounds
                    AND ST_Intersects(ST_Makeenvelope(:xmin, :ymin, :xmax, :ymax, 3034), dc.geom)
                    AND STBOX(SPAN(
                        timestamp_from_date_time_id(:start_date,:start_time),
                        timestamp_from_date_time_id(:end_date,:end_time), TRUE, TRUE
                        )) && fdt.trajectory
            ) cj
        ) cid
    ) cif
) ir;