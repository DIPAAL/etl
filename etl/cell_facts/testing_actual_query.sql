SELECT
    (
      SELECT key FROM json_each_text(start_edges)
      ORDER BY value::float ASC LIMIT 1
    ) as entry_direction,
    (
      SELECT key FROM json_each_text(end_edges)
      ORDER BY value::float ASC LIMIT 1
    ) as exit_direction,
FROM
(
SELECT
    JSON_BUILD_OBJECT(
        'south', ST_Distance(startValue(crossing), south),
        'north', ST_Distance(startValue(crossing), north),
        'east', ST_Distance(startValue(crossing), east),
        'west', ST_Distance(startValue(crossing), west)
        ) AS start_edges,
    JSON_BUILD_OBJECT(
        'south', ST_Distance(endValue(crossing), south),
        'north', ST_Distance(endValue(crossing), north),
        'east', ST_Distance(endValue(crossing), east),
        'west', ST_Distance(endValue(crossing), west)
        ) AS end_edges,
    crossing
FROM
(SELECT
    unnest(sequences(atGeometry(t.trajectory, dc.geom))) crossing,
    ST_SetSRID(ST_MakeLine(
        ST_MakePoint(ST_XMin(dc.geom), ST_YMin(dc.geom)),
        ST_MakePoint(ST_XMax(dc.geom), ST_YMin(dc.geom))
    ), 3034) south,
    ST_SetSRID(ST_MakeLine(
        ST_MakePoint(ST_XMin(dc.geom), ST_YMin(dc.geom)),
        ST_MakePoint(ST_XMin(dc.geom), ST_YMax(dc.geom))
    ), 3034) west,
    ST_SetSRID(ST_MakeLine(
        ST_MakePoint(ST_XMax(dc.geom), ST_YMax(dc.geom)),
        ST_MakePoint(ST_XMax(dc.geom), ST_YMin(dc.geom))
    ), 3034) east,
    ST_SetSRID(ST_MakeLine(
        ST_MakePoint(ST_XMax(dc.geom), ST_YMax(dc.geom)),
        ST_MakePoint(ST_XMin(dc.geom), ST_YMax(dc.geom))
    ), 3034) north
FROM grid dc
JOIN trajectory t ON ST_Intersects(dc.geom, t.trajectory::geometry)
) TR1
) TR2