SELECT STBOX(
    (SELECT geom FROM enc_cells ORDER BY random() LIMIT 1),
    -- create a random period between 2022-01-01 and 2022-03-31
    SELECT period(t.start_time, t.start_time+interval '10 days') FROM (
        SELECT '2022-01-01T00:00:00Z'::timestamptz + random() * ('2022-03-31T00:00:00Z'::timestamptz - '2022-01-01T00:00:00Z'::timestamptz) AS start_time
    ) AS t
) AS box;