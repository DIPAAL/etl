EXPLAIN (ANALYZE)
SELECT dt.*
FROM dim_time dt
WHERE dt.time_id BETWEEN :start_time_id AND :end_time_id