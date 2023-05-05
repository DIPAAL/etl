EXPLAIN (ANALYZE)
SELECT dd.*
FROM dim_date dd
WHERE dd.date_id BETWEEN :start_date_id AND :end_date_id