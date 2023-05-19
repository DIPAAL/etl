-- DELETE cell facts 50m WHERE it doesnt intersect with any ENC geom with country = 'Denmark' and type = 'Harbor'
DELETE FROM fact_cell_1000m fc
WHERE NOT EXISTS (
    SELECT 1
    FROM enc
    WHERE enc.country = 'Denmark'
    AND enc.type = 'Harbor'
    AND STBOX(ST_Transform(enc.geom,3034)) && fc.st_bounding_box
) AND entry_date_id = :date_id;