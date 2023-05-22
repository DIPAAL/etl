CREATE OR REPLACE FUNCTION dipaal_rollback(dateid INTEGER[]) RETURNS void AS $$
DECLARE
    partition_name TEXT;
    month INTEGER;
BEGIN
    month := 0;
    FOR id IN array_lower(dateid, 1)..array_upper(dateid, 1)
    LOOP
        IF month = 0 THEN
            month := substring(dateid[id]::text FROM 5 FOR 2)::INTEGER;
        ELSIF month != substring(dateid[id]::text FROM 5 FOR 2)::INTEGER THEN
            RAISE EXCEPTION 'Dateids provided do not belong to the same month, which is not allowed for this function';
        END IF;
    END LOOP;

    DELETE FROM fact_trajectory WHERE start_date_id = ANY(dateid);
    DELETE FROM dim_trajectory WHERE date_id = ANY(dateid);

    DELETE FROM fact_cell_5000m WHERE entry_date_id = ANY(dateid);
    DELETE FROM fact_cell_1000m WHERE entry_date_id = ANY(dateid);
    DELETE FROM fact_cell_200m WHERE entry_date_id = ANY(dateid);
    DELETE FROM fact_cell_50m WHERE entry_date_id = ANY(dateid);
    DELETE FROM fact_cell_heatmap WHERE date_id = ANY(dateid);

    RAISE NOTICE 'Finished cleanup for %', dateid;
END;
$$ LANGUAGE plpgsql;