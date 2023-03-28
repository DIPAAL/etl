CREATE OR REPLACE FUNCTION dipaal_rollback(dateid INTEGER[]) RETURNS void AS $$
DECLARE
    partition_name TEXT;
BEGIN
    DELETE FROM fact_trajectory WHERE start_date_id = ANY(dateid);
    DELETE FROM dim_trajectory WHERE date_id = ANY(dateid);

    DELETE FROM fact_cell_5000m WHERE entry_date_id = ANY(dateid);
    DELETE FROM fact_cell_1000m WHERE entry_date_id = ANY(dateid);
    DELETE FROM fact_cell_200m WHERE entry_date_id = ANY(dateid);
    DELETE FROM fact_cell_50m WHERE entry_date_id = ANY(dateid);

    partition_name := 'fact_cell_heatmap_' || substring(dateid[1]::text FROM 1 FOR 4) || '_' || substring(dateid[1]::text FROM 5 FOR 2);
    RAISE NOTICE 'Start converting % to row-major storage scheme', partition_name;
    PERFORM alter_table_set_access_method(partition_name, 'heap');
    RAISE NOTICE 'Deleting data from %', partition_name;
    DELETE FROM fact_cell_heatmap WHERE date_id = ANY(dateid);
    PERFORM alter_table_set_access_method(partition_name, 'columnar');
    RAISE NOTICE 'Start converting % to columnar storage scheme', partition_name;

    RAISE NOTICE 'Finished cleanup for %', dateid;
END;
$$ LANGUAGE plpgsql;