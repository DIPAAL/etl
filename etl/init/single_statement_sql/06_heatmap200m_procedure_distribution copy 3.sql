SELECT create_distributed_function(
    'create_heatmap_200m(INTEGER,INTEGER,INTEGER,SMALLINT)',
    'part_id', colocate_with := 'fact_cell_200m'
);