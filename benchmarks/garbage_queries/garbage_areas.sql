-- List of ~50 POIs in Danish Waters represented as PostGis literals
-- The list should have a place name and an ST_MakeEnvelope
SELECT
    'Rømø Waters' AS name,
    ST_MakeEnvelope(8.392181, 55.017394, 8.717651, 55.245467, 4326) AS box
UNION ALL
SELECT
    'Esbjerg Large' AS name,
    ST_MakeEnvelope(9.661111,57.048889, 9.991667, 57.166667, 4326) AS box

