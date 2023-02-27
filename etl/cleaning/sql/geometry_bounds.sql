SELECT
    rg.geom_geodetic AS geom
FROM 
    public.reference_geometries rg
WHERE
    rg.type = 'cleaning_ref'
;