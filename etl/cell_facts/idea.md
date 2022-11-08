## Idea of creating rollups
Some of the queries are pseudo code ish.

### 1. Find the cells that touches the trajectory

```postgresql
SELECT 
    ft.*,
    setSRID(dt.trajectory,4326)::geometry geom 
FROM fact_trajectory ft
JOIN dim_trajectory dt ON ft.trajectory_id = dt.trajectory_id 
JOIN dim_cell_50m dc ON
 ST_Intersects(transform(dt.trajectory, 3034)::geometry, dc.cell_geom)
```

### 2. Find the intersection of the cell and the trajectory
Find the partial trajectory that is inside the cell.
Do it with mobilitydb and atGeometry
```postgresql
atGeometry(dt.trajectory, ST_Transform(dc.geom, 4326)) intersection
```

### 3. Find the sog, cog, direction, entry and exit time.
First find the entry and exit time of the trajectory in the cell.
```postgresql
startTimestamp(intersection) entry_time,
endTimestamp(intersection) exit_time
```

Then calculate the distance sailed in the cell.
```postgresql
ST_Length(transform(intersection, 3034)::geometry) distance
```

Then calculate the average sog
```postgresql
distance / (extract(epoch from exit_time - entry_time) / 3600) sog
```

Create line for each 4 sides of the cell.
```postgresql
ST_MakeLine(
    ST_MakePoint(ST_MinX(dc.geom), ST_MinY(dc.geom)),
    ST_MakePoint(ST_MaxX(dc.geom), ST_MinY(dc.geom))
) south,
ST_MakeLine(
    ST_MakePoint(ST_MinX(dc.geom), ST_MinY(dc.geom)),
    ST_MakePoint(ST_MinX(dc.geom), ST_MaxY(dc.geom))
) west,
ST_MakeLine(
    ST_MakePoint(ST_MaxX(dc.geom), ST_MaxY(dc.geom)),
    ST_MakePoint(ST_MaxX(dc.geom), ST_MinY(dc.geom))
) east,
ST_MakeLine(
    ST_MakePoint(ST_MaxX(dc.geom), ST_MaxY(dc.geom)),
    ST_MakePoint(ST_MinX(dc.geom), ST_MaxY(dc.geom))
) north
```

SELECT 
    ST_MakeLine(
        ST_MakePoint(ST_MinX(dc.geom), ST_MinY(dc.geom)),
        ST_MakePoint(ST_MaxX(dc.geom), ST_MinY(dc.geom))
    ) south,
    ST_MakeLine(
        ST_MakePoint(ST_MinX(dc.geom), ST_MinY(dc.geom)),
        ST_MakePoint(ST_MinX(dc.geom), ST_MaxY(dc.geom))
    ) west,
    ST_MakeLine(
        ST_MakePoint(ST_MaxX(dc.geom), ST_MaxY(dc.geom)),
        ST_MakePoint(ST_MaxX(dc.geom), ST_MinY(dc.geom))
    ) east,
    ST_MakeLine(
        ST_MakePoint(ST_MaxX(dc.geom), ST_MaxY(dc.geom)),
        ST_MakePoint(ST_MinX(dc.geom), ST_MaxY(dc.geom))
    ) north
    atGeometry(setsrid(dt.trajectory,4326), ST_Transform(dc.geom, 4326)) intersection
FROM fact_trajectory ft
JOIN dim_trajectory dt ON ft.trajectory_id = dt.trajectory_id 
JOIN dim_cell_50m dc ON
 ST_Intersects(transform(setsrid(dt.trajectory,4326), 3034)::geometry, dc.cell_geom)

