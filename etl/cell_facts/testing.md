
Create a dummy grid table

Use SRID 3034
```sql
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS mobilitydb;

CREATE TABLE grid (
    id serial PRIMARY KEY,
    geom geometry(Polygon, 3034)
);

INSERT INTO grid (geom)
SELECT ST_MakeEnvelope(
    100, -- min x
    100, -- min y
    200, -- max x
    200, -- max y
    3034 -- srid
) geom;

--Create a dummy trajectory table

CREATE TABLE trajectory (
    id serial PRIMARY KEY,
    trajectory tgeompoint NOT NULL
);

INSERT INTO trajectory (trajectory) 
VALUES (
    tgeompoint 'SRID=3034;[POINT(0 1)@2021-10-09 09:16:00+00,POINT(202 200)@2021-10-09 09:17:00+00, POINT(0 310)@2021-10-09 09:18:00+00,POINT(290 0)@2021-10-09 09:19:00+00]'
);
```