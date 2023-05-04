CREATE TABLE enc (
    title text,
    country text,
    scale int,
    cat int,
    type text,
    edition_date int,
    edition int,
    update_date int,
    update int,
    source text,
    slug text,
    geom geometry
);

CREATE INDEX enc_geom_idx ON enc USING gist (geom);

CREATE INDEX enc_title_idx ON enc (title);

CREATE INDEX enc_country_idx ON enc (country);

CREATE INDEX slug ON enc (scale);

CREATE INDEX enc_cat_idx ON enc (cat);