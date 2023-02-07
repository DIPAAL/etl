CREATE TABLE dim_nav_status (
    nav_status_id smallserial PRIMARY KEY,
    nav_status text NOT NULL UNIQUE
    -- Padding: None. Variable length column is last, no padding is needed.
);

-- Insert Unknown status
INSERT INTO dim_nav_status (nav_status_id, nav_status)
VALUES (0, 'Unknown');
