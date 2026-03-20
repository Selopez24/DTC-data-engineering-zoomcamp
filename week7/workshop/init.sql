CREATE TABLE IF NOT EXISTS pickup_location_counts (
    window_start TIMESTAMP,
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);

CREATE TABLE IF NOT EXISTS session_window_counts (
    session_id VARCHAR(100),
    PULocationID INT,
    trip_count BIGINT,
    PRIMARY KEY (session_id, PULocationID)
);

CREATE TABLE IF NOT EXISTS hourly_tips (
    window_start TIMESTAMP,
    total_tip_amount DECIMAL(10, 2),
    PRIMARY KEY (window_start)
);
