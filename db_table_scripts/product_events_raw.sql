CREATE TABLE staging.product_events_raw (
    event_id TEXT,
    event_timestamp TIMESTAMP,
    event_type TEXT,
    user_id INT,
    account_id INT,
    product_id INT,
    category TEXT,
    experiment_id TEXT,
    event_date DATE,
    hour TEXT,
    PRIMARY KEY (event_id)
);
