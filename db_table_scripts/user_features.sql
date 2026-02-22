CREATE TABLE refined.user_features (
    user_id INT PRIMARY KEY,
    searches_last_24h INT,
    clicks_last_24h INT,
    ctr_last_24h FLOAT,
    last_activity_timestamp TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
