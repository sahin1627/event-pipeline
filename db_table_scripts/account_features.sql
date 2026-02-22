CREATE TABLE refined.account_features (
    account_id INT PRIMARY KEY,
    active_users_last_7d INT,
    account_ctr_last_24h FLOAT,
    most_viewed_category_7d TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
