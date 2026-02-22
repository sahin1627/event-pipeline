WITH last_24h AS (
        SELECT *
        FROM staging.product_events_raw
        WHERE event_timestamp >= NOW() - INTERVAL '24 hours'
    ),
    last_activity AS (
        SELECT user_id, MAX(event_timestamp) AS last_activity_timestamp
        FROM staging.product_events_raw
        GROUP BY user_id
    ),
    searches AS (
        SELECT user_id, COUNT(*) AS searches_last_24h
        FROM last_24h
        WHERE event_type = 'search_performed'
        GROUP BY user_id
    ),
    clicks AS (
        SELECT user_id, COUNT(*) AS clicks_last_24h
        FROM last_24h
        WHERE event_type IN ('result_clicked', 'recommendation_clicked')
        GROUP BY user_id
    ), source_cte as (
        SELECT
            la.user_id,
            COALESCE(s.searches_last_24h, 0) AS searches_last_24h,
            COALESCE(c.clicks_last_24h, 0) AS clicks_last_24h,
            CASE WHEN COALESCE(s.searches_last_24h, 0) = 0 THEN 0
                 ELSE COALESCE(c.clicks_last_24h, 0)::float / s.searches_last_24h
            END AS ctr_last_24h,
            la.last_activity_timestamp
        FROM last_activity la
        LEFT JOIN searches s ON la.user_id = s.user_id
        LEFT JOIN clicks c ON la.user_id = c.user_id
    )
MERGE INTO refined.user_features AS target
USING source_cte as src
ON target.user_id = src.user_id
WHEN MATCHED THEN
    UPDATE SET
        searches_last_24h = src.searches_last_24h,
        clicks_last_24h = src.clicks_last_24h,
        ctr_last_24h = src.ctr_last_24h,
        last_activity_timestamp = src.last_activity_timestamp
WHEN NOT MATCHED THEN
    INSERT (user_id, searches_last_24h, clicks_last_24h, ctr_last_24h, last_activity_timestamp)
    VALUES (src.user_id, src.searches_last_24h, src.clicks_last_24h, src.ctr_last_24h, src.last_activity_timestamp);
