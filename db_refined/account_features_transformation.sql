WITH last_24h AS (
    SELECT *
    FROM staging.product_events_raw
    WHERE event_timestamp >= NOW() - INTERVAL '24 hours'
),
last_7d AS (
    SELECT *
    FROM staging.product_events_raw
    WHERE event_timestamp >= NOW() - INTERVAL '7 days'
),
active_users AS (
    SELECT account_id, COUNT(DISTINCT user_id) AS active_users_last_7d
    FROM last_7d
    GROUP BY account_id
),
searches AS (
    SELECT account_id, COUNT(*) AS searches_last_24h
    FROM last_24h
    WHERE event_type = 'search_performed'
    GROUP BY account_id
),
clicks AS (
    SELECT account_id, COUNT(*) AS clicks_last_24h
    FROM last_24h
    WHERE event_type IN ('result_clicked', 'recommendation_clicked')
    GROUP BY account_id
),
ctr AS (
    SELECT s.account_id,
           CASE WHEN s.searches_last_24h = 0 THEN 0
                ELSE c.clicks_last_24h::float / s.searches_last_24h
           END AS account_ctr_last_24h
    FROM searches s
    LEFT JOIN clicks c ON s.account_id = c.account_id
),
most_viewed AS (
    SELECT DISTINCT ON (account_id)
           account_id,
           category AS most_viewed_category_7d
    FROM (
        SELECT account_id, category, COUNT(*) AS cnt
        FROM last_7d
        WHERE event_type = 'product_viewed'
        GROUP BY account_id, category
        ORDER BY account_id, cnt DESC
    ) t
), source_cte as (
    SELECT
        au.account_id,
        au.active_users_last_7d,
        COALESCE(c.account_ctr_last_24h, 0) AS account_ctr_last_24h,
        COALESCE(mv.most_viewed_category_7d, '') AS most_viewed_category_7d
    FROM active_users au
    LEFT JOIN ctr c ON au.account_id = c.account_id
    LEFT JOIN most_viewed mv ON au.account_id = mv.account_id
)
MERGE INTO refined.account_features AS target
USING source_cte AS src
ON target.account_id = src.account_id
WHEN MATCHED THEN
    UPDATE SET
        active_users_last_7d = src.active_users_last_7d,
        account_ctr_last_24h = src.account_ctr_last_24h,
        most_viewed_category_7d = src.most_viewed_category_7d
WHEN NOT MATCHED THEN
    INSERT (account_id, active_users_last_7d, account_ctr_last_24h, most_viewed_category_7d)
    VALUES (src.account_id, src.active_users_last_7d, src.account_ctr_last_24h, src.most_viewed_category_7d);
