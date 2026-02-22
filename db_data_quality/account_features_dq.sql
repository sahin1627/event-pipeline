-- uniqueness check
-- should return zero
SELECT
    user_id, count(*) as cnt
FROM refined.user_features
GROUP BY user_id
HAVING COUNT(*) > 1
;

-- freshness check
-- should return zero
with max_updated_at as (
    select max(updated_at) as max_updated_at from refined.user_features
)
select
    count(*) as cnt
from refined.user_features
WHERE current_timestamp - (select max_updated_at from max_updated_at) > INTERVAL '1 day'
;
