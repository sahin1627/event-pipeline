-- uniqueness check
-- should return zero
SELECT
    account_id, count(*) as cnt
FROM refined.account_features
GROUP BY account_id
HAVING COUNT(*) > 1
;

-- freshness check
-- should return zero
with max_updated_at as (
    select max(updated_at) as max_updated_at from refined.account_features
)
select
    count(*) as cnt
from refined.account_features
WHERE current_timestamp - (select max_updated_at from max_updated_at) > INTERVAL '1 day'
;
