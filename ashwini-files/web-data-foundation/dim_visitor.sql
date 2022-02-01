CREATE OR REPLACE TABLE dwh_web.dim_visitor as

select anonymous_id visitor_id
        , user_id cloud_user_id
      --  , context_locale --country
        , min(timestamp) first_visit_time
        , max(timestamp) most_recent_visit_time
from confluentio_segment_prod.pages i
group by  1,2
