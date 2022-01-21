select anonymous_id visitor_id
        , user_id cloud_user_id
      --  , context_locale --country
        , min(timestamp) first_visit_time
        , max(timestamp) most_recent_visit_time
from `datascience-222717.confluentio_segment_prod.pages` i
group by  1,2
