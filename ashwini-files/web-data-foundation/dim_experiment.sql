select  experiment_id
        , experiment_name
        , anonymous_id
        , campaign_id
        , campaign_name
        , is_in_campaign_holdback
        , variation_id
        , variation_name
        , timestamp
from `datascience-222717.confluentio_segment_prod.experiment_viewed`