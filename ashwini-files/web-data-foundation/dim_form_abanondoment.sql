
with form_abandonment as (
     select s_id session_id
        , anonymous_id visitor_id
        , user_id cloud_user_id
    --    , page_visit_id
        , event
        , event_text
        , form_id
        , is_form_hidden
        , form_fields_completed completed_form_fields
        , form_fields_empty empty_form_fields
        , last_active_input
        , timestamp   
        , context_page_url url               --           , *
     from `datascience-222717.confluentio_segment_prod.form_abandonment`   
)
, pages as (
     select distinct anonymous_id visitor_id, s_id session_id,url, timestamp, id
     from `datascience-222717.confluentio_segment_prod.pages` pages 
)
select f.*, p.id page_visit_id
from form_abandonment f 
        left join pages p on   f.visitor_id = p.visitor_id 
                           and f.session_id = p.session_id 
                           and f.url = p.url
                           and      f.timestamp >= DATE_ADD( p.timestamp, INTERVAL -2 MIN) 
                                and f.timestamp <= DATE_ADD( p.timestamp, INTERVAL  2 MIN )    
