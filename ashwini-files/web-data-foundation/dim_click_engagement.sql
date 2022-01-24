   select s_id session_id
        , anonymous_id visitor_id
        , user_id cloud_user_id
        , component
    --    , page_visit_id
        , event
        , event_text
        , id click_id
        , timestamp    
        , href_url
        , location                       
     from `datascience-222717.confluentio_segment_prod.click`
