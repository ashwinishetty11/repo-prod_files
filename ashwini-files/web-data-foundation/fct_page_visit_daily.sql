--daily load for page visits table

with pages as
(     select id page_visit_id 
        , url page_url
        , s_id session_id
        , anonymous_id visitor_id
        , user_id cloud_user_id
        , timestamp visit_date
        , timestamp visit_start_time
        , lead(timestamp) over (partition by anonymous_id,s_id order by timestamp) visit_end_time

        , row_number() over (partition by anonymous_id,s_id order by timestamp) rnk1_isentrance
        , row_number() over (partition by anonymous_id,s_id order by timestamp desc) rnk2_isexit

        , context_locale language
        , tracking_utm_campaign utm_campaign
        , tracking_utm_source utm_source
        , tracking_utm_medium utm_medium
        , tracking_utm_term utm_term
      from `datascience-222717.confluentio_segment_prod.pages` pages
      where DATE(timestamp) = DATE('{{ next_execution_date.isoformat() }}') 
)
select page_visit_id 
        , page_url 
        , visitor_id 
        , cloud_user_id 
        , session_id  
        , language 
        , visit_start_time
        , visit_end_time
        , utm_campaign
        , utm_source
        , utm_medium
        , utm_term
        , (case when rnk1_isentrance = 1 then 'True' else 'False' end) is_entrance
        , (case when rnk2_isexit = 1     then 'True' else 'False' end) is_exit
        , (case when  rnk1_isentrance = 1 and rnk2_isexit = 1 then 'True' else 'False' end) is_bounce  
  --    ,  TIMESTAMP('{{next_execution_date.isoformat()}}') as extract_date
from pages

