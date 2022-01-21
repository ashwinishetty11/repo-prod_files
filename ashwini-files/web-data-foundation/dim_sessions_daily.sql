--insert into dim_sessions_daily ()

with pages as( 
    select  s_id session_id
        , anonymous_id visitor_id  
    --   , context_locale language
    --   , SUBSTR(context_locale, 0, 2) visit_ip_country  

        , MIN(timestamp) visit_start_time
        , MAX(timestamp) visit_end_time
        , date_diff(MAX(timestamp), MIN(timestamp), MINUTE) tot_session_time
        , count(url) cnt_page_views
from `datascience-222717.confluentio_segment_prod.pages` pages
-- where timestamp >= DATE_ADD(DATE current_date, INTERVAL -2 DAY) 
--                and timestamp <=  DATE_ADD(DATE current_date, INTERVAL -1 DAY)  -- how to get the extract_date
group by 1,2  
) 
,  inquiries as (
        select s_id session_id
                , anonymous_id visitor_id
                , count(*) cnt_inquiries
                , count(case when form_id in (2835, 6067) then 1 else null end ) cnt_cloud_signup
                , count(case when form_id in (3109, 5922) then 1 else null end ) cnt_cp_downloads
        from `datascience-222717.confluentio_segment_prod.form_submission` 
        group by 1,2
)
select p.session_id
        , p.visitor_id
        , p.visit_start_time 
        , p.visit_end_time
        , p.tot_session_time
        , p.cnt_page_views
        , ifnull(i.cnt_inquiries,0) cnt_inquiries
        , ifnull(i.cnt_cp_downloads,0) cnt_cp_downloads
        , ifnull(i.cnt_cloud_signup,0) cnt_cloud_signup
from pages p
        left join inquiries i on p.visitor_id = i.visitor_id and p.session_id  = i.session_id
