CREATE OR REPLACE TABLE dwh_web.dim_sessions as

with pages as (
       select  s_id session_id
                 , anonymous_id visitor_id
                 , string_agg(distinct context_locale) language
                 , MIN(timestamp) visit_start_time
                 , MAX(timestamp) visit_end_time
                 , date_diff(MAX(timestamp), MIN(timestamp), MINUTE) tot_session_time_min
                 , count(url) cnt_page_views
                 , lower(string_agg(distinct tracking_utm_source)) utm_source
    from confluentio_segment_prod.pages pages
    where DATE(timestamp) >= '2022-01-01'
    group by 1,2
)
,  inquiries as (
      select s_id session_id
                , anonymous_id visitor_id
                , count(*) cnt_inquiries
                , count(case when form_id in (2835, 6067) then 1 else null end ) cnt_cloud_signup
                , count(case when form_id in (3109, 5922) then 1 else null end ) cnt_cp_downloads
      from confluentio_segment_prod.form_submission
      where timestamp >= '2022-01-01'
      group by 1,2
)
select p.session_id
        , p.visitor_id
        , p.language
        , p.visit_start_time
        , p.visit_end_time
        , p.tot_session_time_min
        , p.utm_source
        , p.cnt_page_views
        , ifnull(i.cnt_inquiries,0) cnt_inquiries
        , ifnull(i.cnt_cp_downloads,0) cnt_cp_downloads
        , ifnull(i.cnt_cloud_signup,0) cnt_cloud_signup
        , TIMESTAMP('{{next_execution_date.isoformat()}}') as extract_date
from pages p
        left join inquiries i on p.visitor_id = i.visitor_id and p.session_id  = i.session_id
