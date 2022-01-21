--dim_visitor
select anonymous_id visitor_id
        , user_id cloud_user_id
      --  , context_locale --country
        , min(timestamp) first_visit_time
        , max(timestamp) most_recent_visit_time
from `datascience-222717.confluentio_segment_prod.pages` i
group by  1,2

order by  2, 1
/*Note: 
1. some cloud_user_ids are mapped to more than one anon_ids, meaning the same user has browsed using
different devices */

--fct_page_visit_daily
select id page_view_id
        , timestamp
        , url page_url
        , anonymous_id visitor_id
        , user_id cloud_user_id
        , s_id session_id
        , context_locale 
        , SUBSTR(context_locale, 0, 2) country
        , SUBSTR(context_locale, 4, 5) language 
        , tracking_utm_campaign utm_campaign
        , tracking_utm_source utm_source
        , *
from `datascience-222717.confluentio_segment_prod.pages` 
order by 1
limit 200


----------
select s_id session_id
        , anonymous_id visitor_id
        , SUBSTR(context_locale, 0, 2) country
        , SUBSTR(context_locale, 4, 5) language 
        , id page_view_id
        , timestamp
        , url page_url
       
        , user_id cloud_user_id
        , 
        , context_locale 
        
        
        , tracking_utm_campaign utm_campaign
        , tracking_utm_source utm_source
        , *  
	, min(timestamp) visit_start_time
	, max(timestamp) visit_end_time
	, date_diff(max(timestamp), min(timestamp), 'sec') total_session_time
	
	count_page_views -- count(). -- get it from pages
	count_inquiries -- 
	count_sign_ups
	count_cp_downloads -- from form fills table
	visit_date
	extract_date
        select timestamp, *
from `datascience-222717.confluentio_segment_prod.identifies`  where s_id = '6facec9c-989b-4cdd-b735-66ad59e5dfbd'
order by 1
limit 200



 select timestamp, *
from `datascience-222717.confluentio_segment_prod.pages`  where s_id = '6facec9c-989b-4cdd-b735-66ad59e5dfbd'

select timestamp,* 
from `datascience-222717.confluentio_segment_prod.form_submission`  where s_id = '6facec9c-989b-4cdd-b735-66ad59e5dfbd'

