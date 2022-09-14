  select * from 
  (
  select * , lag(url_cleaned) over (partition by visitor_id, session_id, context_ip, page_visit_id order by visit_start_time asc) url_lead
from `datascience-222717.dwh_web.fct_page_visit_daily`
  )

where lower(url_cleaned) like '%thank%'

select distinct url_cleaned
from 
(
  select distinct url_cleaned 
    from `datascience-222717.dwh_web.fct_page_visit_daily` 
    where (    url_cleaned like '%/resources%'
            or url_cleaned like '%/thank-you%'
            or url_cleaned like '%/designing-event-driven-systems%'
            or url_cleaned like '%/online-talk%'
            or url_cleaned like '%/ebook%'
            or url_cleaned like '%/apache-kafka-stream-processing-book-bundle%'
            or url_cleaned like '%/white-paper%'
            or url_cleaned like '%/demo%'
            or url_cleaned like '%/report/%'
          )
     
    UNION ALL

    select distinct url_previous 
    from (
      select distinct url_cleaned, lag(url_cleaned) over (partition by visitor_id, session_id, context_ip order by visit_start_time asc) url_previous
      from `datascience-222717.dwh_web.fct_page_visit_daily` 
    )
    where url_cleaned like '%/thank-you%'
)
where url_cleaned not like '%/thank-you%'
    and url_cleaned is not null
