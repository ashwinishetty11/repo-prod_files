--CREATE OR REPLACE TABLE dwh_web.fct_page_visit_daily as

with attach_country_data as (
    select *
    from (
      SELECT c.*
            , p2.execution_ts ip_snapshot_ts
            , p2.country country
            , p2.country_3
            , p2.name country_name
            , row_number() over (partition by domain, visitor_id , session_id ,c.visit_start_time, c.page_visit_id, c.context_ip order by execution_ts desc  ) rnk
      FROM   dwh_web.stg_fct_page_visit_daily  c
          LEFT OUTER JOIN dwh_ips.ips p2 ON (c.context_ip = p2.ip AND date(c.visit_start_time) >= date(p2.execution_ts) )
    )
    where rnk =1
 )

 , cte_ip as (
   select *, row_number() over (partition by ip order by execution_ts asc) rnk2
   from dwh_ips.ips
 )
  , add_country_historical as (
    select t1.* except(country, country_3, country_name, ip_snapshot_ts)
        , coalesce(t1.country         , t2.country        ) country
        , coalesce(t1.country_3       , t2.country_3      ) country_3
        , coalesce(t1.country_name    , t2.name           ) country_name
        , coalesce(t1.ip_snapshot_ts  , t2.execution_ts   ) ip_snapshot_ts
    from attach_country_data t1
      left join (select * from cte_ip where rnk2 = 1) t2 on t1.context_ip = t2.ip and t1.country is null
  )

  , theater_info as (
    select distinct country, theater
    from `datascience-222717.rpt_attribution.touchpoints_sessions`
    )
  , region_details as (
    select t1.*, t2.theater
    from   add_country_historical t1
      left join theater_info t2 on t1.country_name = t2.country
  )
  , cta_url_list as (
        select distinct url_cleaned url
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

  )
  select *
    , case when url_cleaned like '%/thank-you%' then True else False end flg_page_thank_you
    , case when url_cleaned in (  select distinct url from cta_url_list) then True else False end flg_page_CTA
   -- , TIMESTAMP('{{next_execution_date.isoformat()}}') as extract_date
    ,  TIMESTAMP(current_date) as extract_date
  from region_details
--19472268
;
