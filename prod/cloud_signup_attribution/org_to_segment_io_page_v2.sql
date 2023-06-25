create or replace table `data-sandbox-123.Workspace_Ashwini.rpt_cloud_signup_attr_v3_rpt_cloud_signup_attr_v3_org_to_segment_io_page_v2` as 

with form_fill_identify as (
select email,
  org_id,
  created_at,
  first_user_id,
  user_created_at,
  anonymous_id,
  cloud_anonymous_id, io_anonymous_id,
  segment_identifies_anonymous_id,
  ts_segment_identify, 
  segment_cloud_timestamp cloud_timestamp,
  tracking_utm_campaign identify_utm_campaign,
  tracking_utm_medium identify_utm_medium,
  tracking_utm_source identify_utm_source,
  context_page_referrer
from `data-sandbox-123.Workspace_Ashwini.rpt_cloud_signup_attr_v3_org_to_segment_io_session_v2` )

--  select count(distinct coalesce(segment_identifies_anonymous_id, io_anonymous_id) ) from  `data-sandbox-123.Workspace_Ashwini.rpt_cloud_signup_attr_v3_org_to_segment_io_session_v2`--16428/ 3705/ 12360
 --     select count(distinct anonymous_id) --2436
 --     from confluentio_segment_prod.pages
 --     where anonymous_id in (
 --       select distinct anonymous_id
 --       from form_fill_identify)


 --   select --count(distinct segment_identifies_anonymous_id) --2436
 --        distinct segment_identifies_anonymous_id --, anonymous_id
 --   from form_fill_identify
 --   where segment_identifies_anonymous_id   in (
 --     select distinct anonymous_id
 --      from confluentio_segment_prod.pages ) 

  -- select --count(distinct cloud_anonymous_id) --2436
  --        distinct cloud_anonymous_id --, anonymous_id
  --   from form_fill_identify
  --   where cloud_anonymous_id   in (
  --     select distinct anonymous_id
  --      from  prod_cloud_ui.pages ) 

 , segment_page as (
  select anonymous_id,
      ga_cid, s_id, url, timestamp ts_segment_page,
      date(date_trunc(timestamp,day)) ts_segment_page_date
      ,tracking_utm_campaign
      ,tracking_utm_medium
      ,tracking_utm_source
      ,tracking_creative
      ,tracking_utm_term
      ,context_page_referrer
  from datascience-222717.confluentio_segment_prod.pages
  where anonymous_id in (
    select distinct anonymous_id
    from form_fill_identify)
)
    , cloud_segment_page as (
  select anonymous_id, url, timestamp ts_cloud_segment_page,
      date(date_trunc(timestamp,day)) ts_cloud_segment_page_date
      ,utm_campaign
      ,utm_medium
      ,utm_source
      ,utm_term
      ,context_page_referrer
  from  datascience-222717.prod_cloud_ui.pages
  where anonymous_id in (
    select distinct anonymous_id
    from form_fill_identify))
    
 , data_all as (
  select t1.* ,--, t2.* except(anonymous_id),
      TIMESTAMP_DIFF(t1.created_at,ts_segment_page,day) delta_orgcreation_web,
      TIMESTAMP_DIFF(t1.created_at,ts_segment_page,hour) delta_orgcreation_web_hour,
      TIMESTAMP_DIFF(t1.created_at,ts_segment_page,second) delta_orgcreation_web_second,
      TIMESTAMP_DIFF(t1.created_at,cloud_timestamp,hour) delta_orgcreation_web_hour_cloud,

        TIMESTAMP_DIFF(ts_segment_identify,ts_segment_page,day) delta_orgcreation_web1,
      TIMESTAMP_DIFF(ts_segment_identify,ts_segment_page,hour) delta_orgcreation_web_hour1,
      TIMESTAMP_DIFF(ts_segment_identify,ts_segment_page,second) delta_orgcreation_web_second1,
      TIMESTAMP_DIFF(ts_segment_identify,cloud_timestamp,hour) delta_orgcreation_web_hour_cloud1,
  from form_fill_identify t1
      left join segment_page t2 on t1.anonymous_id = t2.anonymous_id and ts_segment_page< t1.created_at 
      left join cloud_segment_page t3 on t1.cloud_anonymous_id = t3.anonymous_id and cloud_timestamp <t1.created_at
  where -- (ts_segment_page<ts_segment_identify and TIMESTAMP_DIFF(ts_segment_identify,ts_segment_page,day) <= 30)  
        --or (cloud_timestamp<ts_segment_identify and TIMESTAMP_DIFF(ts_segment_identify,cloud_timestamp,day) <= 30) 
         
        TIMESTAMP_DIFF(t1.created_at,ts_segment_page,day) <= 30 or TIMESTAMP_DIFF(t1.created_at,cloud_timestamp,day) <= 30
     -- or ts_segment_page is null //uncomment later
     -- or ts_cloud_segment_page is null 
)
select * from data_all;
/*
select count(distinct org_id) 
  , count( distinct case when coalesce(t1.identify_utm_campaign , t1.identify_utm_source,t1.identify_utm_medium
          ,   case when    context_page_referrer like '%docs.confluent.io%' 
                        or context_page_referrer like '%developer.confluent.io%' 
                        or context_page_referrer like '%confluent.io%' then null else context_page_referrer end
                 ) is not null then org_id else null end)
  ,  count( distinct case when coalesce(t1.identify_utm_campaign , t1.identify_utm_source,t1.identify_utm_medium , context_page_referrer) 
                   is not null then org_id else null end)
  ,  count( distinct case when  anonymous_id is not null then org_id else null end) anon_id
from data_all t1  */
-- why dont some anonymous-ids not have page visits within 30D in pages tables? 
