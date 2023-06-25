
create or replace table `data-sandbox-123.Workspace_Ashwini.rpt_cloud_signup_attr_v3_org_to_segment_io_session_v2` as

with first_signup_user as (
  select id user_id, email,
    t2.org_id, t2.created_at user_created_at,
    is_deactivated user_deactivated_at,
    is_service_account user_service_account,
    is_internal user_internal,
    --email_verified_at user_email_verified_at,
    row_number() over (partition by t2.org_id order by t2.created_at) as rn
  from fnd_mothership.user t1
  left join fnd_mothership.org_membership t2 on t1.id = t2.user_id
  )

  , org_with_first_user_lead as (
  select id as org_id,
    t1.created_at,
    t2.email,
    t2.user_id as first_user_id,
    t2.user_created_at,
    marketplace_partner,
    commercial_model,
    sign_up_sales_influence,
    sfdc_account_theater,
    sfdc_account_area,
    sfdc_account_region
  from rpt_cc.organization t1
  left join (
    select * except (rn)
    from first_signup_user where rn = 1) t2 on t1.id = t2.org_id
  where is_customer 
  and date_trunc(created_at, quarter) = '2023-01-01' 
     -- and acquisition_channel = 'No Referral'
      and motion ='Product-Led' 
      )
  --  select count(distinct org_id) from org_with_first_user_lead --delete in final script

 , segment_email as (
select distinct email, anonymous_id,ga_cid, s_id,timestamp ts_segment_identify,
  tracking_utm_campaign,
  tracking_utm_medium,
  tracking_utm_source,
  context_page_referrer -- Change: Include context page-referrer field
from confluentio_segment_prod.identifies
where email in (select email from org_with_first_user_lead)
and timestamp >= '2021-04-01'
)
 -- Change: Add Direct Cloud Signup details
 , cloud_signups as (
      select distinct anonymous_id cloud_anonymous_id,timestamp ts_segment_cloud,user_id,
          utm_campaign,
          utm_medium,
          utm_source,
          context_page_referrer -- Change: Include context page-referrer field
      from prod_cloud_ui.pages
      where user_id in (select first_user_id from org_with_first_user_lead)
        and timestamp >= '2021-04-01'
),
-- 2022/05/02 query fix: include social data
social_visit as (
select distinct coalesce(io_anonymous_id,anonymous_id) io_anonymous_id,
  user_id,
  timestamp,
  utm_campaign,
  utm_medium,
  utm_source,
  context_page_referrer -- Change: Include context page-referrer field
from prod_cloud_ui.social_signup_success
where user_id is not null -- Change: Include cloud social signups as well
   -- and lower(type) in ('signup', 'join')
   -- and signup_source like '%iosocial%'
    and timestamp >= '2022-04-01')
    
    --To Eva: why do we need this? (change request w/ dependency)
/*
segment_to_ga_mapping as (
select distinct anonymous_id io_anonymous_id,ga_cid
from confluentio_segment_prod.pages
where ga_cid is not null
and anonymous_id in (select io_anonymous_id from social_visit)),

social_data_merged as (
select * from social_visit
inner join segment_to_ga_mapping using (io_anonymous_id)
)
*/ , 
org_mapped_to_segment_with_social as (
select t1.email,
  t1.org_id,
  t1.created_at,
  t1.first_user_id,
  t1.user_created_at,
  marketplace_partner,
  commercial_model,
  sign_up_sales_influence,
  sfdc_account_theater,
  sfdc_account_area,
  sfdc_account_region,
  coalesce(t2.anonymous_id,t3.io_anonymous_id,t4.cloud_anonymous_id ) as anonymous_id
  , t2.anonymous_id segment_identifies_anonymous_id
  , t3.io_anonymous_id io_anonymous_id
  , t4.cloud_anonymous_id
  , case when io_anonymous_id is not null then 'social signup'
         when anonymous_id is not null then 'direct'
         else null end flg_social_signup,
  t2.ga_cid as ga_cid,
  s_id,
  coalesce(t2.ts_segment_identify,t3.timestamp) ts_segment_identify,
  t2.ts_segment_identify segment_identifies_timestamp,
  t3.timestamp io_timestamp,
  t4.ts_segment_cloud segment_cloud_timestamp,
  coalesce(t2.tracking_utm_campaign,t3.utm_campaign, t4.utm_campaign) tracking_utm_campaign,
  coalesce(t2.tracking_utm_medium  ,t3.utm_medium  , t4.utm_medium  ) tracking_utm_medium,
  coalesce(t2.tracking_utm_source  ,t3.utm_source  , t4.utm_source  ) tracking_utm_source,
  coalesce(t2.context_page_referrer,t3.context_page_referrer, t4.context_page_referrer) context_page_referrer,
from org_with_first_user_lead t1
left join segment_email t2 
  on t1.email = t2.email 
  and TIMESTAMP_DIFF(created_at,ts_segment_identify,hour) <=2
  and TIMESTAMP_DIFF(created_at,ts_segment_identify,hour) >= -2
left join social_visit t3
 on t1.first_user_id = t3.user_id
left join cloud_signups t4 
  on t4.user_id = t1.first_user_id 
  and TIMESTAMP_DIFF(created_at,ts_segment_cloud,hour) <=2
  and TIMESTAMP_DIFF(created_at,ts_segment_cloud,hour) >=-2
 
),

org_mapped_to_segment as (
select *,
  TIMESTAMP_DIFF(created_at,ts_segment_identify,minute) delta_orgcreation_identify_min
from org_mapped_to_segment_with_social)

/*
 select * from org_mapped_to_segment where anonymous_id is null

select count(distinct org_id) 
  , count( distinct case when coalesce(t1.tracking_utm_campaign , t1.tracking_utm_source,t1.tracking_utm_medium
          ,   case when    context_page_referrer like '%docs.confluent.io%' 
                        or context_page_referrer  like '%developer.confluent.io%' 
                        or context_page_referrer  like '%confluent.io%' then null else context_page_referrer end
                 ) is not null then org_id else null end)
  ,  count( distinct case when coalesce(t1.tracking_utm_campaign , t1.tracking_utm_source,t1.tracking_utm_medium , context_page_referrer) is not null then org_id else null end)
  ,  count( distinct case when  anonymous_id is not null then org_id else null end) anon_id
from org_mapped_to_segment t1 
 --where --io_anonymous_id is not null -- cloud_anonymous_id is not null
 -- coalesce(t1.tracking_utm_campaign , t1.tracking_utm_source,t1.tracking_utm_medium, context_page_referrer) is not null ;
*/
-- 4k do not have ano
-- Original scripts below
 , org_mapped_to_session as (
    select * 
    from (
          select *,
          case when  coalesce(tracking_utm_campaign , tracking_utm_source, tracking_utm_medium, context_page_referrer) is not null 
                then row_number() over (partition by org_id order by abs(delta_orgcreation_identify_min)) else null end as rn_identify
          from org_mapped_to_segment
          order by created_at desc
        ) t1
    where rn_identify = 1
  )

  --select count(distinct org_id) from org_mapped_to_session t1 
  --where io_anonymous_id is   null and cloud_anonymous_id is not null 
  --where  coalesce(t1.tracking_utm_campaign , t1.tracking_utm_source,t1.tracking_utm_medium,context_page_referrer) is not null ; --delete in final script


select * from org_mapped_to_session;
