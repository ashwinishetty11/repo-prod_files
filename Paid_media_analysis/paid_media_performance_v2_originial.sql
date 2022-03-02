
CREATE OR REPLACE TABLE rpt_marketing.paid_media_performance_v2 AS (
with pg_base_table as (
  select channel,
    channel_detail,
    source,
    web_campaign_name,
    inquiry_date,
    pg_new_arr_amount_credit,
    pg_new_arr_oppty_credit
  from rpt_new_pg_attribution.mktg_pg_reporting
  where channel_aggregate = 'Paid Media'
  and web_campaign_name is not null
  and inquiry_date  >= '2019-07-01'
  order by 1,2,3),
  pg_summary_table as (
    select
      case when channel_detail = 'Display' then 'display'
        when channel_detail = 'Paid Gmail' then 'gmail'
        when channel_detail = 'Paid Search' then 'sem'
        when channel_detail = 'Paid Social' then channel_detail
        else 'other' end as mapping_channel_type,
      case when channel_detail in ('Display','Paid Gmail') then 'google'
        when channel_detail in ('Paid Search','Paid Social') then lower(source)
        else 'other' end as mapping_channel,
      inquiry_date,
      web_campaign_name,
      sum(pg_new_arr_amount_credit) pg_new_arr_amount_credit,
      sum(pg_new_arr_oppty_credit) pg_new_arr_oppty_credit
    from pg_base_table
    group by 1,2,3,4),
  inbound_lead_funnel as (
    select
      case when inq_channel_detail = 'Paid Social' then inq_channel_detail
        when inq_channel_detail = 'Paid Search' then 'sem'
        when inq_channel_detail = 'Paid Gmail' then 'gmail'
        when inq_channel_detail = 'Display' then 'display'
        else 'other' end as mapping_channel_type,
      case when inq_channel_detail = 'Paid Social' and lower(utm_source) like '%facebook%' then 'facebook'
        when inq_channel_detail = 'Paid Social' and lower(utm_source) like '%linkedin%' then 'linkedin'
        when inq_channel_detail = 'Paid Social' and lower(utm_source) like '%twitter%' then 'twitter'
        when inq_channel_detail = 'Display' then 'google'
        when inq_channel_detail = 'Paid Gmail' then 'google'
        when inq_channel_detail = 'Paid Search' and lower(utm_source) like '%bing%' then 'bing'
        when inq_channel_detail = 'Paid Search' and lower(utm_source) like '%google%' then 'google'
        else 'other' end as mapping_channel,
      inquiry_created_date_utc inquiry_date,
      utm_campaign,
      count(distinct(  campaign_member_id )) as inquiries,
      count(distinct( lead_or_contact_id )) as leads_or_contacts_created,
      count(distinct( case when is_net_new_lead then lead_or_contact_id else null end)) as net_new_leads,
      count(distinct( case when is_sqm then lead_or_contact_id else null end)) as sales_qualified_meetings,
      count(distinct( case when is_mql then lead_or_contact_id else null end)) as marketing_qualified_leads,
      count(distinct( case when is_sql then lead_or_contact_id else null end)) as sales_qualified_leads,
      -- add lead score columns 
      count(distinct( case when mk_lead_grade_score_at_lead_create >= 90 then lead_or_contact_id else null end)) as high_score_leads,
      count(distinct( case when mk_lead_grade_score_at_lead_create >=62 and 
  mk_lead_grade_score_at_lead_create < 90 then lead_or_contact_id else null end)) as medium_score_leads,
      count(distinct( case when mk_lead_grade_score_at_lead_create < 62 then lead_or_contact_id else null end)) as low_score_leads,
      count(distinct( case when mk_lead_grade_score_at_lead_create is null then lead_or_contact_id else null end)) as missing_score_leads,
      -- add persona data 
      count(distinct (case when lead_or_contact_persona like '%Arch%' then lead_or_contact_id else null end)) as Architect,
      count(distinct (case when lead_or_contact_persona like '%Dev%' then lead_or_contact_id else null end)) as Developer,
      count(distinct (case when lead_or_contact_persona like '%Exec%' then lead_or_contact_id else null end)) as Executive,
      count(distinct (case when lead_or_contact_persona = 'Operator' then lead_or_contact_id else null end)) as Operator,
      count(distinct (case when lead_or_contact_persona = 'Other_IT_Buyer' then lead_or_contact_id else null end)) as Other_IT_Buyer,
      count(distinct (case when lead_or_contact_persona = 'Other' then lead_or_contact_id else null end)) as Other,
      count(distinct (case when lead_or_contact_persona is null then lead_or_contact_id else null end)) as Unknown,
      -- add campaign type grouping
      count(distinct (case when campaign_type = 'Online Collateral' then lead_or_contact_id else null end)) as online_collateral,
      count(distinct (case when campaign_type = 'Webinar - Recorded' then lead_or_contact_id else null end)) as webinar_recorded,
      count(distinct (case when campaign_type = 'Webinar - Live' then lead_or_contact_id else null end)) as webinar_live,
      count(distinct (case when campaign_type = 'Confluent Platform Download' then lead_or_contact_id else null end)) as cp_download,
      count(distinct (case when campaign_type = 'Kafka Summit' then lead_or_contact_id else null end)) as kafka_summit,
      count(distinct (case when campaign_type = 'Contact Us' then lead_or_contact_id else null end)) as contact_us
    from `rpt_leads_pipeline.inbound_lead_funnel`
    where inq_channel_agg = 'Paid Media'
    and inquiry_created_date_utc >= '2019-07-01'
    group by 1,2,3,4),
cloud_table as (
    select date(a.signed_up_at) as sign_up_date
    ,case when channel = 'Paid Social' then channel
        when channel = 'Paid Search' then 'sem'
        when channel = 'Paid Gmail' then 'gmail'
        -- note: attribution table doesn't have paid gmail value
        when channel = 'Display' then 'display'
        else 'other' end as mapping_channel_type
    , case when (channel_detail ='Display' or channel_detail = 'Paid Search') then 'Google' else channel_detail end as source
    , a.web_campaign_name as utm_campaign
    , count(a.org_id) as cloud_signups
    , sum(case when email_verified_at is not null then 1 else 0 end) as email_verified
    , sum(case when cluster_created_at is not null then 1 else 0 end) as cluster_created
    , sum(case when usage_at is not null then 1 else 0 end) as ob_activated
    , sum(case when product_active_at is not null then 1 else 0 end) as product_activated
    from rpt_c360.cc_signup_attribution a
    left join  (
        select org_id, email_verified_at, cluster_create_confirmed_at, cluster_created_at, usage_at, product_active_at 
        from rpt_c360.cc_funnel) f  
    on a.org_id = f.org_id
    where channel_aggregate = 'Paid Media'
    group by 1,2,3,4)
select t1.*,
  date_trunc(t1.day, quarter) as quarter,
  t2.pg_new_arr_amount_credit,
  t2.pg_new_arr_oppty_credit,
  t3.inquiries,
  t3.leads_or_contacts_created leads_or_contacts_touched,
  t3.net_new_leads,
  t3.marketing_qualified_leads,
  t3.sales_qualified_meetings,
  t3.sales_qualified_leads,
  t3.high_score_leads,
  t3.medium_score_leads,
  t3.low_score_leads,
  t3.missing_score_leads,
  t3.Architect,
  t3.Developer,
  t3.Executive,
  t3.Operator,
  t3.Other_IT_Buyer,
  t3.Other,
  t3.Unknown,
  t3.online_collateral,
  t3.webinar_recorded,
  t3.webinar_live,
  t3.cp_download,
  t3.kafka_summit,
  t3.contact_us,
  t4.content_name,
  t4.content_type,
  t5.targeting_name,
  t6.cloud_signups as Signups,
  t6.email_verified,
  t6.cluster_created,
  t6.ob_activated,
  t6.product_activated as PAOs
from rpt_marketing.paid_media_summary t1
left join pg_summary_table t2 on
  t1.day = t2.inquiry_date and
  lower(t1.channel_type) = lower(t2.mapping_channel_type) and
  lower(t1.channel) = lower(t2.mapping_channel) and
  t1.adgroup = t2.web_campaign_name
left join inbound_lead_funnel t3 on
  t1.day = t3.inquiry_date and
  lower(t1.channel_type) = lower(t3.mapping_channel_type) and
  lower(t1.channel) = lower(t3.mapping_channel) and
  t1.adgroup = t3.utm_campaign
left join (select content_code, content_name, content_type from rpt_marketing.paid_media_content_mapping group by 1,2,3) t4 on
  t1.content = t4.content_code
left join (select targeting_code, targeting_name from rpt_marketing.paid_media_targeting_mapping group by 1,2) t5 on
  t1.targeting = t5.targeting_code
left join cloud_table t6 on
  t1.day = t6.sign_up_date and
  lower(t1.channel_type) = lower(t6.mapping_channel_type) and
  lower(t1.channel) = lower(t6.source) and
  t1.adgroup = t6.utm_campaign
order by 1,2,3);
