
create or replace table dwh_web.rpt_web_dashboard as

with pages as (
  select session_id
          , visitor_id
          , url
          , url_cleaned
          , visit_start_time
          , domain
          , channel_detail
          , channel_aggregate
  --      , region
          , flg_new_vs_returned
          , url_section_1
          , url_section_2
          , url_section_3
          , rnk_page_visit
  from `datascience-222717.dwh_web.fct_page_visit_daily`
  where date(visit_start_time) >= date(date_trunc(date_add(current_date(), INTERVAL -1 quarter), quarter))
     and domain in (  'confluent.io','docs.io', 'developer.io')
)
 , seg_with_lead_data as  (
        select date(date_trunc(visit_start_time,month)) visit_month,
                        t1.*, t2.lead_or_contact_id
        from pages t1
            left join ( select distinct anonymous_id
                                , lead_or_contact_id
                                , source
                                , min(first_appearance_at) min_first_appearance_at
                         from `datascience-222717.dwh_web.lookup_sfdc_lead_to_segment`
                         group by 1,2,3
                        ) t2
                on t2.anonymous_id = t1.visitor_id and t1.domain = t2.source
            --      and TIMESTAMP_DIFF(min_first_appearance_at,vist_start_time,day) <= 1
       )
  , sfdc_lead_data as (
    select  distinct
      lead_or_contact_id
      , campaign_member_id
      , tactic
      , campaign_type
      , createddate inquiry_date_time
      , opportunity_id
      , lead_or_contact_persona
      , mql_date_time
      , sal_date_time
      , sql_date_time
      , sqm_date_time
      , sao_date_time
    from rpt_leads_pipeline.inbound_lead_funnel
    where date(createddate) >= date(date_trunc(date_add(current_date(), INTERVAL -1 quarter), quarter))  --
    and lead_or_contact_id in ( select distinct lead_or_contact_id from seg_with_lead_data)
)
, seg_pages_inquiry as (
  select t1.*, t2.* except(lead_or_contact_id)
      , TIMESTAMP_DIFF(t2.inquiry_date_time,t1.visit_start_time,day) delta_inqcreation_visit
      , TIMESTAMP_DIFF(t2.mql_date_time,t2.inquiry_date_time,day) delta_mql_inq
      , TIMESTAMP_DIFF(t2.sqm_date_time,t2.inquiry_date_time,day) delta_sqm_inq
      , TIMESTAMP_DIFF(t2.sao_date_time,t2.inquiry_date_time,day) delta_sao_inq
  from seg_with_lead_data t1
      left join sfdc_lead_data t2  on t1.lead_or_contact_id = t2.lead_or_contact_id
                                      and TIMESTAMP_DIFF(t2.inquiry_date_time,t1.visit_start_time,day) >=-7
                                      and TIMESTAMP_DIFF(t2.inquiry_date_time,t1.visit_start_time,day) <=7
)
, lead_metrics_pg as (
  select t1.*
  -- , t1.lead_or_contact_id
      , case when delta_inqcreation_visit >=0 and delta_inqcreation_visit <= 1 then 1 else 0 end flg_1d_inq
      , case when delta_inqcreation_visit >=0 and delta_inqcreation_visit <= 7 then 1 else 0 end flg_7d_inq
      , case when delta_inqcreation_visit >=0 and delta_inqcreation_visit <= 1 and delta_mql_inq <=7 then 1 else 0 end flg_7d_mql
      , case when delta_inqcreation_visit >=0 and delta_inqcreation_visit <= 1 and delta_mql_inq <=7 and delta_sqm_inq <=14 then 1 else 0 end flg_14d_sqm
      , case when delta_inqcreation_visit >=0 and delta_inqcreation_visit <= 1 and delta_mql_inq <=7 and delta_sqm_inq <=14
                                                                                                    and delta_sao_inq <=60 then 1 else 0 end num_60d_sqm
  from seg_pages_inquiry t1
)

,  sfdc_inquiries_campaign_type as (
  -- inquiry details from sfdc inquiry
    select  distinct form_id campaign_type --change column_name here
      , campaign_member_id
      , i.inquiry_created_ts
      , lead_or_contact_id
    from `datascience-222717.dwh_web.fct_inquiry_daily` i
    where date(i.inquiry_created_ts) >= date_add(date(date_trunc(date_add(current_date(), INTERVAL -1 quarter), quarter)) , INTERVAL -7 DAY)
    and lead_or_contact_id in ( select distinct lead_or_contact_id from seg_with_lead_data)
  )

  , inquiry_metrics as (
    select t1.*, t2.* except(lead_or_contact_id)
      , case when campaign_type = 'Confluent Cloud Sign-Up' then 1 else 0 end flg_cloud_signup
      , case when campaign_type = 'Confluent Platform Download' then 1 else 0 end flg_cp_download
    from seg_with_lead_data t1
        left join sfdc_inquiries_campaign_type t2   on t1.lead_or_contact_id = t2.lead_or_contact_id
                                        and TIMESTAMP_DIFF(t2.inquiry_created_ts,t1.visit_start_time,day) >=-7
                                        and TIMESTAMP_DIFF(t2.inquiry_created_ts,t1.visit_start_time,day) <= 7
)

select distinct date(date_trunc(t1.visit_start_time, week)) visit_start_time_week
        ,t1.*
        , im.flg_cloud_signup
        , im.flg_cp_download
        , TIMESTAMP('{{next_execution_date.isoformat()}}') as extract_date
      --   , TIMESTAMP(current_date()) as extract_date
from lead_metrics_pg t1
  left join inquiry_metrics im on t1.campaign_member_id = im.campaign_member_id and t1.lead_or_contact_id = im.lead_or_contact_id
