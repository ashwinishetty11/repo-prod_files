
create or replace table dwh_web.rpt_web_dashboard as

with pages as (
  select coalesce(session_id , 'Null ')  session_id
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
          , page_visit_id
  from `datascience-222717.dwh_web.fct_page_visit_daily`
  where date(visit_start_time) >= date(date_trunc(date_add(current_date(), INTERVAL -1 quarter), quarter))
     --date(visit_start_time) >= '2021-10-01'
        and date(visit_start_time) <  date(date_add(current_date(), interval -1 day))
        and domain in (  'www.confluent.io','www.docs.io', 'www.developer.io', 'www.kafka-tutorials.io')
      --and domain in (  'www.confluent.io','docs.confluent.io', 'developer.confluent.io', 'developer.confluent.io/tutorials')
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
  select t1.* except(session_id, visit_start_time ,page_visit_id,lead_or_contact_id  )
      , coalesce(t1.session_id, 'Null ')session_id
      , t1.visit_start_time
      , coalesce(t1.page_visit_id, 'Null ')     page_visit_id
      , coalesce(t1.lead_or_contact_id, 'Null') lead_or_contact_id
      , t2.* except(lead_or_contact_id)
      , TIMESTAMP_DIFF(t2.inquiry_date_time,t1.visit_start_time,day) delta_inqcreation_visit
      , TIMESTAMP_DIFF(t2.mql_date_time,t2.inquiry_date_time,day) delta_mql_inq
      , TIMESTAMP_DIFF(t2.sqm_date_time,t2.inquiry_date_time,day) delta_sqm_inq
      , TIMESTAMP_DIFF(t2.sao_date_time,t2.inquiry_date_time,day) delta_sao_inq
  from seg_with_lead_data t1
      left join sfdc_lead_data t2  on t1.lead_or_contact_id = t2.lead_or_contact_id
                                      and TIMESTAMP_DIFF(t2.inquiry_date_time,t1.visit_start_time,day) >=-7
                                      and TIMESTAMP_DIFF(t2.inquiry_date_time,t1.visit_start_time,day) <= 7
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
    select  distinct form_id campaign_type --change column_name here
      , campaign_member_id
      , i.inquiry_created_ts
      , lead_or_contact_id
      , tactic
    from `datascience-222717.dwh_web.fct_inquiry_daily` i
    where date(i.inquiry_created_ts) >= date_add(date(date_trunc(date_add(current_date(), INTERVAL -1 quarter), quarter)) , INTERVAL -7 DAY)
    and lead_or_contact_id in ( select distinct lead_or_contact_id from seg_with_lead_data)
  )

  , inquiry_metrics as (
    select t1.* except(session_id, visit_start_time ,page_visit_id,lead_or_contact_id  )
      , coalesce(t1.session_id, 'Null ')session_id
      , t1.visit_start_time
      , coalesce(t1.page_visit_id, 'Null ')     page_visit_id
      , coalesce(t1.lead_or_contact_id, 'Null') lead_or_contact_id
      , t2.campaign_type
      , t2.campaign_member_id campaign_member_id_2
      , t2.inquiry_created_ts inquiry_date_time_2
      , t2.tactic
      , case when campaign_type = 'Confluent Cloud Sign-Up' then 1 else 0 end flg_cloud_signup
      , case when campaign_type = 'Confluent Platform Download' then 1 else 0 end flg_cp_download
    from seg_with_lead_data t1
          join sfdc_inquiries_campaign_type t2   on t1.lead_or_contact_id = t2.lead_or_contact_id --left join
                                        and TIMESTAMP_DIFF(t2.inquiry_created_ts,t1.visit_start_time,day) >=-7
                                        and TIMESTAMP_DIFF(t2.inquiry_created_ts,t1.visit_start_time,day) <= 7
)

select  --LoD: domain,session_id, visit_start_time ,page_visit_id,lead_or_contact_id , campaign_member_id
distinct coalesce(date(date_trunc(t1.visit_start_time, week)) , date(date_trunc(im.visit_start_time, week))) visit_start_time_week
        , coalesce(date(date_trunc(t1.visit_start_time,month)), date(date_trunc(im.visit_start_time,month))) visit_month
        , DATE_SUB(DATE_TRUNC(DATE_ADD(coalesce(date(t1.visit_start_time), date(im.visit_start_time)), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) visit_month_enddate
        , coalesce( t1.domain            , im.domain )             domain
        , coalesce( t1.session_id        , im.session_id )         session_id
        , coalesce( t1.visit_start_time  , im.visit_start_time )   visit_start_time
        , coalesce( t1.page_visit_id     , im.page_visit_id )      page_visit_id
        , coalesce( t1.lead_or_contact_id, im.lead_or_contact_id)  lead_or_contact_id
        , coalesce( t1.campaign_member_id, im.campaign_member_id_2)campaign_member_id

        , coalesce( t1.visitor_id, im.visitor_id)visitor_id
        , coalesce( t1.url, im.url)url
        , coalesce( t1.url_cleaned, im.url_cleaned)url_cleaned
        , coalesce( t1.channel_detail, im.channel_detail)channel_detail
        , coalesce( t1.channel_aggregate, im.channel_aggregate)channel_aggregate
        , coalesce( t1.flg_new_vs_returned, im.flg_new_vs_returned)flg_new_vs_returned
        , coalesce( t1.url_section_1, im.url_section_1)url_section_1
        , coalesce( t1.url_section_2, im.url_section_2)url_section_2
        , coalesce( t1.url_section_3, im.url_section_3)url_section_3


        , coalesce( t1.rnk_page_visit, im.rnk_page_visit)rnk_page_visit
        , coalesce( t1.tactic, im.tactic) tactic
        , coalesce( t1.campaign_type, im.campaign_type)campaign_type

        , coalesce( t1.inquiry_date_time, im.inquiry_date_time_2)inquiry_date_time
        , t1.opportunity_id
        , t1.lead_or_contact_persona, t1.mql_date_time, t1.sal_date_time, t1.sql_date_time, t1.sqm_date_time, t1.sao_date_time, t1.delta_inqcreation_visit, t1.delta_mql_inq
        , t1.delta_sqm_inq, t1.delta_sao_inq, t1.flg_1d_inq,	t1.flg_7d_inq,	t1.flg_7d_mql,	t1.flg_14d_sqm,	t1.num_60d_sqm

        , im.flg_cloud_signup
        , im.flg_cp_download
        , TIMESTAMP('{{next_execution_date.isoformat()}}') as extract_date
       -- , TIMESTAMP(current_date()) as extract_date
from lead_metrics_pg t1
  full outer join inquiry_metrics im on t1.domain = im.domain
                                      and t1.session_id         = im.session_id
                                      and t1.visit_start_time   = im.visit_start_time
                                      and t1.page_visit_id      = im.page_visit_id
                                      and t1.lead_or_contact_id = im.lead_or_contact_id
                                      and t1.campaign_member_id = im.campaign_member_id_2
