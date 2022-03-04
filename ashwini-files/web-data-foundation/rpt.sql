--select * from `datascience-222717.dwh_web.fct_page_visit_daily`  --1251879
 
with seg_with_lead_data as (
        select date(date_trunc(visit_start_time,month)) visit_month,  
                        t1.*, t2.lead_or_contact_id
        from `datascience-222717.dwh_web.fct_page_visit_daily` t1
            INNER join ( select distinct anonymous_id
                                , lead_or_contact_id
                                , min(first_appearance_at) min_first_appearance_at
                         from `datascience-222717.dwh_web.lookup_sfdc_lead_to_segment`
                         group by 1,2
                        ) t2 
                on t2.anonymous_id = t1.visitor_id
              --      and TIMESTAMP_DIFF(min_first_appearance_at,vist_start_time,day) <= 1
        where visit_start_time >= '2021-01-01'
            and visit_start_time < '2022-02-01'
      --      and hostname in (  'www.confluent.io','docs.confluent.io','kafka-tutorials.confluent.io',
     --            'developer.confluent.io') -- to check - to add ashwini
        )
  , sfdc_lead_data as (
    select  distinct lead_or_contact_id,tactic,
      campaign_type,
      createddate inquiry_date_time,
      mql_date_time,
      sal_date_time,
      sql_date_time,
      sqm_date_time,
      sao_date_time,
      opportunity_id,
      lead_or_contact_persona
    from rpt_leads_pipeline.inbound_lead_funnel
    where createddate >= '2021-01-01'
    and lead_or_contact_id in ( select distinct lead_or_contact_id from seg_with_lead_data)
   -- order by 1
)
, seg_pages_inquiry as (   
  select t1.*, t2.* except(lead_or_contact_id)
      , TIMESTAMP_DIFF(t2.inquiry_date_time,t1.visit_start_time,day) delta_inqcreation_visit
      , TIMESTAMP_DIFF(t2.mql_date_time,t2.inquiry_date_time,day) delta_mql_inq
      , TIMESTAMP_DIFF(t2.sqm_date_time,t2.inquiry_date_time,day) delta_sqm_inq
      , TIMESTAMP_DIFF(t2.sao_date_time,t2.inquiry_date_time,day) delta_sao_inq
  from seg_with_lead_data t1
      left join sfdc_lead_data t2    on t1.lead_or_contact_id = t2.lead_or_contact_id
                                      and TIMESTAMP_DIFF(t2.inquiry_date_time,t1.visit_start_time,day) >=-7
                                      and TIMESTAMP_DIFF(t2.inquiry_date_time,t1.visit_start_time,day) <=7
)
, lead_metrics_pg as (
  select t1.* 
 --   , t1.lead_or_contact_id 
    , case when delta_inqcreation_visit >=0 and delta_inqcreation_visit <= 1 then 1 else 0 end flg_1d_inq -- use this for Web to lead Conv % 
    , case when delta_inqcreation_visit >=0 and delta_inqcreation_visit <= 7 then 1 else 0 end flg_7d_inq
    , case when delta_inqcreation_visit >=0 and delta_inqcreation_visit <= 1 and delta_mql_inq <=7 then 1 else 0 end flg_7d_mql
    , case when delta_inqcreation_visit >=0 and delta_inqcreation_visit <= 1 and delta_mql_inq <=7 and delta_sqm_inq <=14 then 1 else 0 end flg_14d_sqm  
    , case when delta_inqcreation_visit >=0 and delta_inqcreation_visit <= 1 and delta_mql_inq <=7 and delta_sqm_inq <=14
                                                                                                   and delta_sao_inq <=60 then 1 else 0 end num_60d_sqm   
  from seg_pages_inquiry t1
)

,  sfdc_inquiries_campaign_type as (
    -- should we bring the inquiry details from segment or sfdc?

  -- inquiry details from sfdc inquiry
    select  distinct form_id campaign_type
      , campaign_member_id
      , i.inquiry_created_ts
      , lead_or_contact_id
    from `datascience-222717.dwh_web.fct_inquiry_daily` i 
    where i.inquiry_created_ts >= '2021-01-01'
    and lead_or_contact_id in ( select distinct lead_or_contact_id from seg_with_lead_data)

    --inquiry details from segment form submission table
  /*
    select s_id session_id
        , anonymous_id
        , form_id 
        , case when form_id in (2835, 6067) then 'Confluent Cloud Signup'
             when form_id in (3109, 5922) then 'Confluent Platform Download'
             else null end campaign_type
        , timestamp      
    from `datascience-222717.confluentio_segment_prod.form_submission` 
    */
  )

  , inquiry_metrics as (
    select t1.*, t2.* except(lead_or_contact_id)
      , case when campaign_type = 'CP Download' then 1 else 0 end flg_cp_download
      , case when campaign_type = 'Confluent Cloud Platform' then 1 else 0 end flg_cp_download
    from seg_with_lead_data t1
        left join sfdc_inquiries_campaign_type t2    on t1.lead_or_contact_id = t2.lead_or_contact_id
                                        and TIMESTAMP_DIFF(t2.inquiry_created_ts,t1.visit_start_time,day) >=-7
                                        and TIMESTAMP_DIFF(t2.inquiry_created_ts,t1.visit_start_time,day) <= 7 
)
SELECT p.* 
      , t1.lead_or_contact_id
      , t1.tactic
      , t1.campaign_type
      , t1.inquiry_date_time
      , t1.mql_date_time
      , t1.sal_date_time
      , t1.sql_date_time
      , t1.sqm_date_time
      , t1.sao_date_time
      , t1.opportunity_id
      , t1.lead_or_contact_persona
FROM `datascience-222717.dwh_web.fct_page_visit_daily` p
  left join lead_metrics_pg t1 on p.url = t1.url and p.session_id = t1.session_id and p.visit_start_time = t1.visit_start_time


