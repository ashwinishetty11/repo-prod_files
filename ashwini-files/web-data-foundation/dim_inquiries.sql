--version that I got reviewed by Eva on 10th Feb 2022

CREATE OR REPLACE TABLE dwh_web.dim_inquiry as

with inquiry as (
        select i.campaign_member_id inquiry_id
            , i.lead_or_contact_id
            , i.created_ts inquiry_created_ts
            , i.campaign_type form_id
            , i.lead_id
            , i.contact_id
            , i.campaign_name campaign_name
            , i.tactic
            , i.tactic_grouping
            , i.utm_campaign
            , i.utm_medium
            , i.utm_source
            , i.new_lead_or_contact
            , cm.fcrm__fcr_opportunity__c opportunity_id
            , date(cm.createddate) as inquiry_created_date_utc
        from `datascience-222717.rpt_marketing.inquiry`  i
            join `datascience-222717.fnd_sfdc.campaign_member` cm on i.campaign_member_id = cm.id
        where created_ts >= '2022-01-01'
)
, inquiry_channel_info as (
    select *
    from (
        select distinct inquiry_id
            , channel
            , action
            , sfdc_campaign_type
          --  marketing_group,
            , channel_aggregate
            , channel_detail
            , row_number() over (partition by inquiry_id order by interaction_date desc) as rn
        from  `datascience-222717.rpt_new_pg_attribution.new_pg_attribution_step2`
    ) x
    where rn = 1
)/*
, opportunity as (
    select inquiry.inquiry_id
        , cm.fcrm__fcr_opportunity__c opportunity_id
        , date(cm.createddate) as inquiry_created_date_utc
    from inquiry
        left join `datascience-222717.fnd_sfdc.campaign_member` cm on inquiry.inquiry_id = cm.id
    --where cm.date>=
)*/
, lead as (
    select id lead_id
        , lsm_score lead_score
        , l.first_routed_at lead_first_routed_at
        , case when  l.first_routed_at is null then 'Yes' else 'No' end lead_routed
    from fnd_sfdc.lead l
)
, pg as (
    select distinct inquiry_id,
          opportunity_id
        , sfdc_lead_id
        , contact_id
        , pg_new_arr_amount_credit
        , pg_new_arr_oppty_credit
        , opp_new_arr
        , case when opp_new_arr > 0 then 'Yes' else 'No' end as pg_attributed_inquiry
    from `datascience-222717.rpt_new_pg_attribution.new_pg_attribution`
)

, segment as (
        select distinct anonymous_id  --1145
                , s_id session_id
                , timestamp
        from `datascience-222717.confluentio_segment_prod.identifies` p
        where DATE(timestamp) >= '2022-01-01'
)
 , inquiry_segment_map as (
        select inquiry_id ,string_agg(distinct anonymous_id) anonymous_id, string_agg(distinct session_id)  session_id
        from
        (
            select i_sfdc.inquiry_id, i_sfdc.lead_or_contact_id ,segment.anonymous_id, segment.session_id
                    , i_sfdc.inquiry_created_ts, segment.timestamp
            from inquiry i_sfdc
                  join `datascience-222717.dwh_web.lookup_sfdc_lead_to_segment` map on i_sfdc.lead_or_contact_id = map.lead_or_contact_id
                  join segment on map.anonymous_id = segment.anonymous_id
        )
        where ABS(date_diff(inquiry_created_ts,timestamp,  MINUTE )) <= 1440  --360
        group by 1
 )
select inq.*, icf.channel
            , icf.sfdc_campaign_type
            , icf.channel_aggregate
            , icf.channel_detail
            , l.lead_score
            , l.lead_first_routed_at
            , l.lead_routed
            , pg.pg_new_arr_amount_credit pg
            , map.anonymous_id
            , map.session_id
from inquiry inq
    left join inquiry_channel_info icf  on inq.inquiry_id = icf.inquiry_id
 -- left join opportunity o             on inq.inquiry_id = o.inquiry_id
    left join lead l                    on inq.lead_or_contact_id = l.lead_id
    left join pg                        on inq.inquiry_id = pg.inquiry_id
                                                and inq.lead_or_contact_id = coalesce(pg.sfdc_lead_id, pg.contact_id)
                                                and inq.opportunity_id = pg.opportunity_id
    left join inquiry_segment_map map   on inq.inquiry_id = map.inquiry_id
