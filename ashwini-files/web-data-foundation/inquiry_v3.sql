
with inquiry as (
        select i.campaign_member_id campaign_member_id
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
        where DATE(created_ts) =  DATE('{{ next_execution_date.isoformat() }}')
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
)
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
        where DATE(timestamp) =  DATE('{{ next_execution_date.isoformat() }}')
)
 , inquiry_segment_map as (
        select campaign_member_id ,string_agg(distinct anonymous_id) anonymous_id, string_agg(distinct session_id)  session_id
        from
        (
            select i_sfdc.campaign_member_id, i_sfdc.lead_or_contact_id ,segment.anonymous_id, segment.session_id
                    , i_sfdc.inquiry_created_ts, segment.timestamp
            from inquiry i_sfdc
                  join `datascience-222717.dwh_web.lookup_sfdc_lead_to_segment` map on i_sfdc.lead_or_contact_id = map.lead_or_contact_id
                  join segment on map.anonymous_id = segment.anonymous_id
        )
        where ABS(date_diff(inquiry_created_ts,timestamp,  MINUTE )) <= 1440  --360
        group by 1
    )
, inq as (
    select inq.campaign_member_id campaign_member_id
                , inq.lead_or_contact_id
                , inq.inquiry_created_ts
                , inq.form_id
                , inq.lead_id
                , inq.contact_id
                , inq.new_lead_or_contact
                , l.lead_score
                , l.lead_first_routed_at
                , l.lead_routed
                , icf.sfdc_campaign_type
                , inq.campaign_name
                , icf.channel
                , icf.channel_aggregate
                , icf.channel_detail
                , inq.utm_campaign
                , inq.utm_medium
                , inq.utm_source
                , inq.tactic
                , inq.tactic_grouping
                , inq.opportunity_id
                , pg.pg_new_arr_amount_credit pg
                , map.anonymous_id
                , map.session_id

    from inquiry inq
        left join inquiry_channel_info icf  on inq.campaign_member_id = icf.inquiry_id
        left join lead l                    on inq.lead_or_contact_id = l.lead_id
        left join pg                        on inq.campaign_member_id = pg.inquiry_id
                                                    and inq.lead_or_contact_id = coalesce(pg.sfdc_lead_id, pg.contact_id)
                                                    and inq.opportunity_id = pg.opportunity_id
        left join inquiry_segment_map map   on inq.campaign_member_id = map.campaign_member_id
)

, map_lead_orgid as
    (
        select * except(rnk)
        from (
             select distinct org_id, lead_or_contact_id
                    , rank() over (partition by  lead_or_contact_id  order by m_signed_up_at ) rnk
            from `datascience-222717.rpt_cloud_signup_attr_v2.all_cloud_signup_map_to_sfdc`
            where lead_or_contact_id is not null
        )
       where rnk =1
)
, inq_org as (
    select f.* , lkup.org_id
    from inq f
        left join map_lead_orgid lkup on f.lead_or_contact_id = lkup.lead_or_contact_id
)

select a.*
    , b.first_throughput_at oa_status
    , c.email_verified_at
    , c.product_active_at
   , TIMESTAMP('{{next_execution_date.isoformat()}}')  extract_date
from inq_org a
    left join (
        select distinct a.id as org_id
             , a.name as org_name
             , a.first_throughput_at
        from datascience-222717.rpt_ccloud.organization a
        where first_throughput_at is not null
         ) b on a.org_id = b.org_id
    left join (
        select org_id
            , org_name
            , email_verified_at
            , product_active_at
            , usage_at
        from rpt_c360.cc_funnel
     ) c on a.org_id = c.org_id


