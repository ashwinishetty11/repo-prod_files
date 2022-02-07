select    campaign_member_id inquiry_id
        --session_id 
        , lead_or_contact_id
        , lead_id 
        , contact_id
        , camaign_type form_id
        , campaign_name camaign_type
        , tactic
        , tactic_grouping
        , utm_campaign
        , utm_medium
        , utm_source
        , creted_ts inquiry_datetime

        , channel_detail
        , channel
        , channel_aggregate

        , cm.FCRM__FCR_Net_New_Lead__c is_new_lead 
        , l.lsm_score lead_score
        , l.first_routed_at lead_routed

        , con.opportunityid
        , map.opportunity_id
      from `datascience-222717.rpt_marketing.inquiry` i
        join `datascience-222717.fnd_sfdc.campaign_member` cm on i.campaign_member_id = cm.id
        join fnd_sfdc.lead l on i.lead_or_contact_id = l.id
        join fnd_sfdc.opportunity_contact_role con on i.contact_id = con.id
        join rpt_attribution.marketo_to_sfdc_map map on i.lead_id = map.sfdc_lead_id
      where timestamp >= '2022-01-23'


with
segment as (
        select distinct anonymous_id  --1145
                , s_id session_id
                , timestamp
        from `datascience-222717.confluentio_segment_prod.identifies` p
        where DATE(timestamp) = '2022-01-24'
)
 , sfdc as (
         select distinct lead_or_contact_id     --1515
                 , campaign_member_id inquiry_id
                 , created_ts
         from `datascience-222717.rpt_marketing.inquiry` i
         where DATE(created_ts) = '2022-01-24'
 ) 
select-- sfdc.inquiry_id, sfdc.lead_or_contact_id ,segment.anonymous_id, segment.session_id, sfdc.created_ts, segment.timestamp
sfdc.inquiry_id , count(distinct segment.session_id )
from sfdc 
         join `datascience-222717.dwh_web.lookup_sfdc_lead_to_segment` map on sfdc.lead_or_contact_id = map.lead_or_contact_id
         join segment on map.anonymous_id = segment.anonymous_id
group by 1 having count(distinct segment.session_id )>1
order by 1
--1247 distinct leads were reduced to 600 after join with mapping table, and 501 after join with segment table
--1515 sfdc inquiries were reduced to 752 after the join , and 638 after join with segment table
-- around 21/ 638 inquiries have a mapping to more than one session_id, this will split the inquiry-level data in our final table,
   -- making the LoD [inquiry_id,s_id]
