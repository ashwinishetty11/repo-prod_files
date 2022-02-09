with
inquiry_channel_info as (
    select * from (
        select 
        distinct
            inquiry_id, 
            channel, 
            action, 
            sfdc_campaign_type, 
          --  marketing_group, 
            channel_aggregate, 
            channel_detail, 
            row_number() over (partition by inquiry_id order by interaction_date desc) as rn
        from 
            `datascience-222717.rpt_new_pg_attribution.new_pg_attribution_step2`  order by 1
    ) x 
    where rn = 1
)
select    i.campaign_member_id inquiry_id 
        , i.lead_or_contact_id
        , i.campaign_type form_id
        , i.lead_id 
        , i.contact_id
        , i.campaign_name camaign_type
        , i.tactic 
        , i.tactic_grouping 
        , i.utm_campaign
        , i.utm_medium
        , i.utm_source
        , i.created_ts inquiry_created_time

        , ici.channel
        , ici.channel_detail
        , ici.channel_aggregate
       
        , cm.FCRM__FCR_Net_New_Lead__c is_new_lead 
        , i.new_lead_or_contact 
        , cm.fcrm__fcr_opportunity__c opp_id

        , l.lsm_score lead_score
        , l.first_routed_at lead_first_routed_at
        , case when  l.first_routed_at is null then 'Yes' else 'No' end lead_routed
       
     --   , con.opportunityid opportunity_id_contactid     
     --   , map.opportunity_id opportunity_id_leadid
     --   , ifnull(map.opportunity_id, con.opportunityid) opportunity_id
      from (select * from `datascience-222717.rpt_marketing.inquiry` where created_ts >= '2022-01-01') i
        join `datascience-222717.fnd_sfdc.campaign_member` cm on i.campaign_member_id = cm.id
        left join fnd_sfdc.lead l on i.lead_or_contact_id = l.id
        left join inquiry_channel_info ici on ici.inquiry_id = i.campaign_member_id
