SELECT  c.name AS campaign_name,
      -- SM : add inquiry / campaign member id
        cm.id as campaign_member_id,
        c.startdate AS campaign_start_date,
        cm.createddate AS created_ts,
        -- Eva: add inquiry date column; which is the SOT for when inquiry happened
        cm.cfcr_inquiry_date_time__c as inquiry_date_time,
        cm.campaignid AS campaignid,
        CASE WHEN cm.leadid IS NULL THEN cm.contactid ELSE cm.leadid END AS lead_or_contact_id,
        cm.contactid AS contact_id,
        cm.leadid AS lead_id,
        cm.country AS country,
        cm.region_match__c AS region_match__c,
        c.theater__c AS campaign_theater,
        c.region_v2__c AS campaign_region,
        c.tactic, -- use campaign tactic from fnd_sfdc.campaign table
        c.actualcost, 
        c.format__c AS format,
        CASE WHEN cm.country = 'United States' THEN 'Americas'
			 WHEN cm.country LIKE '%Korea%' THEN 'APAC'
            WHEN m.theater IS NOT NULL THEN m.theater
            WHEN m.theater IS NULL AND cm.geo__c IN ('APAC', 'EMEA') THEN cm.geo__c
            WHEN m.theater IS NULL AND cm.geo__c IN ('North America','Latin America & Caribbean') THEN 'Americas'
            ELSE 'Other' END AS theater,
        CASE WHEN c.function__c IN ('Digital', 'Event', 'Field', 'Partner') THEN c.function__c
            ELSE 'Other' END AS campaign_function,
        CASE 
            WHEN c.team__c IN ('Corporate','Strategic Events') THEN 'Strategic Events'
            WHEN c.team__c IN ('Growth - Digital', 'Field Marketing', 'Partner', 'Growth - Lifecycle', 'Global Campaigns', 'Revenue Marketing Cloud Partner') THEN c.team__c
            ELSE 'Other' END AS campaign_team,
        c.campaign_type_v2__c AS campaign_type,
        cm.focus_account_campaign_member__c AS focused_account_lead,
        CASE WHEN cm.lead_matched_priority_account__c = true THEN 1 ELSE 0 END AS priority_account_lead,
      -- SM : add UTM Parameters for campaign member 
        cm.utm_medium__c as utm_medium, 
        cm.utm_campaign__c as utm_campaign, 
        cm.utm_source__c as utm_source, 
        row_number() over (partition by cm.id) as rn
FROM fnd_sfdc.campaign_member cm
      JOIN fnd_sfdc.campaign c ON cm.campaignid = c.id
      LEFT JOIN fnd_sfdc.lead l ON cm.leadid = l.id
      LEFT JOIN fnd_sfdc.contact ct ON cm.contactid = ct.id
      LEFT JOIN stg_mau.country_mapping m ON cm.country = m.country 
WHERE (l.is_deleted IS NULL OR l.is_deleted = false) AND
  (ct.isdeleted IS NULL OR ct.isdeleted = false) AND
  (NOT c.name like 'FCI%') AND
  (NOT c.name like '%Confluent Cloud Professional Active Users%') AND
  (NOT c.campaign_type_v2__c = 'Email Nurture') and 
  (NOT c.campaign_type_v2__c = 'Ungated Content')

order by 2

select count(*), count(distinct id) from fnd_sfdc.campaign_member
select * from fnd_sfdc.campaign_member limit 500

select * from fnd_sfdc.campaign_member limit 500
 
select * from confluentio_segment_prod.pages where s_id in (select distinct id from confluentio_segment_prod.experiment_viewed)


with form_abandonment as (
     select s_id session_id
        , anonymous_id visitor_id
        , user_id cloud_user_id
    --    , page_visit_id
        , event
        , event_text
        , form_id
        , is_form_hidden
        , form_fields_completed completed_form_fields
        , form_fields_empty empty_form_fields
        , last_active_input
        , timestamp   
        , context_page_url url               --           , *
     from `datascience-222717.confluentio_segment_prod.form_abandonment`   
)
, pages as (
     select distinct anonymous_id visitor_id, s_id session_id,url, timestamp, id
     from `datascience-222717.confluentio_segment_prod.pages` pages 
)
select f.*, p.id page_visit_id
from form_abandonment f 
        left join pages p on   f.visitor_id = p.visitor_id 
                           and f.session_id = p.session_id 
                           and f.url = p.url
                           and      f.timestamp >= DATE_ADD( p.timestamp, INTERVAL -2 MIN) 
                                and f.timestamp <= DATE_ADD( p.timestamp, INTERVAL  2 MIN )    

select * from `datascience-222717.confluentio_segment_prod.pages` where s_id = 'a0ec5dd6-f438-44bd-95a8-d780dca49e9a'
 --2021-12-08 18:59:00.546 UTC



   select s_id session_id
        , anonymous_id visitor_id
        , user_id cloud_user_id
        , component
    --    , page_visit_id
        , event
        , event_text
        , id click_id
        , timestamp    
        , href_url
        , location                       
     from `datascience-222717.confluentio_segment_prod.click`




---------------------------------------------------------------------- editor 2
select id inquiry_id, 
from fnd_sfdc.campaign_member
limit 200


select id inquiry_id, s_id session_id,form_id, leadid,tactic
from `datascience-222717.confluentio_segment_prod.form_submission` inquiries
Limit 200

select * from rpt_marketing.inquiry limit 200

select * from `datascience-222717.confluentio_segment_prod.form_submission` 
where s_id = '38928e08-3f82-4987-914b-618b019bcbdd'
limit 200


select * from `datascience-222717.confluentio_segment_prod.pages` limit 200

--------------------------------------------------------------------



select context_page_url, * from `datascience-222717.confluentio_segment_prod.form_abandonment` 
--where s_id = '38928e08-3f82-4987-914b-618b019bcbdd'
limit 200


