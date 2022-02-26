------
select distinct lead_or_contact_id 
                    from rpt_cloud_signup_attr_v2.cloud_signup_touch_attr 
                    where email = 'soniyadeshmukh459@gmail.com'  
-----

--segment_signups source
  --  source 1:   FROM  confluentio_segment_prod.identifies  inner join  confluentio_segment_prod.form_submission


    select b.email,a.email, a.anonymous_id, b.lead_or_contact_id,   a.s_id, a.timestamp, a.tracking_utm_medium, a.tracking_utm_source
    from `datascience-222717.confluentio_segment_prod.identifies` a
        inner join `datascience-222717.dwh_web.lookup_sfdc_lead_to_segment` b on a.anonymous_id = b.anonymous_id
    where b.lead_or_contact_id in (select distinct lead_or_contact_id 
                                    from rpt_c360.cc_signup_attribution
                                    where channel_aggregate = 'No Referral'
                                        and signed_up_at >= '2022-01-30')

    order by    a.timestamp
    
    select b.email,  a.anonymous_id, b.lead_or_contact_id,   a.s_id, a.timestamp
    from (select * from `datascience-222717.confluentio_segment_prod.form_submission` WHERE form_id in (2835, 6067) ) a
        inner join `datascience-222717.dwh_web.lookup_sfdc_lead_to_segment` b on a.anonymous_id = b.anonymous_id
    where b.lead_or_contact_id in (select distinct lead_or_contact_id 
                                    from rpt_c360.cc_signup_attribution
                                    where channel_aggregate = 'No Referral'
                                        and signed_up_at >= '2022-01-25')
     order by    a.timestamp
    
    select *
    from `datascience-222717.dwh_web.lookup_sfdc_lead_to_segment` limit 100

--Source 2

--datascience-222717.confluentio_segment_prod.pages
select * from (
select b.email,  a.anonymous_id, b.lead_or_contact_id,   a.s_id, a.timestamp
    ,a.timestamp AS visit_time
    ,a.tracking_utm_campaign 
    ,a.tracking_utm_medium 
    ,a.tracking_utm_source 
    ,a.tracking_utm_term 
    from (select * from `datascience-222717.confluentio_segment_prod.pages` ) a
        inner join `datascience-222717.dwh_web.lookup_sfdc_lead_to_segment` b on a.anonymous_id = b.anonymous_id
    where b.lead_or_contact_id in (select distinct lead_or_contact_id 
                                    from rpt_c360.cc_signup_attribution
                                    where channel_aggregate = 'No Referral'
                                        and signed_up_at >= '2022-01-01')
     
    
) a where  (a.tracking_utm_campaign   is not null or
    a.tracking_utm_medium  is not null or
    a.tracking_utm_source  is not null or
    a.tracking_utm_term  is not null )
 order by    a.timestamp

select * from fnd_ccloud_ui.onboarding_sign_up_completed a
left join `datascience-222717.dwh_web.lookup_sfdc_lead_to_segment` b on  a.anonymous_id = b.anonymous_id --and a.email = b.email
where b.lead_or_contact_id in (select distinct lead_or_contact_id 
                                    from rpt_c360.cc_signup_attribution
                                    where channel_aggregate = 'No Referral'
                                        and signed_up_at >= '2022-01-25')
 limit 200
 -- source 3 - campaign memeber
  select * from (
    select    cm.leadorcontactid   ,cm.utm_campaign__c as utm_campaign
        ,cm.utm_medium__c as utm_medium
        ,cm.utm_source__c as utm_source, *
    FROM datascience-222717.fnd_sfdc.campaign c
    JOIN datascience-222717.fnd_sfdc.campaign_member cm
        ON c.id = cm.campaignid
    WHERE cm.leadid IS NOT NULL
      AND c.name LIKE '2018 - Global - Confluent Cloud Professional%'
      AND c.name <> '2018 - Global - Confluent Cloud Professional Active Users' --credit card add action
and  leadorcontactid in (select distinct lead_or_contact_id 
                                    from rpt_c360.cc_signup_attribution
                                    where channel_aggregate = 'No Referral'
                                        and signed_up_at >= '2022-01-01')

) t where utm_campaign is not null or utm_medium is not null or utm_source is not null 




--source 4

  SELECT interaction_time AS visit_time
    ,session_id
    ,a.ga_client_id
    ,sfdc_lead_id
    ,campaign_name
    ,source,medium
    ,digital_channel
    ,data_source
    ,tactic 
  FROM rpt_cloud_signup_attr_v2.cloud_user_digital_touch a 
  where  sfdc_lead_id in (select distinct lead_or_contact_id 
                                    from rpt_c360.cc_signup_attribution
                                    where channel_aggregate = 'No Referral'
                                        and signed_up_at >= '2022-01-10')
and interaction_time>'2022-01-10'

-------------------------------------
select *
 from rpt_c360.cc_signup_attribution
 where channel_aggregate = 'No Referral'
 and lead_or_contact_id = '00Q3a00000vihgJEAQ'

select * FROM rpt_cloud_signup_attr_v2.all_cloud_signup_map_to_sfdc where email = 'jigarpari@yahoo.com'

select * from rpt_cloud_signup_attr_v2.cloud_signup_touch_attr  where email like 'ravi.hackworks@gmail.com'
  
  select *
  FROM datascience-222717.confluentio_segment_prod.form_submission
  WHERE form_id in (2835, 6067) and s_id = '496971a2-6183-424c-8164-ecdf968ee008'

--source 1.1
    select *
    from `datascience-222717.confluentio_segment_prod.identifies` 
    where anonymous_id = '31566b07-3f71-417e-85be-97f9ce6e4520'

    select *
    from `datascience-222717.confluentio_segment_prod.pages` 
     where s_id = 'e783ce9e-c5da-4289-ac17-cfc7a4cb4b54'

--source 1.1
    select * 
    FROM datascience-222717.confluentio_segment_prod.form_submission
	WHERE form_id in (2835, 6067) 
     and anonymous_id = 'f6502867-2522-4f38-b492-f67d4818ce69'

--Source 2 - pages
--a lot of no-referral leads have a row here in pages with utm_spurce and medium, but they arent recorded in the segment_form_submission table
--datascience-222717.confluentio_segment_prod.pages
select * from (
select b.email,  a.anonymous_id, b.lead_or_contact_id,   a.s_id, a.timestamp
    ,a.timestamp AS visit_time
    ,a.tracking_utm_campaign 
    ,a.tracking_utm_medium 
    ,a.tracking_utm_source 
    ,a.tracking_utm_term 
    from (select * from `datascience-222717.confluentio_segment_prod.pages` ) a
        inner join `datascience-222717.dwh_web.lookup_sfdc_lead_to_segment` b on a.anonymous_id = b.anonymous_id
    where b.lead_or_contact_id in (select distinct lead_or_contact_id 
                                    from rpt_c360.cc_signup_attribution
                                    where channel_aggregate = 'No Referral'
                                        and signed_up_at >= '2022-01-01')
     
    
) a where  (a.tracking_utm_campaign   is not null or
    a.tracking_utm_medium  is not null or
    a.tracking_utm_source  is not null or
    a.tracking_utm_term  is not null )
 order by    a.timestamp



----------------------------------
source 4
with lead_contact AS (
    SELECT DISTINCT *
    FROM (
        SELECT id AS lead_or_contact_id
            ,email 
            ,'lead' AS origin
            , created_at AS lead_create_ts
        FROM `datascience-222717.fnd_sfdc.lead` 
        WHERE is_deleted = FALSE

        UNION ALL
        
        SELECT COALESCE(l.id, c.id) AS lead_or_contact_id
            ,c.email
            ,CASE WHEN l.converted_contact_id IS NULL 
                THEN 'contact' 
                ELSE 'lead' 
            END AS origin 
            ,COALESCE(l.created_at, c.createddate) AS lead_create_ts
        FROM datascience-222717.fnd_sfdc.contact c
        LEFT JOIN datascience-222717.fnd_sfdc.lead l 
            ON l.converted_contact_id = c.id
        WHERE isdeleted = FALSE
    ) a
)
, web_visit AS (
  SELECT interaction_time AS visit_time
    ,session_id
    ,ga_client_id
    ,sfdc_lead_id
    ,campaign_name
    ,source,medium
    ,digital_channel
    ,data_source
    ,tactic
  FROM rpt_cloud_signup_attr_v2.cloud_user_digital_touch
)

, web_visit_match AS (
	SELECT a.org_id
	    ,a.m_signed_up_at
	    ,wv.visit_time
	    ,wv.campaign_name AS web_channel_campaign
	    ,wv.source AS web_channel_source
	    ,wv.digital_channel AS web_channel
	    ,wv.medium AS web_channel_medium
	    ,wv.tactic AS web_channel_tactic
	    ,wv.data_source AS data_source
	    ,wv.session_id
	    ,TIMESTAMP_DIFF(m_signed_up_at, visit_time, minute) AS min_diff
        , wv.sfdc_lead_id
	 FROM web_visit wv 
        INNER JOIN lead_contact lc ON wv.sfdc_lead_id = lc.lead_or_contact_id
        INNER JOIN rpt_cloud_signup_attr_v2.all_cloud_signup_map_to_sfdc a  ON a.email = lc.email	
	 WHERE TIMESTAMP_DIFF(a.m_signed_up_at, visit_time, MINUTE) <= 360 
    --    AND TIMESTAMP_DIFF(a.m_signed_up_at, visit_time, MINUTE) >= -1
)
select distinct sfdc_lead_id--*
from web_visit_match
where web_channel_medium is not null and sfdc_lead_id in (select distinct lead_or_contact_id 
                                    from rpt_c360.cc_signup_attribution
                                    where channel_aggregate = 'No Referral'
                                        and signed_up_at >= '2022-01-30')
and visit_time > '2022-01-30'-- and 



