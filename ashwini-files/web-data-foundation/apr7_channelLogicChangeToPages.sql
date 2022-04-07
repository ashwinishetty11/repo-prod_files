--CREATE OR REPLACE TABLE dwh_web.fct_page_visit_daily as

with
   pages_confluent_io as
(
    select id page_visit_id
        , url url
        , s_id session_id
        , anonymous_id visitor_id
        , 'www.confluent.io' as domain
        , timestamp visit_date
        , timestamp visit_start_time
        , lead(timestamp) over (partition by anonymous_id,s_id order by timestamp) visit_end_time
        , row_number() over (partition by anonymous_id,s_id order by timestamp) rnk1_isentrance
        , row_number() over (partition by anonymous_id,s_id order by timestamp desc) rnk2_isexit
        , REPLACE(case when url not like '%?%'then url
                when ARRAY_REVERSE(split(url,'?'))[safe_offset(0)]  is not null
                    then replace (url,ARRAY_REVERSE(split(url,'?'))[safe_offset(0)],'')
                else url
            end ,'?', '') as url_cleaned

        , context_page_referrer
        , context_locale language

        , lower(tracking_utm_campaign) tracking_utm_campaign
        , case when lower(tracking_utm_campaign) is null then 'null_value' else 'not_null' end as utm_campaign_status

        , lower(tracking_utm_source) utm_source
        , case when tracking_utm_source in ('["google","GaggleAMP-Confluent"]','["google","google","google"]' , '["google","google"]', '["google","google"]'
                        , '["google,google","google,google","google,google,google,google","google,google,google,google,google,google,google,google"]'
                        , '["google,google","google,google","google,google,google,google"]'
                        , '["google,google","google,google"]'
                         ) THEN 'google'
                when tracking_utm_source in ('["linkedin","GaggleAMP-Confluent"]','["linkedin","linkedin"]' )   then 'linkedin'
                when tracking_utm_source in ('google', 'google,google', 'google[…]nEzmTUb1eHRd45332J6qQdX4sRFrSPGc3QXX7P0XDmgoDW8aAtcdEALw_wcB'
                                ,  '%google%')      then 'google'  
            else lower(tracking_utm_source)   end tracking_utm_source

        , lower(tracking_utm_medium)  utm_medium
        , case  when tracking_utm_medium in ( '["email","email"]' , 'e-mail') then 'email'
                when tracking_utm_medium in ( '["marketingemail","marketingemail"]'
                                , '["marketingemail\u003e  or unsubscribe \u003chttps://go.confluent.io/UnsubscribePage.html?mkt_unsubscribe=1","marketingemail"]'
                        )       THEN 'marketingemail'
                when tracking_utm_medium in ('["paidsocial",""]') then 'paidsocial'
                when tracking_utm_medium in ('["sem","sem"]', '["sem,sem","sem,sem","sem,sem,sem,sem","sem,sem,sem,sem,sem,sem,sem,sem"]'
                                , '["sem,sem","sem,sem","sem,sem,sem,sem"]'
                                , '["sem,sem","sem,sem"]'
                                , 'sem,sem'
                                , 'sem'
                                ) then 'sem'
                when tracking_utm_medium in ('landing_page', 'landingpage') then 'landingpage'      
                when tracking_utm_medium in ('marketingemai','marketingemail', 'marketingemail)', 'marketingemail> .' , 'marketingemail]'
                                , 'marketingemail].','marketingemail]]' , 'marketingemail__' , 'marketingemaile')   then 'marketingemail'   
                when tracking_utm_medium in ('nurtureemail','nurtureemail/' , 'nurtureemail^', 'nurtureemaill') then 'nurtureemail'
                when tracking_utm_medium in ('organic-social', 'organic_social','organicsocial' , 'organicsocial ') then 'organicsocial'
                when tracking_utm_medium in ('paid','paid_social', 'paidsocial' ) then 'paidsocial'
                else lower(tracking_utm_medium) end tracking_utm_medium
  
        , tracking_utm_term utm_term

        
      from (select distinct *  except(loaded_at, uuid_ts, received_at, sent_at, context_ip)
             from `datascience-222717.confluentio_segment_prod.pages` p
        ) pages
      where DATE(timestamp) >= '2021-10-01' --= DATE('{{ next_execution_date.isoformat() }}')
)/*
,  pages_docs_io as(
    select id page_visit_id
        , url url
     --   , s_id session_id -- s_id not found
        , anonymous_id visitor_id
        , 'docs.confluent.io' as domain
        , timestamp visit_date
        , timestamp visit_start_time
   --     , lead(timestamp) over (partition by anonymous_id,s_id order by timestamp) visit_end_time -- s_id not found
  --      , row_number() over (partition by anonymous_id,s_id order by timestamp) rnk1_isentrance
  --      , row_number() over (partition by anonymous_id,s_id order by timestamp desc) rnk2_isexit
        , REPLACE(case when url not like '%?%'then url
                when ARRAY_REVERSE(split(url,'?'))[safe_offset(0)]  is not null
                    then replace (url,ARRAY_REVERSE(split(url,'?'))[safe_offset(0)],'')
                else url
            end ,'?', '') as url_cleaned

        , context_page_referrer
        , context_locale language

        , lower(context_campaign_utm_campaign) tracking_utm_campaign
        , case when lower(context_campaign_utm_campaign) is null then 'null_value' else 'not_null' end as utm_campaign_status
        , lower(context_campaign_utm_source) utm_source
        , lower(context_campaign_utm_source) tracking_utm_source        -- ashwini shetty
        , lower(context_campaign_utm_medium)  utm_medium
        , lower(context_campaign_utm_source) tracking_utm_medium        -- ashwini shetty
        , context_campaign_utm_term utm_term

       
      from  (select distinct *  except(loaded_at, uuid_ts, received_at, sent_at, context_ip)
             from `datascience-222717.docs_rebrand.pages` p
        )  d_pages
      where DATE(timestamp) >= '2021-10-01' --= DATE('{{ next_execution_date.isoformat() }}')

)*/

 , pages_dev_io as(
    (select id page_visit_id
        , url url
        , s_id session_id
        , anonymous_id visitor_id
        , 'developer.confluent.io' as domain
        , timestamp visit_date
        , timestamp visit_start_time
        , lead(timestamp) over (partition by anonymous_id,s_id order by timestamp) visit_end_time
        , row_number() over (partition by anonymous_id,s_id order by timestamp) rnk1_isentrance
        , row_number() over (partition by anonymous_id,s_id order by timestamp desc) rnk2_isexit
        , REPLACE(case when url not like '%?%'then url
                when ARRAY_REVERSE(split(url,'?'))[safe_offset(0)]  is not null
                    then replace (url,ARRAY_REVERSE(split(url,'?'))[safe_offset(0)],'')
                else url
            end ,'?', '') as url_cleaned

        , context_page_referrer
        , context_locale language

       , lower(context_campaign_utm_campaign) tracking_utm_campaign
        , case when lower(context_campaign_utm_campaign) is null then 'null_value' else 'not_null' end as utm_campaign_status
        , lower(context_campaign_utm_source) utm_source
        , lower(context_campaign_utm_source) tracking_utm_source    --ashwini shetty
        , lower(context_campaign_utm_medium)  utm_medium
        , lower(context_campaign_utm_medium)  tracking_utm_medium --ashwini shetty
        , context_campaign_utm_term utm_term       
      from (select distinct *  except(loaded_at, uuid_ts, received_at, sent_at, context_ip)
             from confluentdev_segment_prod.pages p
        )    p3
      where DATE(timestamp) >= '2021-10-01' --= DATE('{{ next_execution_date.isoformat() }}')
    )/*
    UNION ALL
    (
        select id page_visit_id
        , url url
   --     , s_id session_id
        , anonymous_id visitor_id
        , 'developer.confluent.io/tutorials' as domain
        , timestamp visit_date
        , timestamp visit_start_time
   --     , lead(timestamp) over (partition by anonymous_id,s_id order by timestamp) visit_end_time
  --      , row_number() over (partition by anonymous_id,s_id order by timestamp) rnk1_isentrance
  --      , row_number() over (partition by anonymous_id,s_id order by timestamp desc) rnk2_isexit
        , REPLACE(case when url not like '%?%'then url
                when ARRAY_REVERSE(split(url,'?'))[safe_offset(0)]  is not null
                    then replace (url,ARRAY_REVERSE(split(url,'?'))[safe_offset(0)],'')
                else url
            end ,'?', '') as url_cleaned

        , context_page_referrer
        , context_locale language

       , lower(context_campaign_name) utm_campaign
        , case when lower(context_campaign_name) is null then 'null_value' else 'not_null' end as utm_campaign_status
        , lower(context_campaign_source) utm_source
        , lower(context_campaign_source) tracking_utm_source    --ashwini shetty
        , lower(context_campaign_medium)  utm_medium
        , lower(context_campaign_medium)  tracking_utm_medium --ashwini shetty
        , context_campaign_term utm_term
      from  (select distinct *  except(loaded_at, uuid_ts, received_at, sent_at, context_ip)
             from kafka_tutorials_prod.pages p
        ) p4
      where DATE(timestamp) >= '2021-10-01' --= DATE('{{ next_execution_date.isoformat() }}')
    )*/
)

, pages as (
        select * 
            , case when tracking_utm_campaign is null
                and (  context_page_referrer like '%www.google%'
                    or context_page_referrer like '%www.bing%'
                    or context_page_referrer like '%duckduckgo.com%'
                    or context_page_referrer like '%baidu.com%'
                    )                                                                    then 'organic_search'
               when tracking_utm_campaign is null and context_page_referrer is null      then 'direct'
             else lower(tracking_utm_medium)
          end as segment_utm_medium
        from pages_confluent_io

        UNION ALL

    /*  select * 
          , case when tracking_utm_campaign is null
                and (  context_page_referrer like '%www.google%'
                    or context_page_referrer like '%www.bing%'
                    or context_page_referrer like '%duckduckgo.com%'
                    or context_page_referrer like '%baidu.com%'
                    )                                                                             then 'organic_search'
               when tracking_utm_campaign is null and context_page_referrer is null       then 'direct'
             else lower(context_campaign_utm_medium)
            end as segment_utm_medium
        from pages_docs_io

        UNION ALL*/
        select * 
            ,  case when tracking_utm_campaign is null
                and (  context_page_referrer like '%www.google%'
                    or context_page_referrer like '%www.bing%'
                    or context_page_referrer like '%duckduckgo.com%'
                    or context_page_referrer like '%baidu.com%'
                    )                                                                    then 'organic_search'
               when tracking_utm_campaign is null and context_page_referrer is null      then 'direct'
             else lower(tracking_utm_campaign)
          end as segment_utm_medium
        from pages_dev_io
)
, pages_channel_detail as (
     SELECT distinct a.* 
         , CASE
              WHEN lower(segment_utm_medium)   = 'paidsocial' and lower(tracking_utm_source) LIKE '%facebook%' THEN 'Paid Social - Facebook'
              WHEN lower(segment_utm_medium)    = 'paidsocial' and lower(tracking_utm_source) LIKE '%twitter%'  THEN 'Paid Social - Twitter'
              WHEN lower(segment_utm_medium)   = 'paidsocial' and lower(tracking_utm_source) LIKE '%linkedin%' THEN 'Paid Social - LinkedIn'
              WHEN lower(tracking_utm_campaign) LIKE '%ch.quora%'                                               Then 'Paid Social - Quora'
              WHEN lower(tracking_utm_source)   LIKE '%facebook%'       THEN 'Organic Social - Facebook'
              WHEN lower(tracking_utm_source)   LIKE '%twitter%'        THEN 'Organic Social - Twitter'
              WHEN lower(tracking_utm_source)   LIKE '%linkedin%'       THEN 'Organic Social - LinkedIn'
              WHEN lower(segment_utm_medium)  = 'display'  and lower(tracking_utm_campaign) LIKE  'ch.display_%' THEN 'Display'
              WHEN lower(segment_utm_medium)  = 'ppc'      and lower(tracking_utm_source)   LIKE  'ch.sem_"%'    THEN 'Paid Search'
              WHEN lower(segment_utm_medium)  = 'sem'      AND lower(tracking_utm_source) = 'google'             THEN 'Paid Search - Google'
              WHEN lower(segment_utm_medium)  = 'sem'      AND lower(tracking_utm_source) = 'bing'               THEN 'Paid Search - Bing'
              WHEN lower(segment_utm_medium)  = 'sem'                                                            THEN 'Paid Search - Other'
              WHEN lower(segment_utm_medium)  = 'gmail'                                                          THEN 'Paid Gmail'
              -- appending lifecycle and global email nurtures
              WHEN   lower(segment_utm_medium)   = 'nurtureemail' AND lower(tracking_utm_campaign) like 'tm.lifecycle%'THEN 'Nurture - Lifecycle'
              WHEN   lower(segment_utm_medium)   = 'nurtureemail' AND (lower(tracking_utm_campaign) like 'tm.global-campaigns%' 
                  OR lower(tracking_utm_campaign) like 'tm.campaigns%' )                                                THEN 'Nurture - Global Campaigns'
              WHEN   lower(tracking_utm_campaign) like '%transform-your-business%' 
                  OR lower(tracking_utm_campaign) like '%reduce-your-tco%'                                              THEN 'Nurture - Global Campaigns'
                -- identify vertical nurtures
              WHEN lower(tracking_utm_campaign) like '%finserv-then-and-now%' OR
                   lower(tracking_utm_campaign) like '%ventana-research-finserv%' OR
                   lower(tracking_utm_campaign) like '%kafka-summit-2020-keynote-finserv%' OR
                   lower(tracking_utm_campaign) like '%idc-spotlight-on-modern-data-management-finserv%' OR
                   lower(tracking_utm_campaign) like '%idc60-exec-snapshot-report-finserv%' OR
                   lower(tracking_utm_campaign) like '%apache-kafka-and-google-cloud-finserv-ebook%' OR
                   lower(tracking_utm_campaign) like '%ongoing-disruption-of-insurance%' OR
                   lower(tracking_utm_campaign) like '%ventana-research-insurance%' OR
                   lower(tracking_utm_campaign) like '%kafka-summit-2020-generali-insurance%' OR
                   lower(tracking_utm_campaign) like '%idc-spotlight-on-modern-data-management-insurance%' OR
                   lower(tracking_utm_campaign) like '%idc60-exec-snapshot-report-insurance%22%' OR
                   lower(tracking_utm_campaign) like '%apache-kafka-and-google-cloud-insurance-ebook%' OR
                   lower(tracking_utm_campaign) like '%ongoing-disruption-of-retail%' OR
                   lower(tracking_utm_campaign) like '%ventana-research-retail%' OR
                   lower(tracking_utm_campaign) like '%kafka-summit-2020-keynote-retail%' OR
                   lower(tracking_utm_campaign) like '%idc-spotlight-on-modern-data-management-retail%' OR
                   lower(tracking_utm_campaign) like '%idc60-exec-snapshot-report-retail%' OR
                   lower(tracking_utm_campaign) like '%apache-kafka-and-google-cloud-retail-ebook%'                     THEN 'Nurture - Global Campaigns'

              WHEN lower(segment_utm_medium)   LIKE '%email%'                                                          THEN 'Email'
              WHEN lower(tracking_utm_source) = 'direct'                                                              THEN 'Direct'

              WHEN lower(tracking_utm_source) IN ('gcp', 'aws', 'azure')                                                THEN 'Partner Referral'
              WHEN lower(segment_utm_medium) IN ('partner-referral')                                                   THEN 'Partner Referral'
              WHEN lower(tracking_utm_source) IN ('aws.amazon.com', 'cloud.google.com','azuremarketplace.microsoft.com') 
                  OR LOWER(segment_utm_medium) LIKE 'partner%' 
                  OR LOWER(tracking_utm_source) LIKE 'partner%'                                                         THEN 'Partner Referral'

              WHEN lower(tracking_utm_source) IN ('webiste', 'confluent.cloud', 'drift')                                THEN 'Confluent website'
              WHEN lower(tracking_utm_campaign) LIKE '%type.community%'                                                 THEN 'DevX Referral'
              WHEN lower(tracking_utm_campaign) LIKE '%tm.devx%'                                                        THEN 'DevX Referral'
              WHEN lower(tracking_utm_campaign) LIKE '%tech-tip.community%'                                             THEN 'DevX Referral'
              WHEN lower(tracking_utm_campaign) IN
              ('ch.client-wont-connect-to-kafka-cluster-in-docker-aws-brothers-laptop.community_content.clients',
               'ch.stream-processing-with-iot-data.community_content.stream-processing',
               'ch.kafka-summit-2020-day-1-recap_content.kafka-summit',
               'ch.mongodb-atlas-connector-in-secure-environments_content.connecting-to-apache-kafka',
               'ch.bounding-ksqldb-memory-usage_content.stream-processing'
                )                                                                                                       THEN 'DevX Referral'

              WHEN lower(tracking_utm_source) IN ('google') AND (lower(tracking_utm_campaign)) NOT LIKE '%ch.%'         THEN 'Organic Search - Google'
              WHEN lower(tracking_utm_source) IN ( 'bing')  AND (lower(tracking_utm_campaign)) NOT LIKE '%ch.%'         THEN 'Organic Search - Bing'
              WHEN lower(tracking_utm_source) IN ('google', 'bing', 'baidu', 'duckduckgo')
                    AND lower(tracking_utm_campaign) NOT LIKE '%ch.%'                                                   THEN 'Organic Search - Other'
              WHEN lower(segment_utm_medium)   = 'referral'                                                            THEN 'Other Referral'
              WHEN lower(tracking_utm_source) IS NOT NULL THEN 'Other Referral'
              WHEN COALESCE(lower(tracking_utm_source),lower(segment_utm_medium) ,lower(tracking_utm_campaign)) IS NOT NULL THEN 'Other'
              ELSE 'No Referral'
              end AS channel_detail
    FROM pages a
)

, pages_channel_aggregate  as (
    SELECT distinct a.* ,
        CASE WHEN channel_detail IS NOT NULL THEN ifnull(Workspace_ShanSun.get_channel_from_detail(channel_detail), 'channel_detail') END AS channel,
        CASE WHEN channel_detail IS NOT NULL THEN ifnull(Workspace_ShanSun.get_agg_from_detail(channel_detail), 'channel_detail')     END AS channel_aggregate,
    FROM pages_channel_detail a
   )

select  p.session_id
        , p.visitor_id
        , p.url
        , url_cleaned
        , p.domain
        , p.visit_start_time
        , p.visit_end_time
        , date_diff(p.visit_end_time, p.visit_start_time, second) visit_length
        , p.page_visit_id
        , case  when split(p.url_cleaned,'/')[safe_offset(2)] like '?%' then '/homepage'
                when split(p.url_cleaned,'/')[safe_offset(2)] is null then '/homepage'
                when split(p.url_cleaned,'/')[safe_offset(2)] = '' then '/homepage'
                when split(p.url_cleaned,'/')[safe_offset(2)] like '%get-started%' then '/get-started'
                when split(p.url_cleaned,'/')[safe_offset(2)] like '%download%' then '/get-started'
                else concat('/',split(p.url_cleaned,'/')[safe_offset(2)])
            end as url_section_1

        , case  when split(p.url_cleaned,'/')[safe_offset(3)] like '?%' then '/homepage'
                when split(p.url_cleaned,'/')[safe_offset(3)] is null then '/homepage'
                when split(p.url_cleaned,'/')[safe_offset(3)] = '' then '/homepage'
                else concat('/',split(p.url_cleaned,'/')[safe_offset(3)])
            end as url_section_2

        , case  when split(p.url_cleaned,'/')[safe_offset(4)] like '?%' then '/homepage'
                when split(p.url_cleaned,'/')[safe_offset(4)] is null then '/homepage'
                when split(p.url_cleaned,'/')[safe_offset(4)] = '' then '/homepage'
                else concat('/',split(p.url_cleaned,'/')[safe_offset(4)])
            end as url_section_3
        , p.context_page_referrer
        , p.language
        , p.channel_detail
        , p.channel_aggregate
        , p.channel
        , p.tracking_utm_campaign
        , p.utm_source
        , p.utm_medium
        , p.tracking_utm_source
        , p.tracking_utm_medium
        , p.utm_term
        , p.rnk1_isentrance rnk_page_visit
        , p.rnk1_isentrance flg_is_entrance
        , p.rnk2_isexit flg_is_exit
        , case  when date(date_trunc(p.visit_start_time, month)) = date(date_trunc(v.first_visit_time, month)) then 'New'
                when date(date_trunc(p.visit_start_time, month)) > date(date_trunc(v.first_visit_time, month)) then 'Returned'
                else 'None' end flg_new_vs_returned
        , (case when p.rnk1_isentrance = 1 then 'True' else 'False' end) is_entrance
        , (case when p.rnk2_isexit = 1     then 'True' else 'False' end) is_exit
        , (case when p.rnk1_isentrance = 1 and p.rnk2_isexit = 1 then 'True' else 'False' end) is_bounce
      --  ,  TIMESTAMP('{{next_execution_date.isoformat()}}') as extract_date
        ,  TIMESTAMP(current_date) as extract_date
from pages_channel_aggregate p
    LEFT JOIN dwh_web.dim_visitor v on p.visitor_id = v.visitor_id
