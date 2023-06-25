--create or replace table `data-sandbox-123.Workspace_Ashwini.rpt_cloud_signup_attr_v3_rpt_cloud_signup_attr_v3_test_last_touch` as

with ranking_data as (
select *,
  row_number() over (partition by org_id order by delta_orgcreation_web_second asc) rn_web--change: ashwini tp add a case to rank rows that have utm values
from `data-sandbox-123.Workspace_Ashwini.rpt_cloud_signup_attr_v3_rpt_cloud_signup_attr_v3_org_to_segment_io_page_v2` -- rpt_cloud_signup_attr_v3.org_to_segment_io_page
  where delta_orgcreation_web_hour <=6 or delta_orgcreation_web_hour_cloud<=6
)  
 /*
 select distinct org_id from ranking_data where date_trunc(created_at, quarter) = '2023-01-01' and rn_web = 1 order by delta_orgcreation_web_hour desc
  --
 select  date_trunc(created_at, quarter)
  , count(distinct org_id) 
  , count( distinct case when coalesce(t1.identify_utm_campaign , t1.identify_utm_source,t1.identify_utm_medium
          ,   case when    context_page_referrer like '%docs.confluent.io%' 
                        or context_page_referrer like '%developer.confluent.io%' 
                        or context_page_referrer like '%confluent.io%' then null else context_page_referrer end
                 ) is not null then org_id else null end)
  ,  count( distinct case when coalesce(t1.identify_utm_campaign , t1.identify_utm_source,t1.identify_utm_medium , context_page_referrer) 
                   is not null then org_id else null end)
  ,  count( distinct case when  anonymous_id is not null then org_id else null end) anon_id
from ranking_data t1
group by 1 order by 1 desc
 */
, segment_source_data as (
select org_id,
	created_at seg_created_at,
  identify_utm_medium seg_medium,
  case when lower(identify_utm_source) in ( 'confluent.io','confluent.cloud','drift') then NULL else lower(identify_utm_source) end as seg_source,
  --tracking_utm_source seg_source,
	identify_utm_campaign seg_campaign,
  context_page_referrer
  , anonymous_id,cloud_anonymous_id,io_anonymous_id,segment_identifies_anonymous_id

from ranking_data
where rn_web = 1)
 /*
select count(distinct org_id) 
  , count( distinct case when coalesce(t1.seg_campaign , t1.seg_source,t1.seg_medium
          ,   case when    context_page_referrer like '%docs.confluent.io%' 
                        or context_page_referrer like '%developer.confluent.io%' 
                        or context_page_referrer like '%confluent.io%' then null else context_page_referrer end
                 ) is not null then org_id else null end)
  ,  count( distinct case when coalesce(t1.seg_campaign , t1.seg_source,t1.seg_medium , context_page_referrer) 
                   is not null then org_id else null end)
  ,  count( distinct case when  anonymous_id is not null then org_id else null end) anon_id
from segment_source_data t1
*/
, ga_ranking_data as (
select *,
  row_number() over (partition by org_id order by delta_orgcreation_web_second asc) rn_web
from rpt_cloud_signup_attr_v3.org_to_ga
where delta_orgcreation_web_hour <=6),

ga_source_data as (
select org_id,
	created_at ga_created_at,
  medium ga_medium,
  ga_ranking_data.source ga_source,
	campaign ga_campaign,
  channel_detail ga_channel_detail,
  channel_aggregate ga_channel_aggregate
  , ga_client_id,session_id,url,hostname
from ga_ranking_data
where rn_web = 1),

inquiry_source_data as (
select org_id,
  created_at inquiry_org_created_at,
	inquiry_created_at,
  case when lower(utm_medium) in ('confluent.io','confluent.cloud','drift', 'unknown','(unknown)', 'webchat') then NULL else lower(utm_medium) end as inquiry_medium,
  case when lower(utm_source) in ('confluent.io','confluent.cloud','drift','webchat','unknown','(unknown)') then NULL else lower(utm_source) end as inquiry_source,
  --lower(utm_source) inquiry_source,
	lower(utm_campaign) inquiry_campaign,

from rpt_cloud_signup_attr_v3.org_to_sfdc_signup
where rn = 1),

data_all_source as (
select * except(context_page_referrer),
  lower(COALESCE(seg_medium,inquiry_medium, ga_medium)) combined_medium,
  lower(COALESCE(seg_source,inquiry_source, ga_source)) combined_source,
  lower(COALESCE(seg_campaign,inquiry_campaign, ga_campaign)) combined_campaign,
  case when  context_page_referrer like '%docs.confluent.io%' 
          or context_page_referrer  like '%developer.confluent.io%' 
          or context_page_referrer  like '%confluent.io%' then 'NA' else context_page_referrer end  as context_page_referrer
from inquiry_source_data
full join segment_source_data using (org_id)
full join ga_source_data using (org_id)
order by 2 desc),
--


get_channel_detail as (
select *,
CASE
   WHEN combined_medium like '%paidsocial%' and lower(combined_source) LIKE '%facebook%' THEN 'Paid Social - Facebook'
   WHEN combined_medium like '%paidsocial%' and lower(combined_source) LIKE '%twitter%'  THEN 'Paid Social - Twitter'
   WHEN combined_medium like '%paidsocial%' and lower(combined_source) LIKE '%linkedin%' THEN 'Paid Social - LinkedIn'
   WHEN combined_campaign LIKE '%ch.quora%' Then 'Paid Social - Quora'

   WHEN combined_medium = 'ppc'and combined_campaign LIKE  'ch.sem_"%'  THEN 'Paid Search'
   WHEN(
                       combined_medium = 'sem'
                    or combined_medium like '%"sem"%'
                    or combined_medium like '%,sem%'
                    or combined_medium like '%sem,%'
                    or combined_medium like '%,sem,%'
                )
            AND (
                    combined_source = 'google'
                    or combined_source like '%"google"%'
                    or combined_source like '%,google'
                    or combined_source like '%google,%'
                    or combined_source like '%,google,%'
                )  THEN 'Paid Search - Google'
   WHEN (
                       combined_medium = 'sem'
                    or combined_medium like '%"sem"%'
                    or combined_medium like '%,sem%'
                    or combined_medium like '%sem,%'
                    or combined_medium like '%,sem,%'
                )
            AND (
                       combined_source = 'bing'
                    or combined_source like '%"bing"%'
                    or combined_source like '%,bing%'
                    or combined_source like '%bing,%'
                    or combined_source like '%,bing,%'
                )            THEN 'Paid Search - Bing'
   WHEN (
                       combined_medium = 'sem'
                    or combined_medium like '%"sem"%' or combined_medium like '%,sem%'  or combined_medium like '%sem,%'  or combined_medium like '%,sem,%'
                )                                           THEN 'Paid Search - Other'
   WHEN combined_medium  = 'gmail' THEN 'Paid Gmail'
   -- appending lifecycle and global email nurtures
   WHEN combined_medium  like '%nurtureemail%'
         AND combined_campaign like 'tm.lifecycle%'
                                         THEN 'Nurture - Lifecycle'
   WHEN combined_medium  like '%nurtureemail%'
         AND (combined_campaign like 'tm.global-campaigns%' OR
              combined_campaign like 'tm.campaigns%'
                                         ) THEN 'Nurture - Global Campaigns'
   WHEN combined_campaign like '%transform-your-business%' OR
        combined_campaign like '%reduce-your-tco%' THEN 'Nurture - Global Campaigns'
     -- identify vertical nurtures
   WHEN combined_campaign like '%finserv-then-and-now%' OR
        combined_campaign like '%ventana-research-finserv%' OR
        combined_campaign like '%kafka-summit-2020-keynote-finserv%' OR
        combined_campaign like '%idc-spotlight-on-modern-data-management-finserv%' OR
        combined_campaign like '%idc60-exec-snapshot-report-finserv%' OR
        combined_campaign like '%apache-kafka-and-google-cloud-finserv-ebook%' OR
        combined_campaign like '%ongoing-disruption-of-insurance%' OR
        combined_campaign like '%ventana-research-insurance%' OR
        combined_campaign like '%kafka-summit-2020-generali-insurance%' OR
        combined_campaign like '%idc-spotlight-on-modern-data-management-insurance%' OR
        combined_campaign like '%idc60-exec-snapshot-report-insurance%22%' OR
        combined_campaign like '%apache-kafka-and-google-cloud-insurance-ebook%' OR
        combined_campaign like '%ongoing-disruption-of-retail%' OR
        combined_campaign like '%ventana-research-retail%' OR
        combined_campaign like '%kafka-summit-2020-keynote-retail%' OR
        combined_campaign like '%idc-spotlight-on-modern-data-management-retail%' OR
        combined_campaign like '%idc60-exec-snapshot-report-retail%' OR
        combined_campaign like '%apache-kafka-and-google-cloud-retail-ebook%' THEN 'Nurture - Global Campaigns'
   WHEN combined_medium  LIKE '%email%' THEN 'Email'
   WHEN combined_source = '(direct)' THEN 'Direct'
   WHEN combined_medium = 'direct'  THEN 'Direct'
   WHEN combined_source IN ('gcp', 'aws', 'azure') THEN 'Partner Referral'
   WHEN combined_medium IN ('partner-referral', 'partner_referral') THEN 'Partner Referral'
   WHEN combined_source IN ('aws.amazon.com', 'cloud.google.com','azuremarketplace.microsoft.com')
            OR LOWER(combined_medium) LIKE 'partner%'
            OR LOWER(combined_source) LIKE 'partner%'
            OR LOWER(combined_source) LIKE 'aws_marketplace' THEN 'Partner Referral'
   WHEN combined_source IN ('webiste', 'confluent.cloud', 'drift') THEN 'Confluent website'
   WHEN combined_campaign LIKE '%type.community%' THEN 'DevX Referral'
   WHEN combined_campaign LIKE '%tm.devx%' THEN 'DevX Referral'
   WHEN combined_campaign LIKE '%tech-tip.community%' THEN 'DevX Referral'
   WHEN combined_campaign IN
   ('ch.client-wont-connect-to-kafka-cluster-in-docker-aws-brothers-laptop.community_content.clients',
    'ch.stream-processing-with-iot-data.community_content.stream-processing',
    'ch.kafka-summit-2020-day-1-recap_content.kafka-summit',
    'ch.mongodb-atlas-connector-in-secure-environments_content.connecting-to-apache-kafka',
    'ch.bounding-ksqldb-memory-usage_content.stream-processing'
     ) THEN 'DevX Referral'
   --ashwini's edit: bringing organic below paid
   --ashwini's edit
   WHEN  (lower(combined_source) LIKE '%facebook%' or context_page_referrer like '%facebook.com%' ) THEN 'Organic Social - Facebook' 
   WHEN  (lower(combined_source) LIKE '%twitter%'  or context_page_referrer like '%twitter%')  THEN 'Organic Social - Twitter'
   WHEN  (lower(combined_source) LIKE '%linkedin%' or context_page_referrer like '%linkedin%' or context_page_referrer like   '%lnkd.in%')  THEN 'Organic Social - LinkedIn'
   WHEN   context_page_referrer like '%instagram.com%' or  context_page_referrer like '%reddit.com%' THEN 'Organic Social - Other'

   WHEN combined_medium = 'display' and combined_campaign LIKE  'ch.display_%' THEN 'Display'
   
   WHEN ( 
           combined_source = 'google'
           or combined_source like '%"google"%'
           or combined_source like '%,google'
           or combined_source like '%google,%'
           or combined_source like '%,google,%'
        ) AND ((combined_campaign) NOT LIKE '%ch.%' or combined_campaign is null ) THEN 'Organic Search - Google'
   WHEN (
            ( combined_source = 'bing' 
                or combined_source like '%"bing"%'  or combined_source like '%,bing%' or combined_source like '%bing,%' or combined_source like '%,bing,%'
            )
            AND (combined_campaign NOT LIKE '%ch.%'  or combined_campaign is null )
        ) 
        or --ashwini's edit
        (context_page_referrer like '%bing%')  THEN 'Organic Search - Bing'
  --ashwini's edit: May17
   when context_page_referrer like '%www.google.%' then 'Organic Search - Google'
 
   WHEN (
            combined_source IN ('google', 'bing', 'baidu', 'duckduckgo') 
            or ( context_page_referrer like  '%duckduckgo%' or context_page_referrer like  '%yahoo.com%' )
        )
         AND (combined_campaign NOT LIKE '%ch.%' or combined_campaign is null )  THEN 'Organic Search - Other'
   WHEN context_page_referrer like '%youtube%' then 'Organic - Youtube' -- we might need to add a new channel aggregate bracket
   WHEN context_page_referrer like '%/kafka.apache.org/%' THEN 'Organic Search - Apache Kafka Website' -- 'Other Referral' --- new bracket
   WHEN combined_medium  = 'referral' THEN 'Other Referral'
   WHEN combined_source IS NOT NULL   THEN 'Other Referral'

   WHEN COALESCE(combined_source,combined_medium,combined_campaign) IS   NULL and  context_page_referrer = 'NA'  THEN 'Other Referral: referrer-acros-domains'
   WHEN COALESCE(combined_source,combined_medium,combined_campaign) IS   NULL and  context_page_referrer is not null  THEN 'Other Referral'

  --when (context_page_referrer <> 'NA' or context_page_referrer is null ) and COALESCE(combined_source,combined_medium,combined_campaign) IS NULL then 'Direct - because null utms'
   when (context_page_referrer <> 'NA' or context_page_referrer is null ) and COALESCE(combined_source,combined_medium,combined_campaign) IS NULL  and anonymous_id is not null
                then 'Direct - because null utms'
  when (context_page_referrer <> 'NA' or context_page_referrer is null ) and COALESCE(combined_source,combined_medium,combined_campaign) IS NULL 
                then 'No Referral'
   WHEN COALESCE(combined_source,combined_medium,combined_campaign) IS NOT NULL or (context_page_referrer <> 'NA' or context_page_referrer is not null ) THEN 'Other Referral'
   ELSE 'No Referral'
   end AS channel_detail
 
from data_all_source
),

get_channel_aggregate as (
select *,
CASE WHEN channel_detail IS NOT NULL THEN ifnull(rpt_cloud_signup_attr_v3.get_channel_from_detail(channel_detail), 'channel_detail') END AS channel,
CASE WHEN channel_detail IS NOT NULL THEN ifnull(rpt_cloud_signup_attr_v3.get_agg_from_detail(channel_detail), 'channel_detail')     END AS channel_aggregate,
COALESCE (inquiry_org_created_at,seg_created_at,ga_created_at) as created_at


from get_channel_detail)

select *,
  --COALESCE (inquiry_org_created_at,seg_created_at,ga_created_at) as created_at
from get_channel_aggregate;
