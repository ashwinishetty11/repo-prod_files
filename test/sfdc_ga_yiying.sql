--- sfdc lead and contact data
with lead_contact AS (
  SELECT DISTINCT *
  FROM (
      SELECT
        id as lead_or_contact_id
          ,email
          ,'lead' AS origin
          , created_at AS lead_create_ts
      FROM `datascience-222717.fnd_sfdc.lead`
      WHERE is_deleted = FALSE

      UNION ALL

      SELECT
          COALESCE(l.id, c.id) AS lead_or_contact_id
          ,c.email
          ,CASE WHEN l.converted_contact_id IS NULL THEN 'contact' ELSE 'lead' END AS origin
          ,COALESCE(l.created_at, c.createddate) AS lead_create_ts
      FROM `datascience-222717.fnd_sfdc.contact` c
        LEFT JOIN `datascience-222717.fnd_sfdc.lead` l
          ON l.converted_contact_id = c.id
      WHERE isdeleted = FALSE) a)

-- Pulls the first sign up associated with a lead
, first_signup as (
  select
    distinct a.* except(rnk)
    , b.lead_or_contact_id
  from (select u.email
                  ,om.org_id
                  ,om.user_id
                  ,om.created_at
                  ,rnk
          from `datascience-222717.fnd_mothership.user` u
          left join (select org_id
                            , user_id
                            , created_at
                            , row_number() over (partition by org_id order by created_at) as rnk
                     from `datascience-222717.fnd_mothership.org_membership`) om
            on u.id = om.user_id) a
  left join lead_contact b
    on a.email = b.email
  where rnk = 1),

-- Combines and aggregates visitor with inquiry data to build conversion rates
temp1 AS
  (select
      -- month
      date_trunc(date(b.vist_start_time), month) as visit_month
      -- domain
      , case when hostname in ('developer.confluent.io', 'kafka-tutorials.confluent.io') then 'developer.io'
        when hostname = 'www.confluent.io' then 'confluent.io'
        when hostname = 'docs.confluent.io' then 'docs'
        else 'other' end as domain
      --   --   --   --   --  web behavior
      -- uv
      , count(distinct b.ga_client_id) as num_uv
      -- new visitor
      , sum(case when b.is_new_visitor = 'New' then 1 else 0 end)/count(distinct b.ga_client_id) as percent_new_visitor
      -- avg pageview = total pageview/ number of unique visitors
      , sum(b.num_pageviews)/count(distinct b.ga_client_id) as avg_pageview
      -- converted leads: distinct lead/contact id
      , count(distinct case when c.tactic != 'Confluent Cloud Sign-Up' then c.lead_or_contact_id else null end) as num_converted_leads
      -- web to lead cov % = converted leads/number of unique visitors
      , count(distinct case when c.tactic != 'Confluent Cloud Sign-Up' then c.lead_or_contact_id else null end)/count(distinct b.ga_client_id) as web_to_lead_cvr
      --  --   --   --   --  product conversion
      -- cp cvr
      , count(distinct case when c.campaign_type in ( 'Confluent Platform Download', 'Confluent Community Download') then b.ga_client_id else NULL end) as CP_CC_inquiry
      , count(distinct case when c.campaign_type in ( 'Confluent Platform Download', 'Confluent Community Download') then b.ga_client_id else NULL end)/ count(distinct b.ga_client_id) as cp_cvr
     -- signup CVR
      , count(distinct d.lead_or_contact_id) as signup
      , count(distinct d.lead_or_contact_id) / count(distinct b.ga_client_id) as signup_cvr
  from
      -- Atrribution table which has additional fields available to parse data with --session data contain ga client id
      rpt_attribution.touchpoints_sessions b
      -- Mapping table ga_client_id to sfdc_lead_id use lookup_sfdc_lead_to_ga to connect sfdc with ga -- can know lead conversion
      left join dwh_web.lookup_sfdc_lead_to_ga a
           on b.ga_client_id = a.ga_client_id
      -- Inquiry data for all web inquiries besides Cloud sign ups and Hushly leads, need to figure out how to add Hushly leads here, confirm with MOPs & DS
      left join rpt_marketing.inquiry c
          on a.sfdc_lead_or_contact_id = c.lead_or_contact_id and c.created_ts >= b.vist_start_time and c.created_ts <= date_add(b.vist_start_time , INTERVAL 24 hour)
      -- Cloud Sign Up data -- additional cloud signup data
      left join first_signup d
          on a.sfdc_lead_or_contact_id = d.lead_or_contact_id and d.created_at >= b.vist_start_time and d.created_at <= date_add(b.vist_start_time , INTERVAL 24 hour)
      -- used to filter out confluent emails
      left join  (select distinct id,email FROM fnd_sfdc.lead) m
          ON a.sfdc_lead_or_contact_id = m.id
  where
      -- need to change for every report (report month and previous month)
      date(b.vist_start_time) >= '2021-08-01'
      and (m.email is NULL or m.email not like '%confluent.io%')
      and hostname in ('developer.confluent.io', 'www.confluent.io','docs.confluent.io', 'docs.ksqldb.io', 'www.kafka-summit.org' ,'ksqldb.io', 'events.confluent.io',
                       'careers.confluent.io', 'training.confluent.io', 'videos.confluent.io','assets.confluent.io', 'resources.confluent.io', 'kafka-tutorials.confluent.io', 'www.ksqldb.io')
  group by 1, 2)

select *
from temp1
where visit_month in ('2022-03-01', '2022-04-01')
order by domain, visit_month desc


