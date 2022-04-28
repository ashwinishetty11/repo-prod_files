--CREATE OR REPLACE TABLE rpt_cloud_signup_attr_v3.channel_attribution_linear_touch AS --165001
CREATE OR REPLACE TABLE `data-sandbox-123.Workspace_Ashwini.channel_attribution_linear_touch` AS 
with base_data as (
select distinct org_id,
  created_at,
  session_id,channel_detail,channel,channel_aggregate,
  '30d_web' attr_source,
  source channel_source,
  medium channel_medium,
  campaign channel_campaign , ga_client_id, url,hostname
from rpt_cloud_signup_attr_v3.org_to_ga
where delta_orgcreation_web_hour >6
and created_at >= '2021-04-01'),

web_with_last_touch as (
  select org_id,created_at,channel_detail,channel,channel_aggregate,attr_source,
    channel_source,channel_medium,channel_campaign, session_id, ga_client_id,  url,hostname
  from base_data
  union all 
  select org_id,created_at,channel_detail,channel,
  channel_aggregate,
  'signup_last_touch' as attr_source,
  inquiry_source,inquiry_medium,inquiry_campaign, 'Null_session_id', 'Null_ga_client_id', 'Null_url', 'Null_hostname'
  from rpt_cloud_signup_attr_v3.channel_attribution_last_touch
  where created_at >= '2021-04-01'
),

ranking_data as (
select org_id,
  count(*) num_channel
from web_with_last_touch
group by 1),

add_linear_credit as (
select *, 1/num_channel linear_credit
from web_with_last_touch 
left join ranking_data using (org_id)),

org_attribute as (select org_id,signed_up_at,
--commercial_model,
--channel marketplace_channel,
--sign_up_sales_influence,
case when cast(signed_up_at as date) >= date_sub(cast(usage_at as date), interval 7 day) then usage_at else null end as onboarding_activated_7d,
case when cast(signed_up_at as date) >= date_sub(cast(product_active_at as date), interval 14 day) then product_active_at else null end as production_activation_14d,
t2.*
from rpt_c360.cc_funnel t1
left join (
  select id,
    marketplace_partner,
    commercial_model,
    motion,
    sfdc_account_theater,
    sfdc_account_region,
    sfdc_account_billingcountry,
    sign_up_theater,
    sign_up_region,
    sign_up_country,
    sign_up_sales_influence,
    first_response_kafka_familiarity,
    first_response_objective,
    first_response_role,
    site_source,social_signup,social_connection
  from rpt_cc.organization
) t2 on t1.org_id = t2.id
where signed_up_at is not null)

select COALESCE(t1.org_id,t2.org_id) org_id,
  COALESCE(t2.signed_up_at,t1.created_at) created_at,
  COALESCE(channel_detail, 'No Referral') channel_detail,
  COALESCE(channel, 'No Referral') channel,
  COALESCE(channel_aggregate, 'No Referral') channel_aggregate,
  channel_source,channel_medium,channel_campaign,
  attr_source,
  num_channel,
  COALESCE(linear_credit,1) as linear_credit,
  marketplace_partner,
  commercial_model,
  motion,
  sfdc_account_theater,
  sfdc_account_region,
  sfdc_account_billingcountry,
  sign_up_theater,
  sign_up_region,
  sign_up_country,
  sign_up_sales_influence,
  first_response_kafka_familiarity,
  first_response_objective,
  first_response_role,
  site_source,social_signup,social_connection,
  onboarding_activated_7d,
  production_activation_14d
  , session_id, ga_client_id
  ,  url,hostname,
  case when onboarding_activated_7d is not null and linear_credit is not null 
    then linear_credit 
    when onboarding_activated_7d is not null and linear_credit is null
    then 1 else null end as linear_credit_oa,
  case when production_activation_14d is not null and linear_credit is not null
    then linear_credit 
    when production_activation_14d is not null and linear_credit is null
    then 1 else null end as linear_credit_pao,
from add_linear_credit t1
full join org_attribute t2 on t1.org_id = t2.org_id
