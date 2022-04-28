--165001
CREATE OR REPLACE TABLE `data-sandbox-123.Workspace_Ashwini.channel_attribution_linear_touch_join_orgtoga` AS 
select  t1.* except(channel_detail)    ,t2.org_id org_id_ga
   
    , t2.dev_section_grouped dev_section_grouped1
    , t2.production_activation_14d production_activation_14d1
    , t2.onboarding_activated_7d onboarding_activated_7d1
    , t1.channel_detail channel_detail_lt
    , t2.channel_detail channel_detail_ga 
    , t2.channel_aggregate channel_aggregate_ga
    , t2.channel channel_ga
    , cast (t2.delta_orgcreation_web_hour as int) delta_orgcreation_web_hour_ga
    , t2.hostname hostname_ga
    , coalesce(t1.channel_detail,t2.channel_detail) channel_detail_grouped
from  `data-sandbox-123.Workspace_Ashwini.channel_attribution_linear_touch` t1
    full outer join (select distinct * from rpt_cloud_signup_attr_v3.org_to_ga 
                 --     where delta_orgcreation_web_hour >6  and created_at >= '2021-04-01'
                      ) t2 
                                on t1.org_id = t2.org_id
                                        and t1.ga_client_id = t2.ga_client_id 
                                        and t1.session_id = t2.session_id 
                                        and t1.url = t2.url 
                                        and t1.hostname = t2.hostname
