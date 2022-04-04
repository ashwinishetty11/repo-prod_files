
 --create or replace table `datascience-222717.dwh_web.rpt_web_dashboard`  as
 
 with rpt_org as (
     select a.*, b.org_id   --here we are mapping lead to org_id
    from `datascience-222717.dwh_web.rpt_web_dashboard`a  
        left join (select distinct org_id, lead_or_contact_id from `data-sandbox-123.Workspace_Ashwini.org_to_lead_map_apr01` ) b
                             on a.lead_or_contact_id = b.lead_or_contact_id 
 )
  
     select t1.* ----here we are getting cloud metrics details at org_id level
            , t2.email_verified_at
            , (case when cast(t2.signed_up_at as date) >= date_sub(cast(t2.email_verified_at as date), interval 7 day) then 1 else 0 end) as email_verified_7d
            , t2.cluster_created_at
            , case when cast(t2.signed_up_at as date) >= date_sub(cast(t2.usage_at as date), interval 7 day) then t2.usage_at else null end as onboarding_activated_7d
            
            , product_active_at stg1_pao

            , stage2_product_active_at stg2_pao 

     from rpt_org t1 
     LEFT JOIN  rpt_c360.cc_funnel t2 on t1.org_id = t2.org_id
     left join (select distinct org_id, org_name, pqo_status, signed_up_at, first_pqo_date from rpt_ccloud.cc_payg_pqo )pqo on t1.org_id = pqo.org_id



