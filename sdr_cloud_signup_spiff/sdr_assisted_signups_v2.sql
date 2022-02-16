-- This code for sdr assitsted signups is very similar to the one present in production, except for the fact that here, we are using the excel input
-- sdr-to-manager mapping of this Quarter(provided by Ali). 
-- This will serve to the rpt needs of the first 3 views of the Dash as requested by Ali and also help us avoid the heavy self-joins on user_daily table.
-- However, if we were to have a trend of the sdr-assisted-cloud-signups and have a filter for manager_name across a time range of more a one quarter,
-- we will have to include a history table(although this request is not presently there)


--CREATE OR REPLACE TABLE sdr_cloud_signup_spiff.rpt_sdr_assisted_signups_v2  AS -- will make changes to table name later 

WITH signup_leads AS (
  SELECT *
  FROM sdr_cloud_signup_spiff.sdr_assisted_signups_leads
),

activity as (
    SELECT *
    FROM `data-sandbox-123.Workspace_Ashwini.sdr_cloud_signup_spiff_sdr_activities` -- activities_v2 table 
)

, sdrs_assisted_signup as (
    select s.org_id
        ,org_name
        , signed_up_at
        , a.task_owner_id
        , coalesce(lead_id, contact_id) as lead_or_contact_id
        , activitydate
        , user_name
        , a.manager_name
        , a.user_id -- added by ashwini 02/16/22
        , a.is_csdr
        ,row_number() over ( partition by org_id order by activitydate ) as activity_rnk
    FROM signup_leads s
        left join activity a  ON coalesce(a.lead_id, a.contact_id) = s.lead_or_contact_id 
    where ((date_diff(date(signed_up_at), date(a.activitydate), day) <= 30  and signed_up_at > a.activitydate and lower(a.subject) like '%email%')
        or (date_diff(date(signed_up_at), date(a.activitydate), day) <= 15  and signed_up_at > a.activitydate and a.subject like '%[Call]%')) 
  )

, promo_signup_ordered as (
    select distinct a.intended_for_internal_org_id as org_id
        ,intended_for_internal_org_name as org_name
        ,promo_code
        ,promo_created_date
        ,b.id
        ,concat(b.firstname, " ", b.lastname) as user_name
        ,lkup.manager_Name manager_name
        ,row_number() over ( partition by intended_for_internal_org_id order by promo_created_date) as promo_rnk
    from datascience-222717.rpt_c360.cc_promo_code a
        inner join datascience-222717.fnd_sfdc.user_daily b on a.created_by_email = b.email
                                                              and date(a.promo_created_date) = date(_snapshot_ts)
         left join `data-sandbox-123.Workspace_Ashwini.lkup_map_sdr_manager_FY2022Q1` lkup on b.name = lkup.sdr_name 
                                                                                --b.id = lkup.sdr_id    -- added by ashwini                                                 
    where promo_code_claim_status = "Claimed"
        and (a.promo_code_id != '4048' and a.promo_code != 'FREETRIAL400')
        and job_role__c IN ('Inbound SDR', 'Outbound SDR', 'cSDR')
),

sdr_assisted_signups AS (
    SELECT a.org_id
        ,a.org_name
        ,a.email
        ,a.lead_or_contact_id
        ,a.signed_up_at AS first_org_signup_at
        ,c.is_csdr
        ,CASE WHEN date(b.promo_created_date) <= date(c.activitydate) THEN date(b.promo_created_date) ELSE date(c.activitydate) END AS first_act_date
        ,CASE WHEN date(b.promo_created_date) <= date(c.activitydate) THEN b.user_name ELSE c.user_name END AS user_name
        ,CASE WHEN date(b.promo_created_date) <= date(c.activitydate) THEN b.manager_name ELSE c.manager_name END AS manager_name
        ,CASE WHEN date(b.promo_created_date) <= date(c.activitydate) THEN b.id ELSE c.task_owner_id END AS ownerid
        ,CASE WHEN date(b.promo_created_date) <= date(c.activitydate) THEN b.assisted_orig ELSE c.assisted_orig END AS assisted_orig
    FROM (
            SELECT *
                ,row_number() over ( partition by org_id order by signed_up_at  ) as rnk
            from signup_leads
        ) a
        LEFT JOIN (
            SELECT *
                 ,"promo_code" AS assisted_orig
            FROM promo_signup_ordered
            WHERE promo_rnk = 1
        ) b                         ON a.org_id = b.org_id
        LEFT JOIN (
            SELECT *
                 ,"activity" AS assisted_orig
            FROM sdrs_assisted_signup
            WHERE activity_rnk = 1
        ) c                         ON a.org_id = c.org_id
    WHERE a.rnk = 1
), 

signup_order as (
    select org_id
         ,row_number() over (  order by first_org_signup_at  ) as signup_rnk
    from sdr_assisted_signups
    where first_org_signup_at >= TIMESTAMP "2021-08-31 16:00:00 America/Los_Angeles"
        and ownerid is not null
        and is_csdr != 1
),

org_region_info as (
    select distinct c.id as org_id
        ,c.sfdc_account_theater
        ,c.sfdc_account_region
        ,acc.subregion
    from datascience-222717.rpt_ccloud.organization c
    left join datascience-222717.fnd_sfdc.account acc
        on c.sfdc_account_id = acc.id
)

select a.*
    ,sfdc_account_theater
    ,sfdc_account_region
    ,subregion
    ,b.signup_rnk
    ,case when EXTRACT(QUARTER from date_sub(current_date(), interval 10 day)) = EXTRACT(QUARTER from first_org_signup_at) 
                AND EXTRACT(YEAR from date_sub(current_date(), interval 10 day)) = EXTRACT(YEAR from first_org_signup_at) 
            then 1 else 0 
          end as is_signup_current_quarter
    ,case when ownerid in ("0053a00000PBIGHAA5", "0053a00000PPTDiAAP", "0053a00000PB4u3AAD", "0053a00000PnCfMAAV","0053a00000PnScvAAF","0053a00000PnSbiAAF","0053a00000PnW6eAAF") 
            then "Jacquelyn Yeow"
          else manager_name 
         end as manager_name_adj
from sdr_assisted_signups a
    left join signup_order b on a.org_id = b.org_id
    left join org_region_info ori on a.org_id = ori.org_id
;
