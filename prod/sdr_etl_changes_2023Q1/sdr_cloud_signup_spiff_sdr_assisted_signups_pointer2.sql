--CREATE OR REPLACE TABLE `datascience-222717.Workspace_AshwiniShetty.sdr_cloud_signup_spiff_sdr_assisted_signups_pointer2` AS--267972
WITH signup_leads AS (
  SELECT *
  FROM `datascience-222717.Workspace_AshwiniShetty.sdr_cloud_signup_spiff_sdr_assisted_signups_leads_pointer2`
  where org_id = '10204'--select * from sdr_cloud_signup_spiff.sdr_assisted_signups_leads
),

activity as (
    SELECT *
    FROM  Workspace_AshwiniShetty.sdr_activities_new--sdr_cloud_signup_spiff.sdr_activities
),

sdrs_assisted_signup as (--activity, lead_id
select s.org_id
    ,org_name
    ,signed_up_at
    ,a.task_owner_id
    ,coalesce(lead_id, contact_id) as lead_or_contact_id
    ,activitydate
    ,user_name
    ,manager_name
    ,a.is_csdr
    ,row_number() over ( partition by org_id order by activitydate ) as activity_rnk--check change
FROM signup_leads s
left join activity a
	ON coalesce(a.lead_id, a.contact_id) = s.lead_or_contact_id
where ((date_diff(date(signed_up_at), date(activitydate), day) <= 30 and signed_up_at > activitydate and lower(a.subject) like '%email%')
    or (date_diff(date(signed_up_at), date(activitydate), day) <= 30 and signed_up_at > activitydate and a.subject like '%[Call]%'))
)
--select org_id ,count(distinct lead_or_contact_id ) ,count(distinct task_owner_id ) from sdrs_assisted_signup
--group by 1 having count(distinct lead_or_contact_id )>1 order by 1

 --select * from sdrs_assisted_signup where org_id = '10204' order by org_id

,promo_signup_ordered as (
    select distinct a.intended_for_internal_org_id as org_id
        ,intended_for_internal_org_name as org_name
        ,promo_code
        ,promo_created_date
        ,b.id
        ,concat(b.firstname, " ", b.lastname) as user_name
        ,manager_name
        ,row_number() over (
            partition by intended_for_internal_org_id order by promo_created_date
        ) as promo_rnk
    from datascience-222717.rpt_c360.cc_promo_code a
    inner join datascience-222717.fnd_sfdc.user_daily b
        on a.created_by_email = b.email
        and date(a.promo_created_date) = date(_snapshot_ts)
    left join (
        select id, concat(firstname, " ", lastname) as manager_name, _snapshot_ts
        from datascience-222717.fnd_sfdc.user_daily
    ) c
        on b.managerid = c.id
        and date(b._snapshot_ts) = date(c._snapshot_ts)
    where promo_code_claim_status = "Claimed"
        and (a.promo_code_id != '4048' and a.promo_code != 'FREETRIAL400')
        and job_role__c IN ('Inbound SDR', 'Outbound SDR', 'cSDR')
)
  --select * from promo_signup_ordered where org_id = '10204'

,sdr_assisted_signups AS (--lod: lead_id, activity_date, promo_date/first_act_date
    SELECT distinct a.org_id
        ,a.org_name
        ,a.email
        ,a.lead_or_contact_id
        ,a.signed_up_at AS first_org_signup_at
        ,c.activitydate
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
    ) b
        ON a.org_id = b.org_id  
    LEFT JOIN (
        SELECT *
            ,"activity" AS assisted_orig
        FROM sdrs_assisted_signup
        --WHERE activity_rnk = 1
    ) c
        ON a.org_id = c.org_id  and a.lead_or_contact_id = c.lead_or_contact_id
   -- WHERE a.rnk = 1--check change
) 
        select * from sdr_assisted_signups where org_id = '10204' order by activitydate
