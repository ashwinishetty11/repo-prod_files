create or replace table `data-sandbox-123.Workspace_Ashwini.dwh_web_lookup_org_to_lead` as

WITH stg_signup_info_from_mothership AS (
    select cast(om.org_id as string) as org_id
            , u.id as user_id
            , u.email
            , COALESCE(u.created_at, om.created_at) AS signed_up_at
            , o.deactivated_at
            , o.is_customer
    from `datascience-222717.fnd_mothership.user` u
        inner join `datascience-222717.fnd_mothership.org_membership` om  on u.id = om.user_id
        left join  `datascience-222717.fnd_mothership.organization` o on om.org_id = cast(o.id as string)
    where  u.email NOT LIKE '%@serviceaccounts.confluent.cloud'
)
, signup_info_from_mothership as (
    select t1.*
    from stg_signup_info_from_mothership t1
     inner join `datascience-222717.rpt_cc.org_activation_milestones` o on t1.org_id = o.org_id -- rpt_cc.organization o
    where is_customer and motion = 'Product-Led'

)

--select * from signup_info_from_mothership where org_id = '10001'
, lead_contact AS (
    SELECT DISTINCT *
    FROM (
        SELECT id
            ,email
            ,'lead' AS origin
            , created_at AS lead_create_ts
        FROM `datascience-222717.fnd_sfdc.lead`
        WHERE is_deleted = FALSE or is_deleted is null

        UNION ALL

        SELECT COALESCE(c.id, l.id) AS id
            ,c.email
            ,'contact'  origin
            ,COALESCE(c.createddate, l.created_at) AS lead_create_ts
        FROM datascience-222717.fnd_sfdc.contact c
        LEFT JOIN datascience-222717.fnd_sfdc.lead l
            ON l.converted_contact_id = c.id
        WHERE isdeleted = FALSE or isdeleted is null
    ) a
)

, signup_map_to_leadid AS (
    SELECT org_id
        , b.id AS lead_or_contact_id
        , user_id
        , a.email
        , a.signed_up_at
        , origin
        , lead_create_ts
        , deactivated_at
        , is_customer
        -- here, we are choosing the first signup by a lead of a respective org. Therefore, we have info of all distinct leads who signed up from any org
        , ROW_NUMBER() OVER (  PARTITION BY a.org_id, b.id ORDER BY signed_up_at,lead_create_ts  ) AS rnk_u
        , ROW_NUMBER() OVER (  PARTITION BY a.org_id ORDER BY signed_up_at,lead_create_ts  ) AS rnk_is_first_signup

        , ROW_NUMBER() OVER (  PARTITION BY a.org_id, a.user_id, a.email, b.id ORDER BY signed_up_at, lead_create_ts  ) AS rnk_email
    FROM signup_info_from_mothership a
        LEFT JOIN lead_contact b  ON a.email = b.email
)
--[LoD: org_id,user_id, email, lead_or_contact_id]
select * except(rnk_u)
from signup_map_to_leadid
where rnk_email = 1

-- we want to map the org-id/ user-id to both - lead and contact ids
-- havent handled scenarios where same signup email is used by multiple organizations
