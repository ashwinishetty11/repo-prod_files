create or replace table `data-sandbox-123.Workspace_Ashwini.org_to_lead_map` as
WITH signup_info_from_mothership AS (
select cast(om.org_id as string) as org_id
         , u.id as user_id 
         , u.email
        ,COALESCE(u.created_at, om.created_at) AS signed_up_at
  from `datascience-222717.fnd_mothership.user` u 
  join `datascience-222717.fnd_mothership.org_membership` om
    on u.id = om.user_id
--order by 1

) 
, lead_contact AS (
    SELECT DISTINCT *
    FROM (
        SELECT id
            ,email 
            ,'lead' AS origin
            , created_at AS lead_create_ts
        FROM `datascience-222717.fnd_sfdc.lead` 
        WHERE is_deleted = FALSE

        UNION ALL
        
        SELECT COALESCE(l.id, c.id) AS id
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
 
, signup_map_to_leadid_first_lead AS (
    SELECT org_id
        , b.id AS lead_or_contact_id
        ,user_id
        ,a.email
        ,a.signed_up_at 
        ,origin
        ,lead_create_ts
        -- one user may have multiple leads chooser the first leads
        ,ROW_NUMBER() OVER (  PARTITION BY a.org_id, b.id, a.email ORDER BY signed_up_at,lead_create_ts  ) AS rnk_u
    FROM signup_info_from_mothership a
        LEFT JOIN lead_contact b  ON a.email = b.email
)
select * except(rnk_u) from signup_map_to_leadid_first_lead
where rnk_u = 1
order by 1
--[LoD: org_id, lead_or_contact_id, email]
