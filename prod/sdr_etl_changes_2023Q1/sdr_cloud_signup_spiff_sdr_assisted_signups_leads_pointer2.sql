CREATE OR REPLACE TABLE `datascience-222717.Workspace_AshwiniShetty.sdr_cloud_signup_spiff_sdr_assisted_signups_leads_pointer2` AS
with user_signups as (
  select cast(om.org_id as string) as org_id,
         u.id as user_id,
         u.created_at as signed_up_at,
         email,
         row_number() over(partition by om.org_id order by u.created_at) as rn
  from `datascience-222717.fnd_mothership.user` u 
  join `datascience-222717.fnd_mothership.org_membership` om
    on u.id = om.user_id
),
org_signups as (
  select * except(rn)
    from user_signups
  -- where rn = 1--change
),--level of detail = org,user
ccp_orgs as (
   select distinct cast(o.id as string) as org_id,
          name as org_name,
          user_id,
          email,
          coalesce(os.signed_up_at, created_at) as signed_up_at
     -- there are org without user id information
     from `datascience-222717.fnd_mothership.organization` o
     left join org_signups os
       on os.org_id = cast(o.id as string)
    where o.is_customer = true
)--level of detail = org,user

     -- select org_id,user_id, count(*) from ccp_orgs group by 1,2 having count(*)>1
-- all lead email infor
 ,lead_contact AS (
    SELECT DISTINCT *
    FROM (
        SELECT a.id
            ,email 
            ,'lead' AS origin
            ,created_at AS lead_create_ts
            ,a.theater
            ,coalesce(b.area, c.area, a.area) as area
            ,coalesce(b.region, c.region, a.region) as region
            ,coalesce(b.subregion, c.subregion) as subregion
        FROM `datascience-222717.fnd_sfdc.lead` a
        left join datascience-222717.fnd_sfdc.account b
            on a.matched_acct_id = b.id
        left join datascience-222717.fnd_sfdc.account c
            on a.converted_acct_id = c.id
        WHERE a.is_deleted = FALSE

        UNION ALL
        
        SELECT COALESCE(l.id, c.id) AS id
            ,c.email
            ,CASE WHEN l.converted_contact_id IS NULL 
                THEN 'contact' 
                ELSE 'lead' 
            END AS origin 
            ,COALESCE(l.created_at, c.createddate) AS lead_create_ts
            ,coalesce(l.theater,c.account_theater) as theater
            ,coalesce(a.area, c.area) as area
            ,coalesce(a.region, c.region_new__c) as region
            ,a.subregion as subregion
        FROM `datascience-222717.fnd_sfdc.contact` c
        LEFT JOIN `datascience-222717.fnd_sfdc.lead` l 
            ON l.converted_contact_id = c.id
        left join datascience-222717.fnd_sfdc.account a
            on c.accountid = a.id
        WHERE c.isdeleted = FALSE
    ) a
)
 --select * from lead_contact where id= '00Q3a00000ruhReEAI'
,lead_merge AS (
    select l.name||', '||l.company as lkey
        , l.id deleted_leadid
        , l.email as deleted_email
        , lh.leadid master_leadid
        , ml.email as master_email
        , l.lastmodifieddate 
    from stg_sfdc.lead as l
    inner join stg_sfdc.lead_history lh 
        on (lh.oldvalue = l.name||', '||l.company 
            AND l.lastmodifieddate = lh.createddate)-- name, company concatentation composite key + created date / modified date
        and lh.field = 'leadMerged' -- only need to pull merge operations
    left join stg_sfdc.lead ml 
        on lh.leadid = ml.id
    where 
        l.isdeleted = true -- only care about delete records to join onto main query -- add into the different scenarios
        and ml.email != l.email
),

-- this is map leads to sign ups
-- one sign up may still map to multiple leads here
-- since one lead can be mqled multiple times
signup_email_update as (
    select distinct a.org_id
        ,org_name
        ,a.email as signup_email
        ,lm.master_email
        ,coalesce(lm.master_email,a.email) as merged_email
        ,lm.master_leadid
        ,signed_up_at
    from (
      SELECT *  ,row_number() over ( partition by org_id order by signed_up_at ) as rnk
      from ccp_orgs ) a
  LEFT JOIN lead_merge lm
    ON a.email = lm.deleted_email
  --where a.rnk = 1
)

,signup_leads AS (
  SELECT a.org_id
    ,org_name
    ,signup_email
    ,master_email
    ,merged_email as email
    ,coalesce(master_leadid, b.id) AS lead_or_contact_id
    ,signed_up_at
    ,theater
    ,area
    ,region
    ,subregion
  FROM signup_email_update a
  INNER JOIN lead_contact b
    ON a.merged_email = b.email
  -- lead must created before sign ups to be considered
  -- there are might be delays in salesforce, so lead time may be delayed
  WHERE TIMESTAMP_DIFF(signed_up_at, lead_create_ts, DAY) >= -1
)
--select org_id,coalesce(master_leadid, b.id) , count(*) from signup_email_update group by 1,2 having count(*)>1
, final as (
SELECT *, row_number() over (partition by org_id,lead_or_contact_id ) rnk
FROM signup_leads
)
select * except(rnk) from final where rnk =1
order by org_id  



