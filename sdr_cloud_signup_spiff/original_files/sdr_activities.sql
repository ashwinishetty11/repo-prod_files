CREATE OR REPLACE TABLE sdr_cloud_signup_spiff.sdr_activities AS

WITH task_all as 
(select  subject,
         whoid,
         ownerid,
         activitydate
from `datascience-222717.fnd_sfdc.task`
where (subject like '%[Email] [In]%' or  (subject like '%[Call]%'))
	and activitydate >= "2021-01-01"
union all
select  peopleai__activitytype__c as subject,
     whoid,
     ownerid,
     activitydate
from `datascience-222717.fnd_sfdc.task` t
where peopleai__direction__c = "Inbound"
    and peopleai__activitytype__c like "Received Email" 
	and activitydate >= "2021-01-01"
),

lead_contact AS (
    SELECT DISTINCT *
    FROM (
        SELECT a.id
            ,email 
            ,'lead' AS origin
            , created_at AS lead_create_ts
            ,a.theater
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
            ,c.account_theater as theater
            ,coalesce(a.region, c.region_new__c) as region
            ,a.subregion as subregion
        FROM `datascience-222717.fnd_sfdc.contact` c
        LEFT JOIN `datascience-222717.fnd_sfdc.lead` l 
            ON l.converted_contact_id = c.id
        left join datascience-222717.fnd_sfdc.account a
            on c.accountid = a.id
        WHERE c.isdeleted = FALSE
    ) a
),

activity as (
SELECT  
        subject, 
        coalesce(l2.id, l.id) as lead_id,  --- leads converted or not converted into contacts, capture orginal lead id or current lead id
        l.converted_contact_id, --- leads converted into contacts, capture orginal the converted contact id
        c.id  as contact_id,    ----- contacts, capture sales created contacts
        whoid, 
        activitydate, 
        employeenumber,
        t.ownerid as task_owner_id,
        case when date(activitydate) between "2020-11-15" and "2022-02-01"
                and u.id in ("0053a00000PnEpTAAV", "0053a00000PBO9vAAH", "0053a00000LAmiAAAT")
            then "cSDR" else u.job_role__c end as task_owner_role,
        u.name as user_name,
        manager_name
FROM task_all t
left join datascience-222717.fnd_sfdc.user_daily u 
    on t.ownerid = u.id 
    and date(activitydate) =  date(u._snapshot_ts)
left join datascience-222717.fnd_sfdc.lead l 
    on l.converted_contact_id = whoid
left join datascience-222717.fnd_sfdc.lead l2 
    on l2.id = whoid
left join datascience-222717.fnd_sfdc.contact c 
    on c.id = whoid
left join (
    select id, name as manager_name, _snapshot_ts 
    from datascience-222717.fnd_sfdc.user_daily
) d
    on u.managerid = d.id
    and date(u._snapshot_ts) = date(d._snapshot_ts)
WHERE u.job_role__c IN ('Inbound SDR', 'Outbound SDR', "cSDR")
	--AND t.ownerid NOT IN ("0053a00000PnEpTAAV", "0053a00000PBO9vAAH",  "0053a00000LAmiAAAT")
)

SELECT a.*
    ,theater
    ,region
    ,subregion
    ,case when task_owner_role = "cSDR" then 1 else 0 end as is_csdr
FROM activity a
left join lead_contact b
    on a.whoid = b.id
