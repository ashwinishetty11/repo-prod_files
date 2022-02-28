SELECT * FROM  `sdr_cloud_signup_spiff.sdr_leads_csdr`
Limit 100

SELECT count(*), count(distinct  lead_or_contact_id)--, count(distinct org_id)
FROM `sdr_cloud_signup_spiff.sdr_leads_csdr`
----
SELECT distinct * FROM   sdr_cloud_signup_spiff.csdr_discovery_call order by 1
Limit 100
  
SELECT count(*), count(distinct  task_id)
FROM sdr_cloud_signup_spiff.csdr_discovery_call
----
SELECT * FROM sdr_cloud_signup_spiff.csdr_assisted_pqo
Limit 100
 
SELECT count(*), count(distinct  org_id)
FROM sdr_cloud_signup_spiff.csdr_assisted_pqo
----
SELECT org_id, count(*)
FROM `workspace_yliu.csdr_perf_monitoring_csdr_cloud_contribution`
group by 1
having count(*)>1
----
SELECT * FROM sdr_cloud_signup_spiff.sdr_assisted_pao
Limit 100

SELECT count(*),count(distinct org_id)
FROM  sdr_cloud_signup_spiff.sdr_assisted_pao

----
SELECT * FROM sdr_cloud_signup_spiff.csdr_assisted_pqo
Limit 100

SELECT count(*),count(distinct org_id)
FROM sdr_cloud_signup_spiff.csdr_assisted_pqo


