select count(*) from `data-sandbox-123.Workspace_Ashwini.pages_29mar`       --3635613
select count(*) from `data-sandbox-123.Workspace_Ashwini.seg_with_lead_data_29mar`  --3873690

select count(*) from`data-sandbox-123.Workspace_Ashwini.seg_pages_inquiry_29mar` --4236669

select count(*)  from `data-sandbox-123.Workspace_Ashwini.inquiry_metrics_29mar`  --648390
select count(*)  from `data-sandbox-123.Workspace_Ashwini.rpt_29mar` --4243447 

select *  from `data-sandbox-123.Workspace_Ashwini.inquiry_metrics_29mar` 
where campaign_member_id_2 not in (select distinct campaign_member_id from`data-sandbox-123.Workspace_Ashwini.seg_pages_inquiry_29mar`) 

select campaign_member_id, * from `data-sandbox-123.Workspace_Ashwini.rpt_29mar` where flg = 2 order by 1
select * from `data-sandbox-123.Workspace_Ashwini.seg_pages_inquiry_29mar` where campaign_member_id = '00v3a00001HJr27AAD'
select * from `data-sandbox-123.Workspace_Ashwini.inquiry_metrics_29mar`  where campaign_member_id_2 = '00v3a00001HJr27AAD'

select distinct campaign_member_id  from  `data-sandbox-123.Workspace_Ashwini.seg_pages_inquiry_29mar` 
where campaign_member_id not in (select distinct campaign_member_id_2 from `data-sandbox-123.Workspace_Ashwini.inquiry_metrics_29mar`  ) 

--LoD
select  session_id, visit_start_time ,page_visit_id  ,count(*)
from dwh_web.fct_page_visit_daily
group by 1,2,3
having count(*)>1
order by 3 desc

select  session_id, visit_start_time ,page_visit_id  ,count(*)
from `data-sandbox-123.Workspace_Ashwini.pages_29mar`  
group by 1,2,3
having count(*)>1
order by 3 desc
----
select source, lead_or_contact_id, anonymous_id ,count(*)
from dwh_web.lookup_sfdc_lead_to_segment
group by 1,2,3
having count(*)>1
order by 3 desc

select * from dwh_web.lookup_sfdc_lead_to_segment limit 30
----
select domain,session_id, visit_start_time ,page_visit_id,lead_or_contact_id  ,count(*)
from `data-sandbox-123.Workspace_Ashwini.seg_with_lead_data_29mar`
group by 1,2,3, 4,5
having count(*)>1
order by 3 desc

--0--0--0
select campaign_member_id,count(*)
from `data-sandbox-123.Workspace_Ashwini.inbound_24mar`
group by 1 
having count(*)>1
order by 3 desc

--0--0--0-0
select domain,session_id, visit_start_time ,page_visit_id,lead_or_contact_id , campaign_member_id ,count(*)
from `data-sandbox-123.Workspace_Ashwini.seg_pages_inquiry_29mar` 
group by 1,2,3, 4,5,6
having count(*)>1
order by 3 desc

--0--0--0--
select campaign_member_id,count(*)
from `datascience-222717.dwh_web.fct_inquiry_daily` 
group by 1 
having count(*)>1
order by 3 desc

--0--0--0--0--
select domain,session_id, visit_start_time ,page_visit_id,lead_or_contact_id , campaign_member_id_2 ,count(*)
from  `data-sandbox-123.Workspace_Ashwini.inquiry_metrics_29mar` 
group by 1,2,3, 4,5,6
having count(*)>1
order by 3 desc

--0--0--0--0--
select domain,session_id, visit_start_time ,page_visit_id,lead_or_contact_id ,   campaign_member_id ,count(*)
from  `data-sandbox-123.Workspace_Ashwini.rpt_24mar`
group by 1,2,3, 4,5,6
having count(*)>1
order by 1,2,3,4,5 desc

select * from `data-sandbox-123.Workspace_Ashwini.rpt_24mar` where domain is null

--

select * from  dwh_web.fct_inquiry_daily 
where campaign_member_id not in (select distinct campaign_member_id from rpt_leads_pipeline.inbound_lead_funnel where date(inquiry_date_time) >= '2021-01-01')


select * from  rpt_leads_pipeline.inbound_lead_funnel 
where date(inquiry_date_time) >= '2021-01-01' and  campaign_member_id not in (select distinct campaign_member_id from dwh_web.fct_inquiry_daily   )

select distinct inquiry_date_time from dwh_web.fct_inquiry_daily 

select min(inquiry_date_time) from rpt_leads_pipeline.inbound_lead_funnel 

select * from  `data-sandbox-123.Workspace_Ashwini.rpt_24mar` where campaign_member_id = '00v3a00001KDRmHAAX'
--1.
select domain,visitor_id, session_id, visit_start_time ,url ,count(*)
from `data-sandbox-123.Workspace_Ashwini.pages_24mar` 
group by 1,2,3,4,5
having count(*)>1
select * from `data-sandbox-123.Workspace_Ashwini.pages_24mar`  where session_id ='eae2f195-a58c-4316-8ab4-912ab2995aec' order by rnk_page_visit

select  distinct * 
from (select distinct * except(loaded_at, uuid_ts, received_at, sent_at, context_ip) 
        from `datascience-222717.confluentio_segment_prod.pages`) p 
where s_id ='c6b26f46-1ef6-4b39-b332-e7a469c159a5' 
order by timestamp

select distinct * from (select *   from `datascience-222717.confluentio_segment_prod.pages`) p where s_id ='f0af4979-a501-4a68-b2e4-6a243f5b0faa' order by timestamp

--2.

--3.
select * from `data-sandbox-123.Workspace_Ashwini.sfdc_lead_data_24mar`
where campaign_member_id is null 
seg_pages_inquiry

--4.
select * from  `data-sandbox-123.Workspace_Ashwini.seg_pages_inquiry_24mar` 
where campaign_member_id is null  and lead_or_contact_id is not null

select campaign_member_id , count(distinct lead_or_contact_id)
from `datascience-222717.dwh_web.rpt_web_dashboard`
group by 1
having count(distinct lead_or_contact_id)>1

select lead_or_contact_id,campaign_member_id, * from `datascience-222717.dwh_web.rpt_web_dashboard` where campaign_member_id = '00v3a00001K0pWXAAZ'

select lead_or_contact_id,* from`data-sandbox-123.Workspace_Ashwini.seg_pages_inquiry_29mar` where campaign_member_id = '00v3a00001K0pWXAAZ' 

select  lead_or_contact_id,*  from `data-sandbox-123.Workspace_Ashwini.inquiry_metrics_29mar` where campaign_member_id_2 = '00v3a00001K0pWXAAZ'


select sum(a)/count(distinct lead_or_contact_id ), avg(a), sum(a),count(distinct lead_or_contact_id )
from
(select lead_or_contact_id ,  count(distinct campaign_member_id) a  from `datascience-222717.dwh_web.rpt_web_dashboard`
where campaign_member_id is not null 
and date(date_trunc(visit_start_time,month)) = '2022-03-01'
group by 1)

select sum(a)/count(distinct lead_or_contact_id ), avg(a), sum(a),count(distinct lead_or_contact_id )
from
(select lead_or_contact_id ,  count(distinct campaign_member_id) a  from `datascience-222717.dwh_web.rpt_web_dashboard`
where campaign_member_id is not null 
and date(date_trunc(visit_start_time,month)) = '2022-01-01'
group by 1)
select sum(a)/count(distinct lead_or_contact_id ), avg(a), sum(a),count(distinct lead_or_contact_id )
from
(select lead_or_contact_id ,  count(distinct campaign_member_id) a  from `datascience-222717.dwh_web.rpt_web_dashboard`
where campaign_member_id is not null 
and date(date_trunc(visit_start_time,week)) = '2022-03-06'
group by 1)

select  count(distinct campaign_member_id) a  
from `datascience-222717.dwh_web.rpt_web_dashboard`
where --campaign_member_id is not null and
 date(date_trunc(visit_start_time,month)) = '2022-03-01'
group by 1
