
with
segment as (
        select distinct anonymous_id , --SEGMENT: time range (date>= '2022-01-01') 37629 rows| 26289 anonIDs | 27612 sessions
                  s_id session_id
                , timestamp
        from `datascience-222717.confluentio_segment_prod.identifies` p
        where DATE(timestamp) >= '2022-01-01'
)
 , sfdc as (
         select distinct lead_or_contact_id ,  --SFDC: time range (date>= '2022-01-01') 80599 rows| 56629 Leads | 80599 inquiries
                     campaign_member_id inquiry_id
                   , created_ts
         from `datascience-222717.rpt_marketing.inquiry` i
         where DATE(created_ts) >= '2022-01-01'
 ) 
 select --distinct inquiry_id  , count(distinct anonymous_id ) 
       -- distinct inquiry_id--  , count(distinct session_id ) 
       *, date_diff(created_ts,timestamp,  MINUTE )
 from (
            select sfdc.inquiry_id, segment.session_id , sfdc.created_ts, segment.timestamp
                --sfdc.lead_or_contact_id ,segment.anonymous_id, segment.session_id, sfdc.created_ts, segment.timestamp
                --sfdc.inquiry_id , count(distinct segment.anonymous_id )
            from sfdc 
                join `datascience-222717.dwh_web.lookup_sfdc_lead_to_segment` map on sfdc.lead_or_contact_id = map.lead_or_contact_id
                join segment on map.anonymous_id = segment.anonymous_id
        )
where ABS(date_diff(created_ts,timestamp,  MINUTE )) <= 360
--group by 1 having count(distinct session_id )>1
order by 1 
