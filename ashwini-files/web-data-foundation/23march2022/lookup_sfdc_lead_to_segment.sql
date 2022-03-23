
CREATE OR REPLACE TABLE dwh_web.lookup_sfdc_lead_to_segment as

with
  sfdc as (
      select distinct   id lead_or_contact_id,  email
        from fnd_sfdc.lead
    )

, segment as (
        SELECT anonymous_id
                , email
                , min(timestamp) first_appearance_at
       from confluentio_segment_prod.identifies
       group by 1, 2
   )
, cte as (
select  distinct sfdc.lead_or_contact_id lead_or_contact_id
            , segment.anonymous_id anonymous_id
            , sfdc.email email
            , segment.first_appearance_at first_appearance_at
            , 'confluent.io' source
from  (select * from sfdc where email is not null) sfdc
    inner join (select * from segment where email is not null)  segment on sfdc.email =  segment.email
)
select *,  TIMESTAMP(current_date()) as extract_date
from cte

