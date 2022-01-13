-- dependancy on rpt_attribution.ga_to_marketo_map and rpt_attribution.marketo_to_sfdc_map


CREATE OR REPLACE TABLE rpt_attribution.sfdc_to_ga_map as --name yet to be decided on

with
  sfdc as (     
    -- // get the leads from sfdc
      select distinct   id lead_or_contact_id  
                , created_at first_appearance_at
        from fnd_sfdc.lead
    )
, ga_seg as (  
     -- // get the UNION of ga_cids from GA and Segment. If one GA_CID is present in both the sources, then source is considered as 'GA'
    select ga_cid 
            --case when count(*) > 1 then 'GA' else STRING_AGG(source) end  source
            , source
            , RANK() OVER (PARTITION BY ga_cid ORDER BY source ) AS rank,
    from
    (
        (      -- // get the clientIDs from GA
            SELECT distinct a.clientId ga_cid 
                , 'GA' as source    
            FROM fnd_ga.sessions a, UNNEST(hits) as hits  
        )
    UNION ALL
        (      -- // get the leads from sfdc
            SELECT distinct  ga_cid  
                , 'Segment' as source 
            from confluentio_segment_prod.identifies i 
        )
    )     
   -- group by 1 
  )

,ga_marketo as (    
  -- // fetching the mapping marketo_lead_Ids 
    select distinct ga_seg.ga_cid , map_ga_marketo.ga_client_id, map_ga_marketo.marketo_lead_id, ga_seg.source
    from (select * from ga_seg where rank = 1 and ga_cid is not null ) ga_seg  
        left join rpt_attribution.ga_to_marketo_map map_ga_marketo 
                                            on  ga_seg.ga_cid =  map_ga_marketo.ga_client_id 

  )
   -- // final mapping to SFDC lead IDs, we are filtering out rows where either SFDC_Lead_ID or GA_Cid is null
   
    SELECT distinct sfdc.lead_or_contact_id sfdc_lead_or_contact_id
          --  , map_mar_sfdc.sfdc_lead_id  
          --  , map_mar_sfdc.marketo_lead_id
            , ga_marketo.ga_client_id ga_client_id
            , ga_marketo.source
            , sfdc.first_appearance_at 
    FROM  sfdc   
        left join rpt_attribution.marketo_to_sfdc_map map_mar_sfdc 
                                on cast( sfdc.lead_or_contact_id as string)     = cast(map_mar_sfdc.sfdc_lead_id as string)
        left join ga_marketo  
                                on cast(map_mar_sfdc.marketo_lead_id as string) = cast(ga_marketo.marketo_lead_id as string)

    where (sfdc.lead_or_contact_id  is not null and ga_marketo.ga_client_id is not null)
