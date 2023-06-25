
--testing queries

select  channel_detail
              -- case when io_anonymous_id is not null then '.io' when cloud_anonymous_id is not null then '.cloud' else null end ,
              , count(distinct org_id)
      from  `data-sandbox-123.Workspace_Ashwini.rpt_cloud_signup_attr_v3_rpt_cloud_signup_attr_v3_test_last_touch`  t1
      inner join ( select distinct id 
                        from `datascience-222717.rpt_cc.organization` 
                        where date_trunc(created_at, quarter) = '2022-07-01' 
                          and acquisition_channel = 'No Referral'
                          and motion ='Product-Led' and is_customer
            ) t2 on t1.org_id = t2.id
      where --channel_detail in( 'Other Referral' , 'Direct - because null utms') and
              date_trunc(created_at, quarter) = '2022-07-01'  
           --  and anonymous_id is null 
         -- and inquiry_created_at is  not  null  
      group by 1 order by 2 desc
