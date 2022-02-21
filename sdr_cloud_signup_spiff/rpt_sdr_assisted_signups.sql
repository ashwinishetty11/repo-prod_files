CREATE OR REAPLCE sdr_cloud_signup_spiff.rpt_sdr_assisted_signups

with lkup_manager_quarter_wise_grp_size as (
  select manager_name
        , count(distinct sdr_name) grp_size
  from sdr_cloud_signup_spiff.lkup_sdr_manager
  group by 1
  )
select a.*
        , lkup.sdr_id lkup_sdr_id 
        , lkup.sdr_name lkup_sdr_name
        , lkup.manager_id lkup_manager_id
        , lkup.manager_name lkup_manager_name
        , grp_size.grp_size lkup_manager_grp_size
from sdr_cloud_signup_spiff.sdr_assisted_signups a
    left join sdr_cloud_signup_spiff.lkup_sdr_manager lkup on a.ownerid = lkup.sdr_id
    left join lkup_manager_quarter_wise_grp_size grp_size  on grp_size.manager_name = lkup.manager_name

 
