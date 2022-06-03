with credit_400 as (
    with org_with_400_credit as (
        select 
            distinct claimed_by_internal_org_id as org_id, 
            credit_expiration_date
        from `datascience-222717.rpt_c360.cc_promo_code`
           where promo_code = "FREETRIAL400"  
        )
    
        select 
            o.org_id, 
            o.credit_expiration_date,
            `rpt_ccloud.lc_billing_daily_agg`.date, 
            sum(bill_amount_usd) as bill_amount_usd,
            sum(sum(bill_amount_usd)) OVER(partition by o.org_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
        from org_with_400_credit o
        join `rpt_ccloud.lc_billing_daily_agg` 
            on o.org_id = `rpt_ccloud.lc_billing_daily_agg`.org_id
                and `rpt_ccloud.lc_billing_daily_agg`.metric_type = 'PromoCredit'
                and `rpt_ccloud.lc_billing_daily_agg`.commercial_model in ('PAYG', 'Commit') 
                and o.credit_expiration_date >= `rpt_ccloud.lc_billing_daily_agg`.date
            group by 1, 2, 3
        
)

select 
    distinct org_id, 
    * except (org_id)
from (
select 
    org_id, 
    `rpt_ccloud.organization`.name,
    date(`rpt_ccloud.organization`.created_at) as created_at,
    credit_expiration_date, 
    billing_method,
    date(first_production_active_at) as first_production_activation_date,
    case when billing_method = 'STRIPE' then coalesce(has_payment_method_attached, FALSE)
        when billing_method = 'MARKETPLACE' then TRUE
        when billing_method = 'MANUAL' then TRUE end as has_payment_method,
    min(case when abs(running_total) >= 100 then date end) over (partition by org_id) as date_reach_100,
    min(case when abs(running_total) >= 200 then date end) over (partition by org_id) as date_reach_200,
    min(case when abs(running_total) >= 300 then date end) over (partition by org_id) as date_reach_300,
    min(case when abs(running_total) >= 400 then date end) over (partition by org_id) as date_reach_400
from credit_400 
left join `rpt_ccloud.organization`  
    ON `rpt_ccloud.organization`.id = credit_400.org_id
LEFT JOIN `fnd_ccloud_payments.customers` 
    ON `rpt_ccloud.organization`.stripe_cust_id = `fnd_ccloud_payments.customers`.id
where `rpt_ccloud.organization`.motion = 'Product-Led'
    and created_at >= '2022-03-03 20:41:00 UTC'
    and is_customer
    and channel = 'Direct'
    and {{sign_up_sales_influence}}
    and {{channel_aggregate}}
    and {{channel_detail}}
    and {{account_theater}}
    and {{account_region}}
    and {{channel}}
    ) 
where date_reach_200 is not null
order by date_reach_200 desc
