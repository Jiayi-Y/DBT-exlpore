{{
    config(
        materialized = 'table'
        )
}}


with sellout_promo as(
    select *,
    case when promo<=0.95 then 1 else 0 end as promo_bool,
    lag(ASP_amt) over(partition by market_code, product_code order by sellout_date ) as sales_lag_1d,
    from(
        select *,
        ASP_amt/RSP_amt as promo
        from {{ ref('sellout_kr') }}
        )
    )


select *,
avg(sales_lag_1d) over (partition by market_code, product_code order by sellout_date) as pre_sales_avg,
from sellout_promo
where sales_lag_1d >= 0
