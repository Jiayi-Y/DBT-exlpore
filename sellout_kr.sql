{{
    config(
        materialized = 'table'
        )
}}

{% set kwr_euro = 0.00073%}

with sellout_kr as (
    select 
    market_code,
    currency_code,
    division_code,
    signature_code,
    product_code,
    sellout_date,
    EXTRACT(DAYOFWEEK FROM sellout_date) sellout_weekday,
    sellout_year,
    sellout_month,
    sum(sellout_unit) units_daily,
    sum(sellout_gross_value_w_tax) RSP_amt,
    sum(net_sellout_value_w_tax) ASP_amt,
    sum()

    from(
        select
        market_code,
        currency_code,
        division_code,
        signature_code,
        product_code,
        sellout_date,
        EXTRACT(YEAR FROM sellout_date) sellout_year,
        EXTRACT(MONTH FROM sellout_date) sellout_month,
        sellout_unit,
        sellout_gross_value_w_tax,
        net_sellout_value_w_tax
        from  `apmena-onedata-dna-pd.sdds_sellout.fact_sellout_v1`
        WHERE sellout_date >= "2022-06-07" and net_sellout_value_w_tax > 0
    )
    group by market_code, currency_code, division_code, signature_code, product_code, sellout_year, sellout_month, sellout_date
    order by market_code, currency_code, division_code, signature_code, product_code, sellout_year, sellout_month ASC, sellout_date ASC
)

select *,
ASP_amt/units_daily as ASP,
RSP_amt/units_daily as RSP,
from sellout_kr
where units_daily <> 0
