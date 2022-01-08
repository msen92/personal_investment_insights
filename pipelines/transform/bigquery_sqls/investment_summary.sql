declare start_date date;
declare end_date date;

set start_date = (select date(cast(extract(year from current_datetime('Europe/Istanbul')) - 1 as string)  || '-01-01'));
set end_date = (select date(current_datetime('Europe/Istanbul')));

merge into `msen-gcloud.classic_investment_tools.classic_investment_tools_summary` a
using
(
with dim_date as
(
    select
        a.investment_tool 
        ,b as date
    from
    (
        select 'dollar' as investment_tool,generate_date_array(start_date, end_date, INTERVAL 1 day) as dates
        union all
        select 'euro' as investment_tool,generate_date_array(start_date, end_date, INTERVAL 1 day) as dates
        union all
        select 'gold' as investment_tool,generate_date_array(start_date, end_date, INTERVAL 1 day) as dates
    ) a,
    unnest(dates) b
), investment_info as 
(
    select
        *
        ,a._table_suffix as table_suffix
        ,(
            select as struct 
                sum(buy_amount) as buy_amount
                ,sum(paid_amount_in_tl) as paid_amount_in_tl 
            from `msen-gcloud.classic_investment_tools.investments_*` b 
            where a.date >= b.date and a._table_suffix = b._table_suffix
        ) as investment
    from `msen-gcloud.classic_investment_tools.daily_rates_*` a
    where date between start_date and end_date
), filled_fields as
(
select
    a.date
    ,a.investment_tool 
    ,array_agg((select as struct b as investment_info) order by b.date desc limit 1)[safe_ordinal(1)] as investments
from dim_date a
left join investment_info b on a.date >= b.date and a.investment_tool = b.table_suffix
group by 1,2
),summary_calculated as
(
select
    a.date
    ,sum(if(investment_tool = 'dollar',investments.investment_info.buying_price,0))  as dollar_closing_price
    ,sum(if(investment_tool = 'dollar',investments.investment_info.investment.buy_amount,0))  as dollar_total_buy_amount
    ,sum(if(investment_tool = 'dollar',investments.investment_info.investment.paid_amount_in_tl,0))  as dollar_total_investment_in_tl
    ,sum(if(investment_tool = 'euro',investments.investment_info.buying_price,0))  as euro_closing_price
    ,sum(if(investment_tool = 'euro',investments.investment_info.investment.buy_amount,0))  as euro_total_buy_amount
    ,sum(if(investment_tool = 'euro',investments.investment_info.investment.paid_amount_in_tl,0))  as euro_total_investment_in_tl
    ,sum(if(investment_tool = 'gold',investments.investment_info.buying_price,0))  as gold_closing_price
    ,sum(if(investment_tool = 'gold',investments.investment_info.investment.buy_amount,0))  as  gold_total_buy_amount
    ,sum(if(investment_tool = 'gold',investments.investment_info.investment.paid_amount_in_tl,0))  as gold_total_investment_in_tl
from filled_fields a
group by a.date
)
select
    a.date
    ,a.dollar_closing_price
    ,a.dollar_total_buy_amount
    ,a.dollar_total_investment_in_tl
    ,a.dollar_total_buy_amount * a.dollar_closing_price as total_assets_in_tl_from_dollar
    ,(a.dollar_total_buy_amount * a.dollar_closing_price - a.dollar_total_investment_in_tl) as dollar_profit
    ,a.euro_closing_price
    ,a.euro_total_buy_amount
    ,a.euro_total_investment_in_tl
    ,a.euro_total_buy_amount * a.euro_closing_price as total_assets_in_tl_from_euro
    ,(a.euro_total_buy_amount * a.euro_closing_price - a.euro_total_investment_in_tl) as euro_profit
    ,a.gold_closing_price
    ,a.gold_total_buy_amount
    ,a.gold_total_investment_in_tl
    ,a.gold_total_buy_amount * a.gold_closing_price as total_assets_in_tl_from_gold
    ,(a.gold_total_buy_amount * a.gold_closing_price - a.gold_total_investment_in_tl) as gold_profit
from summary_calculated a 
) b on a.date = b.date
when matched then update set
     dollar_closing_price = b.dollar_closing_price
    ,dollar_total_buy_amount = b.dollar_total_buy_amount
    ,dollar_total_investment_in_tl = b.dollar_total_investment_in_tl
    ,total_assets_in_tl_from_dollar = b.total_assets_in_tl_from_dollar
    ,dollar_profit = b.dollar_profit
    ,euro_closing_price = b.euro_closing_price
    ,euro_total_buy_amount = b.euro_total_buy_amount
    ,euro_total_investment_in_tl = b.euro_total_investment_in_tl
    ,total_assets_in_tl_from_euro = b.total_assets_in_tl_from_euro
    ,euro_profit = b.euro_profit
    ,gold_closing_price = b.gold_closing_price
    ,gold_total_buy_amount = b.gold_total_buy_amount
    ,gold_total_investment_in_tl = b.gold_total_investment_in_tl
    ,total_assets_in_tl_from_gold = b.total_assets_in_tl_from_gold
    ,gold_profit = b.gold_profit
when not matched then insert
(
    date
    ,dollar_closing_price
    ,dollar_total_buy_amount
    ,dollar_total_investment_in_tl
    ,total_assets_in_tl_from_dollar
    ,dollar_profit
    ,euro_closing_price
    ,euro_total_buy_amount
    ,euro_total_investment_in_tl
    ,total_assets_in_tl_from_euro
    ,euro_profit
    ,gold_closing_price
    ,gold_total_buy_amount
    ,gold_total_investment_in_tl
    ,total_assets_in_tl_from_gold
    ,gold_profit
)
values
(
     b.date
    ,b.dollar_closing_price
    ,b.dollar_total_buy_amount
    ,b.dollar_total_investment_in_tl
    ,b.total_assets_in_tl_from_dollar
    ,b.dollar_profit
    ,b.euro_closing_price
    ,b.euro_total_buy_amount
    ,b.euro_total_investment_in_tl
    ,b.total_assets_in_tl_from_euro
    ,b.euro_profit
    ,b.gold_closing_price
    ,b.gold_total_buy_amount
    ,b.gold_total_investment_in_tl
    ,b.total_assets_in_tl_from_gold
    ,b.gold_profit
);