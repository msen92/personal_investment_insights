declare last_date date;

set last_date = (select max(date) from `msen-gcloud.classic_investment_tools.classic_investment_tools_summary`);

create or replace table `msen-gcloud.classic_investment_tools.classic_investment_tools_current_state`
as
with investment_tools as
(
    select 'dollar' as investment_tool
    union all
    select 'euro' as investment_tool
    union all
    select 'gold' as investment_tool
)
select
    b.date
    ,a.investment_tool
    ,case
        when investment_tool = 'dollar'
            then dollar_total_investment_in_tl
        when investment_tool = 'euro'
            then euro_total_investment_in_tl
        when investment_tool = 'gold'
            then gold_total_investment_in_tl
    end as total_investment_in_tl
    ,case
        when investment_tool = 'dollar'
            then total_assets_in_tl_from_dollar
        when investment_tool = 'euro'
            then total_assets_in_tl_from_euro
        when investment_tool = 'gold'
            then total_assets_in_tl_from_gold
    end as total_assets_in_tl
    ,case
        when investment_tool = 'dollar'
            then dollar_profit
        when investment_tool = 'euro'
            then euro_profit
        when investment_tool = 'gold'
            then gold_profit
    end as profit
from investment_tools a,`msen-gcloud.classic_investment_tools.classic_investment_tools_summary` b
where b.date = last_date
;
