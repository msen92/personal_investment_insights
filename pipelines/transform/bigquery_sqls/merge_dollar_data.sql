declare min_date date;

set min_date = (select min(date) from `msen-gcloud.temp.dollar_yearly_data`);

merge into `msen-gcloud.classic_investment_tools.daily_rates_dollar` a
using `msen-gcloud.temp.dollar_yearly_data` b on a.date = b.date and a.date >= min_date
when matched then update set
buying_price = b.buying_price
when not matched then insert
(
    date
    ,buying_price
)
values
(
    b.date
    ,b.buying_price
);