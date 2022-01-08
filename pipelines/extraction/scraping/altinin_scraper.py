import requests
from bs4 import BeautifulSoup
from datetime import date
import pandas as pd
from dagster import solid,OutputDefinition
import dagster_pandas as dp

daily_buying_price_df_for_gold = dp.create_dagster_pandas_dataframe_type(
    name="daily_buying_price_df_for_gold",
    columns=[
        dp.PandasColumn.string_column("date"),
        dp.PandasColumn.string_column("buying_price")
    ],
)

@solid(output_defs=[OutputDefinition(name="daily_buying_price_df_for_gold", dagster_type=daily_buying_price_df_for_gold)])
def download_contents_as_df(context,start_date : str , end_date : str) -> pd.DataFrame:
    date_price_list = []
    date_range_list_formatted = list(map(
        lambda x : {'date_for_url' : x.strftime('%Y/%m/%d'),'date_for_output' : x.strftime('%Y-%m-%d')}
        ,pd.date_range(start_date,end_date).to_pydatetime().tolist()
        ))
    for dt in date_range_list_formatted:
        headers = { 
                    'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0',
                    'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
                }
        link = 'https://altin.in/arsiv/{date}'.format(
            date=dt['date_for_url']
        )
        content = requests.get(link , headers = headers).text
        soup = BeautifulSoup(content,features='html.parser')
        price = soup.find_all('li',{'title': 'Gram Altın - Alış'})[0].text
        date_price_list.append({ 'date' : dt['date_for_output'] , 'buying_price' : price  })
    return pd.DataFrame(date_price_list)
