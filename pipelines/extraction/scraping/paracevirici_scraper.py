import requests
from bs4 import BeautifulSoup
import sys
import pandas as pd
from dagster import solid,OutputDefinition,InputDefinition
import dagster_pandas as dp

daily_buying_price_df = dp.create_dagster_pandas_dataframe_type(
    name="daily_buying_price_df",
    columns=[
        dp.PandasColumn.string_column("date"),
        dp.PandasColumn.string_column("buying_price")
    ],
)

@solid
def download_contents(context,investment_tool : str , year : str) -> str:
    investment_tool_mapping = { 'dollar' : 'amerikan-dolari','euro' : 'euro' }
    if investment_tool not in investment_tool_mapping:
        sys.exit('Supplied investment tool is not scrapable from this website')
    else:
        investment_tool = investment_tool_mapping[investment_tool]
    headers = { 
                'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0',
                'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
              }
    link = 'https://paracevirici.com/doviz-arsiv/merkez-bankasi/gecmis-tarihli-doviz/{year}/{investment_tool}'.format(
        investment_tool = investment_tool
        ,year = year
    )
    return requests.get(link , headers = headers).text

@solid(output_defs=[OutputDefinition(name="daily_buying_price_df", dagster_type=daily_buying_price_df)])
def extract_price_table(context,content : str) -> pd.DataFrame:
    soup = BeautifulSoup(content,features='html.parser')
    divs = soup.find_all("div", {"class": "row"})
    data_list = []
    for div in divs:
        temp_date = div.find('p',{ 'class' : 'date' }).text
        if temp_date != 'TARİH':
            buying_price = div.find('p',{ 'class' : 'price buy' }).text.replace(',','.')[:-2]
            splitted_date = temp_date.split('-')
            date = splitted_date[2] + '-' + splitted_date[1] + '-' +  splitted_date[0]
            data_list.append( { 'date' : date , 'buying_price' : buying_price } )
    return pd.DataFrame(data_list)

@solid(input_defs=[InputDefinition(name="dataframe", dagster_type=daily_buying_price_df),InputDefinition(name="path", dagster_type=str)])
def write_df_to_csv(context,dataframe : pd.DataFrame,path : str) -> None:
    dataframe.to_csv(path,index=False)
