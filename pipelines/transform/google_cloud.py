from google.cloud import bigquery
from dagster import solid,InputDefinition,Nothing
import os

@solid
def execute_sql(context,sql_query : str) -> None:
    client = bigquery.Client()
    query_job = client.query(sql_query)
    results = query_job.result()

@solid
def execute_sql_file(context,file_name : str,dependency_list : list) -> None:
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    client = bigquery.Client()
    with open('bigquery_sqls/' + file_name) as file:
        sql_query = file.read()
    query_job = client.query(sql_query)
    results = query_job.result()
