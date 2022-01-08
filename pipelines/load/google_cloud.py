import pandas as pd
from dagster import solid,OutputDefinition,InputDefinition,Nothing
import dagster_pandas as dp
from google.cloud import storage, bigquery

@solid
def load_df_to_gcs(context,dataframe,bucket_name : str,csv_name : str,project : str) -> str:
    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket_name)
    bucket.blob(csv_name + '.csv').upload_from_string(dataframe.to_csv(index=False,header = False), 'text/csv')
    return 'gs://' + bucket_name + '/' + csv_name + '.csv'

@solid(input_defs=[InputDefinition("uri", str),InputDefinition("project", str),InputDefinition("table_id", str),InputDefinition("dummy_dependency_param", Nothing)])
def load_gcs_data_to_table(context,uri : str,project : str,table_id : str) -> None:
    client = bigquery.Client(project=project)

    job_config = bigquery.LoadJobConfig(
    autodetect=True,
    source_format=bigquery.SourceFormat.CSV
    )

    load_job = client.load_table_from_uri(
    uri, table_id
    )
    
    load_job.result()
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))
