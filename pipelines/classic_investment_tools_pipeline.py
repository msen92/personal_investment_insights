from extraction.scraping import paracevirici_scraper as ps,altinin_scraper as als
from dagster import execute_pipeline, pipeline
from load import google_cloud as gc
from transform import google_cloud as gcs
import datetime

run_config = {
    'solids': {
         'download_dollar_data': {
            'inputs': {
                'investment_tool': {
                    'value': 'dollar'
                },
                'year': {
                    'value': '2022'
                }
            }
        },
        'download_euro_data': {
            'inputs': {
                'investment_tool': {
                    'value': 'euro'
                },
                'year': {
                    'value': '2022'
                }
            }
        },
        'download_gold_data': {
            'inputs': {
                'start_date' : '2021-09-25',# str(datetime.date.today() - datetime.timedelta(days=7)),
                'end_date' : '2022-01-08'#str(datetime.date.today())
            }
        },
        'load_dollar_data_to_gcs' : {
            'inputs' : {
                'bucket_name' : {
                    'value' : 'msen-gcloud-temp'
                },
                'csv_name' : {
                    'value' : 'dollar_data'
                },
                'project' : {
                    'value' : 'msen-gcloud'
                }
            }
        },
        'load_euro_data_to_gcs' : {
            'inputs' : {
                'bucket_name' : {
                    'value' : 'msen-gcloud-temp'
                },
                'csv_name' : {
                    'value' : 'euro_data'
                },
                'project' : {
                    'value' : 'msen-gcloud'
                }
            }
        },
        'load_gold_data_to_gcs' : {
            'inputs' : {
                'bucket_name' : 'msen-gcloud-temp',
                'csv_name' : 'gold_data',
                'project' : 'msen-gcloud'
            }
        },
        'load_dollar_data_to_table' : {
            'inputs' : {
                'table_id' : {
                    'value' : 'msen-gcloud.temp.dollar_yearly_data'
                },
                'project' : {
                    'value' : 'msen-gcloud'
                }
            }
        },
        'load_euro_data_to_table' : {
            'inputs' : {
                'table_id' : {
                    'value' : 'msen-gcloud.temp.euro_yearly_data'
                },
                'project' : {
                    'value' : 'msen-gcloud'
                }
            }
        },
        'load_gold_data_to_table' : {
            'inputs' : {
                'table_id' : {
                    'value' : 'msen-gcloud.temp.gold_weekly_data'
                },
                'project' : {
                    'value' : 'msen-gcloud'
                }
            }
        },
        'truncate_dollar_data' : {
            'inputs' : {
                'file_name' : 'truncate_dollar_yearly_data.sql',
                'dependency_list' : []
            }
        },
        'truncate_euro_data' : {
            'inputs' : {
                'file_name' : 'truncate_euro_yearly_data.sql',
                'dependency_list' : []
            }
        },
        'truncate_gold_data' : {
            'inputs' : {
                'file_name' : 'truncate_gold_weekly_data.sql',
                'dependency_list' : []
            }
        },
        'merge_dollar_data' : {
            'inputs' : {
                'file_name' : 'merge_dollar_data.sql'
            }
        },
        'merge_euro_data' : {
            'inputs' : {
                'file_name' : 'merge_euro_data.sql'
            }
        },
        'merge_gold_data' : {
            'inputs' : {
                'file_name' : 'merge_gold_data.sql'
            }
        },
        'investment_summary' : {
            'inputs' : {
                'file_name' : 'investment_summary.sql'
            }
        },
        'investment_current_state' : {
            'inputs' : {
                'file_name' : 'investment_current_state.sql'
            }
        }
    }
}

@pipeline
def classic_investment_tools_pipeline():
    download_dollar_data = ps.download_contents.alias('download_dollar_data')
    download_euro_data = ps.download_contents.alias('download_euro_data')
    download_gold_data = als.download_contents_as_df.alias('download_gold_data')
    load_dollar_data_to_gcs = gc.load_df_to_gcs.alias('load_dollar_data_to_gcs')
    load_euro_data_to_gcs = gc.load_df_to_gcs.alias('load_euro_data_to_gcs')
    load_gold_data_to_gcs = gc.load_df_to_gcs.alias('load_gold_data_to_gcs')
    truncate_dollar_data = gcs.execute_sql_file.alias('truncate_dollar_data')
    truncate_euro_data = gcs.execute_sql_file.alias('truncate_euro_data')
    truncate_gold_data = gcs.execute_sql_file.alias('truncate_gold_data')
    load_dollar_data_to_table = gc.load_gcs_data_to_table.alias('load_dollar_data_to_table')
    load_euro_data_to_table = gc.load_gcs_data_to_table.alias('load_euro_data_to_table')
    load_gold_data_to_table = gc.load_gcs_data_to_table.alias('load_gold_data_to_table')
    merge_dollar_data = gcs.execute_sql_file.alias('merge_dollar_data')
    merge_euro_data = gcs.execute_sql_file.alias('merge_euro_data')
    merge_gold_data = gcs.execute_sql_file.alias('merge_gold_data')
    investment_summary = gcs.execute_sql_file.alias('investment_summary')
    investment_current_state = gcs.execute_sql_file.alias('investment_current_state')
    dollar_df = ps.extract_price_table(download_dollar_data())
    euro_df = ps.extract_price_table(download_euro_data())
    gold_df = download_gold_data()
    load_dollar_to_gcs = load_dollar_data_to_gcs(dollar_df)
    load_euro_data_to_gcs = load_euro_data_to_gcs(euro_df)
    load_gold_data_to_gcs = load_gold_data_to_gcs(gold_df)
    _load_dollar_data_to_table = load_dollar_data_to_table(uri=load_dollar_to_gcs,dummy_dependency_param=truncate_dollar_data())
    _load_euro_data_to_table = load_euro_data_to_table(uri=load_euro_data_to_gcs,dummy_dependency_param=truncate_euro_data())
    _load_gold_data_to_table = load_gold_data_to_table(uri=load_gold_data_to_gcs,dummy_dependency_param=truncate_gold_data())
    _merge_dollar_data = merge_dollar_data(dependency_list = [_load_dollar_data_to_table])
    _merge_euro_data = merge_euro_data(dependency_list = [_load_euro_data_to_table])
    _merge_gold_data = merge_gold_data(dependency_list = [_load_gold_data_to_table])
    _investment_summary = investment_summary(dependency_list = [_merge_dollar_data,_merge_euro_data,_merge_gold_data])
    investment_current_state(dependency_list = [_investment_summary])

if __name__ == '__main__':
    execute_pipeline(classic_investment_tools_pipeline,run_config=run_config)
