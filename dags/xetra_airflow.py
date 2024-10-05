import sys
import os
from datetime import datetime
import pandas as pd
import yaml

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.s3 import get_s3_connection, list_files_in_prefix, read_csv_to_df, write_df_to_s3
from common.meta_process import return_date_list, update_processed_date
from common.constants import MetaProcessFormat


def stg_to_trg():
    """
    Saves a Pandas DataFrame to the target S3 bucket as a parquet file

    """
    config = yaml.safe_load(open("config/xetra_report1_config.yml"))
    hook = PostgresHook(postgres_conn_id=config['db']['trg_connection'])

    trg_key = config['target']['trg_key']
    trg_key_date_format = config['target']['trg_key_date_format']
    trg_format = config['target']['trg_format']
    # Creating target key
    target_key = (
        f'{trg_key}'
        f'{datetime.today().strftime(trg_key_date_format)}.'
        f'{trg_format}'
    )

    # Writing to target

    records = hook.get_records(sql="select * from src_stock_data")
    data_frame = pd.DataFrame(records, columns=config['target']['trg_columns'])
    write_df_to_s3(config['s3'], data_frame, target_key, trg_format)

    # Updating meta file
    hook.copy_expert(
        sql="COPY run_info FROM stdin WITH DELIMITER as '|'"
        , filename='/tmp/run_info.csv')

    update_processed_date()

    return True


def src_to_stage():
    """Read the source data and extract it to a csv file
    """

    config = yaml.safe_load(open("config/xetra_report1_config.yml"))
    s3 = get_s3_connection(config['s3'])
    hook = PostgresHook(postgres_conn_id=config['db']['src_connection'])
    src_date_list = hook.get_records(sql="Select CAST(max(source_date) as TEXT) as src_date from run_info;")
    extract_date = datetime.today().date().strftime(MetaProcessFormat.META_DATE_FORMAT.value) if src_date_list[0][
                                                                                                     0] is None else \
    src_date_list[0][0]
    print(extract_date)
    extract_date_list = return_date_list(extract_date)

    files = [key for date in extract_date_list \
             for key in list_files_in_prefix(date, s3, config['s3']['src_bucket'])]

    if not files:
        data_frame = pd.DataFrame()
    else:
        data_frame = pd.concat([read_csv_to_df(file, config['s3']['src_bucket'], s3) \
                                for file in files], ignore_index=True)

    data_frame.to_csv('/tmp/stage_stock_data.csv', index=None, header=False, sep='|')

    hook.copy_expert(
        sql="COPY src_stock_data FROM stdin WITH DELIMITER as '|'"
        , filename='/tmp/stage_stock_data.csv')

    # Create a run_info file with processing dates for the current run
    df_new = pd.DataFrame(columns=[
        MetaProcessFormat.META_SOURCE_DATE_COL.value,
        MetaProcessFormat.META_PROCESS_COL.value])
    df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list
    df_new[MetaProcessFormat.META_PROCESS_COL.value] = \
        datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
    df_new.to_csv('/tmp/run_info.csv', index=None, header=False, sep='|')


with DAG('Xetra-data-pipeline-1', start_date=datetime(2023, 1, 1)
        , schedule_interval='@daily', catchup=False) as dag:
    setup = PostgresOperator(
        task_id='setup'
        , postgres_conn_id='postgres'
        , sql='/sql/setup.sql'
    )

    src_to_stage = PythonOperator(
        task_id='src_to_stage'
        , python_callable=src_to_stage
    )

    stg_to_trg = PythonOperator(
        task_id='stg_to_trg'
        , python_callable=stg_to_trg
    )

    setup >> src_to_stage >> stg_to_trg


