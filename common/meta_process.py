"""
Methods for processing the meta file
"""
import os
import sys

import yaml
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.constants import MetaProcessFormat


def update_processed_date():
    """
    Update the run_info table with the dates processed in the current run
    """
    config = yaml.safe_load(open("config/xetra_report1_config.yml"))
    hook = PostgresHook(postgres_conn_id=config['db']['trg_connection'])

    # Writing run_info to DB
    hook.copy_expert(
        sql="COPY run_info FROM stdin WITH DELIMITER as '|'"
        , filename='/tmp/run_info.csv')

    return True

def return_date_list(first_date: str):
        """
        Creating a list of dates based on the input first_date and the already
        processed dates in the meta file

        :param: first_date -> the earliest date Xetra data should be processed

        returns:
          return_dates: list of all dates from min_date till today
        """

        start = datetime.strptime(first_date,
                                  MetaProcessFormat.META_DATE_FORMAT.value) \
                    .date() - timedelta(days=1)
        today = datetime.today().date()

        # Creating a list of dates from first_date until today
        return_dates = [start + timedelta(days=x) for x in range(1, (today - start).days + 1)]

        return return_dates

