"""
Methods for processing the meta file
"""
import os
import sys
import collections
from datetime import datetime, timedelta
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.s3 import read_csv_to_df
from common.constants import MetaProcessFormat


def return_date_list(first_date: str, meta_key: str, s3_bucket_meta: dict,s3):
        """
        Creating a list of dates based on the input first_date and the already
        processed dates in the meta file

        :param: first_date -> the earliest date Xetra data should be processed
        :param: meta_key -> key of the meta file on the S3 bucket
        :param: s3_bucket_meta -> S3BucketConnector for the bucket with the meta file

        returns:
          min_date: first date that should be processed
          return_date_list: list of all dates from min_date till today
        """

        start = datetime.strptime(first_date,
                                  MetaProcessFormat.META_DATE_FORMAT.value)\
                                      .date() - timedelta(days=1)
        today = datetime.today().date()
        try:
            # If meta file exists create return_date_list using the content of the meta file
            # Reading meta file
            df_meta = read_csv_to_df(meta_key,'xetra-trg-data-rn23',s3)
            # Creating a list of dates from first_date until today
            dates = [start + timedelta(days=x) for x in range(0, (today - start).days + 1)]
            # Creating set of all dates in meta file
            src_dates = set(pd.to_datetime(
              df_meta[MetaProcessFormat.META_SOURCE_DATE_COL.value]
              ).dt.date)
            dates_missing = set(dates[1:]) - src_dates
            if dates_missing:
                # Determining the earliest date that should be extracted
                min_date = min(set(dates[1:]) - src_dates) - timedelta(days=1)
                # Creating a list of dates from min_date until today
                return_min_date = (min_date + timedelta(days=1))\
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                return_dates = [
                    date.strftime(MetaProcessFormat.META_DATE_FORMAT.value) \
                        for date in dates if date >= min_date
                        ]
            else:
                # Setting values for the earliest date and the list of dates
                return_dates = []
                return_min_date = datetime(2200, 1, 1).date()\
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        except s3.exceptions.NoSuchKey:
            # No meta file found -> creating a date list from first_date - 1 day untill today
            return_min_date = first_date
            return_dates = [
              (start + timedelta(days=x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value) \
              for x in range(0, (today - start).days + 1)
              ]
        return return_min_date, return_dates