"Connector and methods accessing S3"

import os
import sys

from io import StringIO, BytesIO
from typing import Union
import boto3
import pandas as pd



sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.constants import S3FileTypes
from common.custom_exceptions import WrongFormatException

def get_s3_connection(s3_bucket_meta: dict):
    """
    Establish a connection with S3
    """
    endpoint_url = s3_bucket_meta['src_endpoint_url']
    session = boto3.Session(aws_access_key_id=s3_bucket_meta['access_key'],
                            aws_secret_access_key=s3_bucket_meta['secret_key'])
    s3 = session.resource(service_name='s3', endpoint_url=endpoint_url)

    return s3

def list_files_in_prefix( prefix: str, s3,bucket: str ):
    """
        listing all files with a prefix on the S3 bucket
        :param prefix: prefix on the S3 bucket that should be filtered with
        :param s3 src bucket object
        :param bucket
        returns: 
            files:list of all the file names containing the prefix in the key
    """
    bucket = s3.Bucket(bucket)
    print(bucket)
    files = [obj.key for obj in bucket.objects.filter(Prefix=prefix)]
    return files

def read_csv_to_df( key: str,s3_bucket: str,s3,encoding: str ='utf-8',sep: str =','):
    """
    Read a csv file from the S3 bucket and return a dataframe

    :param key: key of the file that should be read
    :param s3_bucket
    :param s3 contains connection details required
    :param encoding: encoding of the data inside the cs file
    :param sep: separator of the csv file

     returns:
      data_frame: Pandas DataFrame containing the data of the csv file
    """

        #self._logger.info('Reading file %s/%s/%s', self.endpoint_url, self._bucket.name, key)

    bucket = s3.Bucket(s3_bucket)
    #print(type(bucket))
    csv_obj =bucket.Object(key=key).get().get('Body').read().decode(encoding)
    data = StringIO(csv_obj)
    data_frame = pd.read_csv(data, sep=sep)
    #print(data_frame)
    return data_frame

def write_df_to_s3(s3,data_frame: pd.DataFrame, key: str,file_format: str):
        """
        writing a pandas DataFrame to S3
        supported formats: .csv , .parquet

        :data_frame: Pandas DataFrame that should be written
        :key: target key of the saved file
        :file_format: format of the saved file
        """

        if data_frame.empty:
            print('The DataFrame is empty! No file will be written')
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer,index=False)
            return __put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer,index=False)
            s3_conn = get_s3_connection(s3)
            return __put_object(s3_conn,s3,out_buffer, key)
        print('The file format %s is not supported to be written to S3!',file_format)
        raise WrongFormatException

def __put_object(s3_conn, s3,out_buffer: Union[StringIO,BytesIO] , key: str):
        """
        Helper function for self.write_df_to_s3()

        :out_buffer: STringIO | BytesIO that should be written
        :key: target key of the saved file
        """
        bucket = s3_conn.Bucket(s3['trg_bucket'])
        print('Writing file to %s/%s/%s',s3['trg_endpoint_url'], s3['trg_bucket'], key)
        bucket.put_object(Body= out_buffer.getvalue(),Key=key)
        return True
    