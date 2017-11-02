import uuid
import boto3
import os
import json
import xarray

from lambda_base.aws_lambda_base import AWSLambdaBase


def _get_s3_bucket_name_for_record(record):
    return record['s3']['bucket']['name']


def _get_s3_key_name_from_record(record):
    return record['s3']['object']['key']


def _get_geo_area_from_dataset(ds):
    return json.loads(ds.attrs['core_geographic_area_shape'])


def _get_forecast_date_from_dataset(ds):
    return ds.data_vars['issue_time'].values


class S3ShredNetcdfMetadataHandler(AWSLambdaBase):

    def __init__(self, s3_client):
        super(S3ShredNetcdfMetadataHandler, self).__init__()
        self.create_dynamodb_table = None
        self.s3_client = s3_client
        self.es_end_point = os.getenv("ES_ENDPOINT")
        self.index_name = "metadata"
        self.doc_name = "netcdf-metadata"


    def _get_netcdf_data_for_file(self, bucket_name, object_key):
        file_path = "/tmp/" + uuid.uuid4().get_hex()
        self.s3_client.download_file(
            bucket_name,
            object_key,
            file_path)
        return xarray.open_dataset(file_path)



    def _handle(self, event, context):
        records = event['Records']
        for record in records:
            bucket_name = _get_s3_bucket_name_for_record(record)
            file_name = _get_s3_key_name_from_record(record)

        ds = self._get_netcdf_data_for_file(bucket_name, file_name)
        print(ds)


handler = S3ShredNetcdfMetadataHandler.get_handler(boto3.client('s3'))
