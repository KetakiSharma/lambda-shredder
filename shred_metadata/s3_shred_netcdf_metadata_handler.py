import uuid
import boto3
import logging
import os
import json
import xarray

from lambda_base.aws_lambda_base import AWSLambdaBase


def _get_s3_bucket_name_for_record(record):
    return record['s3']['bucket']['name']


def _get_s3_key_name_from_record(record):
    return record['s3']['object']['key']


def _get_geo_area_from_dataset(data_set):
    return json.loads(data_set.attrs['core_geographic_area_shape'])


def _get_forecast_date_from_dataset(data_set):
    return str(data_set.data_vars['issue_time'].values)


def _get_ddb_geojson_from_coordinates(coordinate_list):
    coords = []
    for i in coordinate_list:
        lon = {"N": str(i[0])}
        lat = {"N": str(i[1])}
        coords.append ({"L": [lon, lat]})
    print(coords)
    return coords


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
        print(xarray.open_dataset(file_path))
        return xarray.open_dataset(file_path)


    def _handle(self, event, context):
        records = event['Records']
        for record in records:
            bucket_name = _get_s3_bucket_name_for_record(record)
            print("bucket name is: " + bucket_name)
            file_name = _get_s3_key_name_from_record(record)
            print("file name is: " + file_name)
            data_set  = self._get_netcdf_data_for_file(bucket_name, file_name)
            print(data_set )
            self._send_metadata_to_dynamo_db(data_set,bucket_name,file_name)
            data_set.close()

    def _send_metadata_to_dynamo_db(self,data_set,bucket_name,file_name):
        geojson_object = _get_geo_area_from_dataset(data_set)

        dynamodb = boto3.resource('dynamodb' , region_name='eu-west-1')
        file_id = uuid.uuid4().get_hex()
        print(file_id)

        table = dynamodb.Table('netcdf-metadata-table')
        table.put_item(
            Item={
                'bulk_forecast_file_id': str(uuid.uuid4()),
                'bucket_name' : bucket_name,
                'key_name': file_name,
                'forecast_date': str(_get_forecast_date_from_dataset(data_set)),
                'geo_json':_get_ddb_geojson_from_coordinates(geojson_object['coordinates'][0])
            }
        )


handler = S3ShredNetcdfMetadataHandler.get_handler(boto3.client('s3'))
