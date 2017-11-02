import json
import xarray
import uuid
import boto3


class ExampleShredding():

    def _get_ddb_geojson_from_coordinates(self,coordinate_list):
        coords = []
        for i in coordinate_list:
            lon = {"N": str(i[0])}
            lat = {"N": str(i[1])}
            coords.append ({"L": [lon, lat]})
        print(coords)
        return coords

    def _get_geo_area_from_dataset(self,ds):
        return json.loads(ds.attrs['core_geographic_area_shape'])

    def _get_forecast_date_from_dataset(self,ds):
        print(str( ds.data_vars['issue_time'].values))
        return str(ds.data_vars['issue_time'].values)


    def _get_netcdf_data_for_file(self,bucket_name, object_key):
        file_path = "/tmp/" + uuid.uuid4().get_hex()
        print(bucket_name)
        print(object_key)
        s3_client = boto3.client('s3')
        s3_client.download_file(
            bucket_name,
            object_key,
            file_path)
        return xarray.open_dataset(file_path)


    def _get_geo_area_from_dataset(self,ds):
        return json.loads(ds.attrs['core_geographic_area_shape'])

    def _send_metadata_to_dynamo_db(self,ds):
        es = ExampleShredding()
        geojson_object = es._get_geo_area_from_dataset(ds)

        dynamodb = boto3.resource('dynamodb' , region_name='eu-west-1')
        file_id = uuid.uuid4().get_hex()
        print(file_id)

        table = dynamodb.Table('netcdf-metadata-table')
        table.put_item(
            Item={
                'bulk_forecast_file_id': file_id,
                'forecast_date': es._get_forecast_date_from_dataset(ds),
                'bucket_name' : bucket_name,
                'key': file_name,
                'geo_json':es._get_ddb_geojson_from_coordinates(geojson_object['coordinates'][0])
                }
        )


if __name__ == '__main__':
    es = ExampleShredding()
    bucket_name = 'ketaki-bucket-ks'
    file_name = 'BEST_FCS_1HOURLY_2017102500_00aeefe8-1d4c-4c0b-9486-b3581beed53d.nc'
    ds = es._get_netcdf_data_for_file(bucket_name, file_name)
    es._send_metadata_to_dynamo_db(ds)
    geojson_object = es._get_geo_area_from_dataset(ds)
    print(geojson_object['coordinates'][0])
    es._get_ddb_geojson_from_coordinates(geojson_object['coordinates'][0])
    es._get_geo_area_from_dataset(ds)
