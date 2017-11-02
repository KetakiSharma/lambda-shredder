import boto3,json
import os

from rest_handler_base.s3_client_lambda import S3ClientLambdaRestHandler


class S3ListBucketsRestHandler(S3ClientLambdaRestHandler):
    def __init__(self, s3):
        super(S3ListBucketsRestHandler, self).__init__(s3)
        self.es_end_point = os.getenv("ES_ENDPOINT")

    def _handle(self, event,context):
        list_response = self.s3.list_buckets()
        output_entries = []
        for entries in list_response['Buckets']:
            output_entries.append({
                'Name': entries['Name']
            })
        json_response = {'Buckets': output_entries}
        return {
            "statusCode": 200,
            "body": json.dumps(json_response)
        }

handler = S3ListBucketsRestHandler.get_handler(boto3.client('s3'))




