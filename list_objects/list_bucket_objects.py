from rest_handler_base.s3_client_lambda import S3ClientLambdaRestHandler

import boto3,json
import os


class S3ListObjectsRestHandler(S3ClientLambdaRestHandler):

    def __init__(self, s3):
        super(S3ListObjectsRestHandler, self).__init__(s3)
        self.es_end_point = os.getenv("ES_ENDPOINT")

    def _handle(self, event, context):
        bucket_name = self._get_bucket_name(event)
        objects = {'Objects': self.get_keys(bucket_name)}
        return {
            "statusCode": 200,
            "body": json.dumps(objects)
        }

    def get_keys(self, bucket_name):
        s3resource = boto3.resource('s3')
        bucket_object = s3resource.Bucket(bucket_name)
        mylist = list()
        for s3_file in bucket_object.objects.all():
            print(s3_file.key)
            mylist.append(s3_file.key)
        return mylist

handler = S3ListObjectsRestHandler.get_handler(boto3.client('s3'))

