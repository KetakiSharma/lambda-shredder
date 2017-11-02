from rest_handler_base.s3_client_lambda import S3ClientLambdaRestHandler

import boto3


class S3GetObjectRestHandler(S3ClientLambdaRestHandler):


    def __init__(self, s3):
        super(S3GetObjectRestHandler, self).__init__(s3)


    def _handle(self, event, context):

        url = self.s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': self._get_bucket_name(event),
                'Key': self._get_object_name(event)
            }
        )

        return {
            "statusCode": 302,
            "headers": {'location': url}
        }

handler = S3GetObjectRestHandler.get_handler(boto3.client('s3'))

# obj= S3GetObjectRestHandler(boto3.client('s3'))
# obj.list_s3_object_name()
