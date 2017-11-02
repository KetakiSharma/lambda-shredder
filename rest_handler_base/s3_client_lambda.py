from rest_handler_base.aws_lambda_rest_handler_base import AWSLambdaRestHandlerBase
from abc import ABCMeta

class S3ClientLambdaRestHandler(AWSLambdaRestHandlerBase):
    __metaclass__ = ABCMeta

    def __init__(self, s3):
        super(S3ClientLambdaRestHandler, self).__init__()
        self.s3 = s3

    def _handle_rest(self, event, context):
        return self._handle(self, event, context)

    @staticmethod
    def _get_bucket_name(event):
        return event['pathParameters']['bucket']

    @staticmethod
    def _get_object_name(event):
        return event['pathParameters']['object']