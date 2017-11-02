import logging,json

from abc import abstractmethod, ABCMeta
from lambda_base.aws_lambda_base import AWSLambdaBase

class AWSLambdaRestHandlerBase(AWSLambdaBase):
    __metaclass__ = ABCMeta

    def __init__(self, log_level=logging.INFO):
        super(AWSLambdaRestHandlerBase, self).__init__(log_level)

    @abstractmethod
    def _handle_rest(self, event, context):
        raise NotImplementedError

    def _handle(self, event, context):
        try:
            return self._handle_rest(event, context)
        except Exception as e:
            self.logger.error(e)
            return {
                "statusCode": 500,
                "body": json.dumps({
                    "message": repr(e)
                })
            }
