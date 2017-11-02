import logging
from abc import abstractmethod, ABCMeta


class AWSLambdaBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, log_level=logging.INFO):
        self.logger = logging.getLogger()
        self.logger.setLevel(log_level)

    @classmethod
    def get_handler(cls, *args, **kwargs):
        def handler(event, context):
            return cls(*args, **kwargs)._handle(event, context)
        return handler

    @abstractmethod
    def _handle(self, event, context):
        raise NotImplementedError