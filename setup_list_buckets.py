from distutils.core import setup
setup(name='s3_get_object_lambda_service',
      version='1.0',
      packages=[
            'list_buckets',
            'lambda_base',
            'rest_handler_base']
      )
