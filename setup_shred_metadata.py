from distutils.core import setup
setup(name='s3_shred_metadata_lambda_service',
      version='1.0',
      packages=[
            'shred_metadata',
            'lambda_base']
      )