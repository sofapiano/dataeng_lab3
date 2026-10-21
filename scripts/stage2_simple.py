import boto3
from botocore.config import Config

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='rustfsadmin',
    aws_secret_access_key='rustfsadmin',
    region_name='us-east-1',
    config=Config(signature_version='s3v4', s3={'addressing_style': 'path'})
)

bucket = 'hh-lake'
key = 'raw/it_vacancies_2024.csv'
local = 'data/raw/it_vacancies_2024.csv'

try:
    s3.head_object(Bucket=bucket, Key=key)
    print('already exists')
    resp = s3.head_object(Bucket=bucket, Key=key)
    print(resp['ContentLength'])
    raise SystemExit(0)
except Exception as e:
    if '404' not in str(e) and 'NotFound' not in str(e):
        print(e)
        raise SystemExit(1)

print('uploading')
s3.upload_file(local, bucket, key)
print('uploaded')
resp = s3.head_object(Bucket=bucket, Key=key)
print(resp['ContentLength'])
