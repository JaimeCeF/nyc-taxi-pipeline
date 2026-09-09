import boto3

endpoint_url='http://localhost:4566'

aws_session = boto3.Session(profile_name='localstack')

s3 = aws_session.resource('s3',
                    endpoint_url= endpoint_url)

bucket = s3.Bucket('nyc-taxi-pipeline-local')
for obj in bucket.objects.all():
    print(obj.key)