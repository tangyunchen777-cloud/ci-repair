"""LocalStack READY hook (Python avoids executable-bit issues on Windows)."""
import json
import os
from pathlib import Path

import boto3

region = os.environ["AWS_DEFAULT_REGION"]
credentials = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}
sqs = boto3.client("sqs", region_name=region, endpoint_url="http://localhost:4566", **credentials)
s3 = boto3.client("s3", region_name=region, endpoint_url="http://localhost:4566", **credentials)
dlq = sqs.create_queue(QueueName=os.environ["SQS_DLQ_NAME"], Attributes={"MessageRetentionPeriod": "1209600"})["QueueUrl"]
arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
sqs.create_queue(QueueName=os.environ["SQS_QUEUE_NAME"], Attributes={
    "RedrivePolicy": json.dumps({"deadLetterTargetArn": arn, "maxReceiveCount": "3"}),
    "ReceiveMessageWaitTimeSeconds": "20", "VisibilityTimeout": "300",
})
bucket = os.environ["S3_BUCKET"]
args = {} if region == "us-east-1" else {"CreateBucketConfiguration": {"LocationConstraint": region}}
s3.create_bucket(Bucket=bucket, **args)
s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
    "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True,
})
s3.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration={
    "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}],
})
Path("/tmp/ci-repair-ready").touch()
