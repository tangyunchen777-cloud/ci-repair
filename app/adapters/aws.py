import boto3
from botocore.config import Config

from app.config import Settings


def aws_client(service: str, settings: Settings):
    settings.require("aws_region")
    return boto3.client(
        service, region_name=settings.aws_region, endpoint_url=settings.aws_endpoint_url,
        config=Config(connect_timeout=5, read_timeout=25 if service == "sqs" else 15,
                      retries={"mode": "standard", "total_max_attempts": 2}),
    )
