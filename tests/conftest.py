import boto3
import pytest
from moto import mock_aws

BUCKET = "test-bucket"


@pytest.fixture
def s3(monkeypatch):
    """A moto-backed S3 client with BUCKET created.

    Credentials and region are pinned to fakes so a misconfigured test can
    never fall through to a real AWS profile.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET)
        yield client
