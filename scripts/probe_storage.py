"""Диагностика доступа к RustFS: подбор учётных данных и проверка консоли."""
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

ENDPOINT = "http://localhost:9000"
CANDIDATES = [
    ("rustfsadmin", "rustfsadmin"),
    ("minioadmin", "minioadmin"),
    ("admin", "admin"),
]

for ak, sk in CANDIDATES:
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=ENDPOINT,
            aws_access_key_id=ak,
            aws_secret_access_key=sk,
            region_name="us-east-1",
            config=Config(signature_version="s3v4", retries={"max_attempts": 1}),
        )
        r = s3.list_buckets()
        print(f"OK  creds={ak}/{sk} -> buckets={[b['Name'] for b in r['Buckets']]}")
    except ClientError as e:
        print(f"ERR creds={ak}/{sk} -> {e.response['Error']['Code']}: {e.response['Error']['Message']}")
    except EndpointConnectionError as e:
        print(f"ERR creds={ak}/{sk} -> endpoint unreachable: {e}")
    except Exception as e:  # noqa: BLE001
        print(f"ERR creds={ak}/{sk} -> {type(e).__name__}: {str(e)[:200]}")
