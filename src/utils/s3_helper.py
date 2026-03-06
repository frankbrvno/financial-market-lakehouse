import boto3


def get_s3_client():
    return boto3.client("s3")


def upload_file_to_s3(local_file_path: str, bucket_name: str, s3_key: str) -> None:
    s3 = get_s3_client()
    s3.upload_file(local_file_path, bucket_name, s3_key)