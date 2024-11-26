from chainlit.data.storage_clients.base import BaseStorageClient
from chainlit.logger import logger
from typing import Dict, Union, Any
import boto3    # type: ignore


class MinioStorageClient(BaseStorageClient):
    """
    Class to enable MinIO storage provider

    params:
        bucket: Bucket name, should be set with public access
        endpoint_url: MinIO server endpoint, defaults to "http://localhost:9000"
        access_key: Default is "minioadmin"
        secret_key: Default is "minioadmin"
        verify_ssl: Set to True only if not using HTTP or HTTPS with self-signed SSL certificates
    """

    def __init__(self, bucket: str, endpoint_url: str = 'http://localhost:9000', access_key: str = 'minioadmin', secret_key: str = 'minioadmin', verify_ssl: bool = False):

        self.bucket = bucket
        self.endpoint_url = endpoint_url

        try:
            self.client = boto3.client("s3", endpoint_url=endpoint_url, aws_access_key_id=access_key, aws_secret_access_key=secret_key, verify=verify_ssl)
        except Exception as e:
            logger.warn(f"MinioStorageClient initialization error: {e}")
        else:
            logger.info("MinioStorageClient initialized")


    async def upload_file(self, object_key: str, data: Union[bytes, str], mime: str = 'application/octet-stream', overwrite: bool = True) -> Dict[str, Any]:
        try:
            self.client.put_object(Bucket=self.bucket, Key=object_key, Body=data, ContentType=mime)
        except Exception as e:
            logger.warn(f"MinioStorageClient, upload_file error: {e}")
            return {}
        else:
            return {"object_key": object_key, "url": f"{self.endpoint_url}/{self.bucket}/{object_key}"}
