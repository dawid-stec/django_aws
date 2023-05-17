import os
import logging
from pathlib import Path
from tempfile import NamedTemporaryFile

from boto3 import resource, client, set_stream_logger
from botocore.client import Config, BaseClient

from aws.settings import (
    S3_REGION,
    S3_ENDPOINT_URL,
    S3_SIGNED_VERSION,
    S3_ACCESS_KEY_ID,
    S3_SECRET_ACCESS_KEY
)

set_stream_logger("", logging.INFO)


class BaseOperator:
    resource_instance = None
    client_instance = None

    def __init__(self):
        self.config = dict(
            endpoint_url=S3_ENDPOINT_URL,
            aws_access_key_id=S3_ACCESS_KEY_ID,
            aws_secret_access_key=S3_SECRET_ACCESS_KEY,
            config=Config(
                signature_version=S3_SIGNED_VERSION,
                connect_timeout=1500,
                read_timeout=1500
            ),
            region_name=S3_REGION
        )

    @property
    def resource(self):
        if not self.resource_instance:
            self.resource_instance = resource('s3', **self.config)

        return self.resource_instance

    @property
    def client(self) -> BaseClient:
        if not self.client_instance:
            self.client_instance = client('s3', **self.config)

        return self.client_instance


class Reader(BaseOperator):

    def list(self, root, path):
        bucket = self.resource.Bucket(root)

        return [
            dict(root=obj.bucket_name, path=obj.key)
            for obj in bucket.objects.all()
            if not path or str(obj.key).startswith(path)
        ]

    def read(self, root, path):
        ext = f'.{path.split(".")[-1]}'
        temp_file = NamedTemporaryFile(delete=False, suffix=ext)

        kwargs = {
            'Bucket': root,
            'Key': path,
            'Fileobj': temp_file
        }

        try:
            self.client.download_fileobj(**kwargs)
        except Exception as e:
            if Path(temp_file.name).is_file():
                if not temp_file.closed:
                    temp_file.close()
                os.remove(temp_file.name)
            raise e

        return temp_file.name


class WriterException(Exception):
    pass


class Writer(BaseOperator):

    def clean_directory(self, root, path):
        reader = Reader()
        file_list = reader.list(root, path)

        for file in file_list:
            self.remove_file(**file)

    def remove_file(self, root, path):
        self.resource.Object(root, path).delete()

    def write(self, root, path, local_file):
        if not Path(local_file).is_file():
            raise WriterException(f'File: {local_file} does not exist')

        self.client.upload_file(Bucket=root, Filename=local_file, Key=path)

    def write_binary(self, root, path, binary_data: bytes):
        if binary_data is None:
            raise WriterException('No binary data to write')

        self.client.put_object(Body=binary_data, Bucket=root, Key=path)
