from io import BytesIO
from multiprocessing.pool import ThreadPool
from azure.storage.blob import BlobServiceClient
import pyarrow.parquet as pq

from flask import current_app as app


class AzureBlobDownloader:
    def __init__(self):
        self.MY_CONNECTION_STRING = app.config["ASA_URI"]
        self.MY_BLOB_CONTAINER = "uploads"

        # Initialize the connection to Azure storage account
        self.blob_service_client = BlobServiceClient.from_connection_string(self.MY_CONNECTION_STRING)
        self.my_container = self.blob_service_client.get_container_client(self.MY_BLOB_CONTAINER)

    def download_all_blobs_in_container(self, prefix=None, filter=None):
        # get a list of blobs
        my_blobs = self.my_container.list_blobs(name_starts_with=prefix)

        if filter is not None:
            def valid_blob(blob,filter):
                for f in filter:
                    if f in blob.name:
                        return True
                return False
            my_blobs = (blob for blob in my_blobs if valid_blob(blob,filter))
        result = self.run(my_blobs)
        return result

    def run(self, blobs):
        # Download 10 files at a time!
        with ThreadPool(processes=int(10)) as pool:
            return pool.map(self.get_parquet_table, blobs)

    def get_parquet_table(self, blob):
        blob_client = self.my_container.get_blob_client(blob=blob.name)
        byte_stream = BytesIO()
        blob_client.download_blob().readinto(byte_stream)
        partition = pq.read_table(source=byte_stream)
        byte_stream.close()
        return partition