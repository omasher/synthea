import os
import azure.durable_functions as df
from azure.storage.blob.aio import BlobServiceClient

app = df.DFApp()
API_URL = "https://api.opendental.com/api/v1"
od_container = "open-dental-data"
connection_string = os.getenv("BLOB_CONNECTION_STRING")

blob_service_client = BlobServiceClient.from_connection_string(
    conn_str=connection_string
)
container_client = blob_service_client.get_container_client(od_container)
