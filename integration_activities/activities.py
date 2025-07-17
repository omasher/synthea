import json
import os

import azure.functions as func
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

from config import app
from constants import OPEN_DENTAL_API_KEY


@app.activity_trigger(input_name="inputjson")
@app.queue_output(
    arg_name="msg", queue_name="scheduled-transforms", connection="QUEUE_STORAGE_ACC"
)
def notify_transform(inputjson: dict, msg: func.Out[str]) -> None:
    msg.set(json.dumps(inputjson))


@app.activity_trigger(input_name="inputjson")
@app.queue_output(
    arg_name="msg",
    queue_name="provider-reconciliation",
    connection="QUEUE_STORAGE_ACC",
)
def notify_prov_rec(inputjson: dict, msg: func.Out[str]) -> None:
    msg.set(json.dumps(inputjson))


@app.activity_trigger(input_name="inputjson")
def get_token_value(inputjson: dict) -> tuple[str, str]:
    key_vault_name = os.getenv("KEY_VAULT_NAME")
    secret_name = inputjson.get("secret_name", OPEN_DENTAL_API_KEY)
    if not key_vault_name:
        raise EnvironmentError("KEY_VAULT_NAME environment variable is not set.")

    vault_url = f"https://{key_vault_name}.vault.azure.net"
    secret_client = SecretClient(
        vault_url=vault_url, credential=DefaultAzureCredential()
    )

    try:
        secret_value = secret_client.get_secret(secret_name).value
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve secrets: {e}")

    return secret_value
