import backoff
import hashlib
import json
from collections import defaultdict
from pathlib import Path
import secrets 

import aiohttp
import asyncio

from azure.storage.blob.aio import BlobType

from config import container_client
from logging_config import logger

from constants import MAX_CONCURRENT_UPLOADS, REDACT_KEYS


def get_api_headers(input_json: dict) -> dict:
    return {
        "Authorization": "ODFHIR " + get_api_token(input_json),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def get_api_token(input_json: dict) -> str:
    return f"{input_json['dev_token']}/{input_json['practice_token']}"


def construct_file_path(inputjson: dict, segment: str) -> str:
    referral_code = inputjson.get("referral_code", "")
    ingestion_blob_path = inputjson.get("ingestion_blob_path", "")

    patient_id = inputjson.get("patient_id", "")
    resource_id = inputjson.get("resource_id", patient_id)

    file_path = (
        Path(referral_code) / ingestion_blob_path / segment / f"{resource_id}.ndjson"
    )
    return str(file_path)


@backoff.on_exception(backoff.expo, (aiohttp.ClientError, Exception), max_tries=5)
async def safe_upload(payload, upload_path):
    return await upload_file(upload_data=payload, upload_path=upload_path)


async def upload_file(upload_data: str, upload_path: str) -> None:
    if not upload_data:
        logger.info(f"Blob cannot be saved to {upload_path}: data is empty")
        return

    blob_client = container_client.get_blob_client(blob=upload_path)

    try:
        if await blob_client.exists():
            # Download existing content
            existing_blob = await blob_client.download_blob()
            existing_data = await existing_blob.readall()
            combined_data = existing_data.decode("utf-8") + upload_data + "\n"
        else:
            combined_data = upload_data + "\n"

        # Upload the combined data as a new block blob (overwrite)
        await blob_client.upload_blob(
            data=combined_data, overwrite=True, blob_type=BlobType.BLOCKBLOB
        )
        return True

    except Exception as e:
        logger.error(f"Failed to upload blob to {upload_path}: {e}")
        return False


def create_msg(input_json: dict, pat_id: int, ingestion_blob_path: str) -> dict:
    return {
        "patient_id": pat_id,
        "ingestion_blob_path": ingestion_blob_path,
        "practice_id": input_json["practice_id"],
        "dev_token": input_json["dev_token"],
        "practice_token": input_json["practice_token"],
    }


def create_result_msg(results, pat_id):
    result_msg = {"pat_id": pat_id, "success": True, "errors": [], "data": []}

    for result in results:
        success, segment, cb_result_array, res = result

        if not success:
            result_msg["errors"].append(cb_result_array)
            result_msg["success"] = False
            continue

        result_msg["data"].append(res)

        if cb_result_array:
            for sub_result in cb_result_array:
                sub_success, sub_segment, parent_id, sub_res = sub_result
                if not sub_success:
                    result_msg["errors"].append(parent_id)
                    result_msg["success"] = False
                    continue
                result_msg["data"].append(sub_res)

    if not result_msg["errors"]:
        result_msg["errors"] = None

    return result_msg


def get_redacted_phi(input_json: dict) -> dict:
    for key in REDACT_KEYS:
        input_json.pop(key, None)
    return input_json


def is_ingestion_allowed(now):
    weekday = now.weekday()
    hour = now.hour
    if weekday < 5:
        return hour >= 20 or hour < 6
    return True


def get_next_allowed_time(now):
    if is_ingestion_allowed(now):
        return now
    if now.weekday() < 5:
        if now.hour < 6:
            return now.replace(hour=6, minute=0, second=0, microsecond=0)
        return now.replace(hour=20, minute=0, second=0, microsecond=0)
    return now


# This function checks if there are connectivity errors in the batch results.
# If any error contains "ConnectivityError", it returns True, otherwise False.
# This is used to determine if the batch should be retried due to connectivity issues.
def has_connectivity_error(batch_results):
    for result in batch_results:
        errors = result.get("errors", [])
        if errors:
            for error in errors:
                if error.get("reason") and "ConnectivityError" in error["reason"]:
                    return True
    return False


def get_batch_data(batch_results):
    uploads = []

    # Extract all patient data from batch_results
    batch_upload_data = [
        data for result in batch_results for data in result["data"] if data
    ]

    for record in batch_upload_data:
        data = record["data"]
        upload_path = record["path"]

        # Convert to NDJSON if data is a list, else just JSON
        if isinstance(data, list):
            payload = "\n".join(json.dumps(item) for item in data)
        else:
            payload = json.dumps(data)

        uploads.append({"path": upload_path, "data": payload})

    return uploads


def group_resource_by_patnum(procedures):
    grouped = defaultdict(list)
    for proc in procedures:
        grouped[proc["PatNum"]].append(proc)
    return dict(grouped)


async def upload_list_data(resource_list, inputjson, segment):
    rec_by_patnum = group_resource_by_patnum(resource_list)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)
    tasks = []

    async def upload_task(patnum, procs):
        async with semaphore:
            msg_json = create_msg(inputjson, patnum, inputjson["ingestion_blob_path"])
            msg_json = {**msg_json, "referral_code": inputjson["referral_code"]}
            payload = "\n".join(json.dumps(proc) for proc in procs)
            upload_path = construct_file_path(msg_json, segment)
            return await safe_upload(payload=payload, upload_path=upload_path)

    for patnum, procs in rec_by_patnum.items():
        tasks.append(upload_task(patnum, procs))

    return await asyncio.gather(*tasks)


def get_latest_datestamp(records, key="DateTStamp"):
    return max(
        (r[key] for r in records if r.get(key)),
        default=None,
    )


def get_hash_reference(value: str) -> str:
    """Returns a salted SHA-256 hash for patient reference."""
    salt = secrets.token_hex(16)
    return hashlib.sha256((salt + value).encode()).hexdigest()


