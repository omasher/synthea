import asyncio
import json

import aiohttp

from config import app, API_URL
from constants import INGESTION_START_YEAR
from logging_config import logger
from ingestion_helpers import (
    get_resource,
    get_paginated_resource,
    ingest_new_providers,
    get_perio_measures,
    get_claim_proc,
)
from utils import (
    upload_list_data,
    get_latest_datestamp,
    get_api_headers,
    create_result_msg,
    get_redacted_phi,
    construct_file_path,
    safe_upload,
    get_batch_data,
)
from constants import MAX_CONCURRENT_UPLOADS


@app.activity_trigger(input_name="inputjson")
async def get_pats_and_provs(inputjson: dict) -> dict:
    ingestion_type = inputjson.get("ingestion_type", "initial")
    clinic_id = inputjson.get("clinic_id")
    practice = inputjson.get("referral_code")

    offset = 0
    res_count = 1000
    proc_list = []
    str_res = ""
    url = f"{API_URL}/procedurelogs?ClinicNum={clinic_id}"
    if INGESTION_START_YEAR.get(practice, None):
        url += f"&DateTStamp={INGESTION_START_YEAR.get(practice)}"
    if ingestion_type == "incremental":
        url += f"&DateTStamp={inputjson.get('last_proc_date_t_stamp', '')}"
    async with aiohttp.ClientSession() as session:
        while res_count >= 1000:
            async with session.request(
                method="GET",
                url=f"{url}&Offset={offset}",
                headers=get_api_headers(inputjson),
            ) as response:
                logger.warning(
                    f"Practice {practice}, Clinic: {clinic_id}: Retrieving data for procedurelogs?Offset={offset}"
                )
                if response.status >= 400:
                    error_msg = await response.text()
                    logger.error(
                        f"Practice {practice}, Clinic: {clinic_id}: Error retrieving paginated info: {error_msg}"
                    )
                else:
                    res = await response.json()
                    offset += 1000
                    if isinstance(res, list):
                        res_count = len(res)
                        proc_list.extend(res)
                    else:
                        str_res = res
                        res_count = 0
    logger.warning(
        f"Practice {practice}, Clinic: {clinic_id}: Retrieved {len(proc_list)} procedure logs for clinic {clinic_id}"
    )
    patients = set()
    providers = set()
    practice_id = inputjson["practice_id"]
    if "eConnector is not running" in str_res:
        logger.error(
            "Practice {practice}, Clinic: {clinic_id}: Open Dental connector not running for practice "
            + str(practice_id)
        )
    else:
        patients = {str(proc["PatNum"]) for proc in proc_list}
        providers = {proc["ProvNum"] for proc in proc_list}
        last_proc_date_t_stamp = get_latest_datestamp(proc_list)

        upload_result = await upload_list_data(proc_list, inputjson, "procedures")
        upload_success = all(upload_result)

    return {
        "upload_success": upload_success,
        "patients": list(patients),
        "providers": list(providers),
        "last_proc_date_t_stamp": last_proc_date_t_stamp,
    }


@app.activity_trigger(input_name="inputjson")
async def get_patient_info(inputjson: dict) -> None:
    pat_id = str(inputjson["patient_id"])
    clinic_id = inputjson.get("clinic_id", "")
    async with aiohttp.ClientSession() as session:
        tasks = [
            get_resource(
                session=session,
                inputjson=inputjson,
                req_url=f"{API_URL}/patientraces",
                segment="patientraces",
            ),
            get_resource(
                session=session,
                inputjson=inputjson,
                req_url=f"{API_URL}/diseases",
                segment="conditions",
            ),
            get_resource(
                session=session,
                inputjson=inputjson,
                req_url=f"{API_URL}/medicationpats",
                segment="medications",
            ),
            get_resource(
                session=session,
                inputjson=inputjson,
                req_url=f"{API_URL}/perioexams",
                segment="perioexams",
                cb=get_perio_measures,
            ),
            get_resource(
                session=session,
                inputjson=inputjson,
                req_url=f"{API_URL}/claims",
                segment="claims",
                cb=get_claim_proc,
            ),
            get_resource(
                session=session,
                inputjson=inputjson,
                req_url=f"{API_URL}/treatplans",
                segment="treatplans",
            ),
        ]
        if inputjson.get("ingestion_type", "initial") == "incremental":
            tasks.append(
                get_resource(
                    session=session,
                    inputjson=inputjson,
                    req_url=f"{API_URL}/appointments?ClinicNum={clinic_id}",
                    segment="appointments",
                )
            )
            tasks.append(
                get_resource(
                    session=session,
                    inputjson=inputjson,
                    req_url=f"{API_URL}/patients",
                    segment="patients",
                )
            )
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return create_result_msg(results, pat_id)
        except Exception as e:
            logger.error(f"Exception: {e}")
            return []


@app.activity_trigger(input_name="inputjson")
async def get_providers(inputjson: dict) -> None:
    await ingest_new_providers(input_json=inputjson)


@app.activity_trigger(input_name="inputjson")
async def get_paginated_patient_record(inputjson: dict) -> None:
    patient_ids = set(inputjson.get("patients", []))
    base_input = {k: v for k, v in inputjson.items() if k != "patients"}

    resources = {
        "patients": f"{API_URL}/patients",
        "appointments": f"{API_URL}/appointments?ClinicNum={inputjson['clinic_id']}",
    }

    async def process_resource(resource_name, url):
        offset = 0 if resource_name != "conditions" else None
        records = await get_paginated_resource(
            session=session, inputjson=base_input, url=url, offset=offset
        )
        logger.info(
            f"Clinic {inputjson['clinic_id']}: Fetched {len(records)} records from {resource_name}"
        )

        filtered_records = [
            rec for rec in records if str(rec.get("PatNum")) in patient_ids
        ]

        if resource_name == "appointments":
            await upload_list_data(filtered_records, inputjson, "appointments")

        elif resource_name == "patients":
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)
            tasks = []

            async def upload_patient(record):
                async with semaphore:
                    pat_id = record.get("PatNum")
                    base_input["patient_id"] = pat_id
                    upload_path = construct_file_path(base_input, resource_name)
                    redacted_data = get_redacted_phi(record)
                    await safe_upload(
                        payload=json.dumps(redacted_data), upload_path=upload_path
                    )

            for record in filtered_records:
                tasks.append(upload_patient(record))

            await asyncio.gather(*tasks)

    async with aiohttp.ClientSession() as session:
        tasks = [
            process_resource(resource_name, url)
            for resource_name, url in resources.items()
        ]
        await asyncio.gather(*tasks)


@app.activity_trigger(input_name="inputjson")
async def upload_blob(inputjson: list) -> None:
    """
    Uploads multiple blobs to Azure Blob Storage in parallel with concurrency control.
    """
    batch_upload_data = get_batch_data(inputjson)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)
    tasks = []

    async def upload_task(upload_data):
        async with semaphore:
            return await safe_upload(
                upload_path=upload_data["path"], payload=upload_data["data"]
            )

    for upload_data in batch_upload_data:
        tasks.append(upload_task(upload_data))

    return await asyncio.gather(*tasks)
