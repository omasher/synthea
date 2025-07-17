import asyncio
from functools import partial
import json

import aiohttp
from aiohttp.client_exceptions import ContentTypeError

from config import API_URL
from logging_config import logger
from utils import (
    construct_file_path,
    get_redacted_phi,
    get_api_headers,
    upload_file,
)
from queries import UPDATE_INGESTION_HISTORY


async def get_nested_resources(
    session: aiohttp.ClientSession,
    inputjson: dict,
    item_list: list,
    key_name: str,
    segment: str,
    endpoint: str,
) -> list:
    res_list = []
    for item in item_list:
        input_with_item = {**inputjson, **item}
        payload = {key_name: item[key_name]}
        res = await get_resource(
            session=session,
            inputjson=input_with_item,
            req_url=f"{API_URL}/{endpoint}",
            segment=segment,
            params=payload,
        )
        res_list.append((res[0], res[1], item[key_name], res[3]))
    return res_list


get_perio_measures = partial(
    get_nested_resources,
    key_name="PerioExamNum",
    segment="periomeasures",
    endpoint="periomeasures",
)

get_claim_proc = partial(
    get_nested_resources,
    key_name="ClaimNum",
    segment="claimprocs",
    endpoint="claimprocs",
)


async def get_preferences(input_json: dict):
    pref_names = {
        "PracticeAddress": "Address",
        "PracticeAddress2": "Address2",
        "PracticeCity": "City",
        "PracticeST": "State",
        "PracticeZip": "Zip",
        "PracticeTitle": "Description",
        "ProgramVersion": "version",
    }
    org_0 = {"ClinicNum": "0"}
    async with aiohttp.ClientSession() as session:
        for pref, name in pref_names.items():
            async with session.get(
                f"{API_URL}/preferences?PrefName={pref}",
                headers=get_api_headers(input_json),
            ) as response:
                data = await response.json()
                org_0[name] = data[0]["ValueString"]
    return org_0


async def ingest_new_clinics(input_json: dict):
    new_clinics = input_json.get("new_clinics")
    practice = input_json.get("referral_code", "")
    ingestion_blob_path = input_json.get("ingestion_blob_path", "")
    clinics = []
    if "0" in new_clinics:
        org_0 = await get_preferences(input_json)
        clinics.append(org_0)

    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{API_URL}/clinics", headers=get_api_headers(input_json)
        ) as response:
            data = await response.json()
            clinics.extend(data)
        for clc in new_clinics:
            clinic_data = next(
                (c for c in clinics if str(c["ClinicNum"]) == str(clc)), {}
            )
            upload_path = construct_file_path(
                {
                    "referral_code": practice,
                    "ingestion_blob_path": ingestion_blob_path,
                    "resource_id": clc,
                },
                "clinics",
            )
            await upload_file(json.dumps(clinic_data), upload_path=upload_path)


async def get_definitions(input_json: dict, session):
    async with session.get(
        f"{API_URL}/definitions?Category=35", headers=get_api_headers(input_json)
    ) as response:
        definitions = await response.json()
    return {str(df["DefNum"]): df["ItemName"] for df in definitions}


async def get_definitions(input_json: dict, session) -> dict:
    try:
        async with session.get(
            f"{API_URL}/definitions?Category=35", headers=get_api_headers(input_json)
        ) as response:
            response.raise_for_status()
            definitions = await response.json()
            return {str(df["DefNum"]): df["ItemName"] for df in definitions}
    except Exception as e:
        logger.error(f"Failed to fetch definitions: {e}")
        return {}


async def ingest_new_providers(input_json: dict):
    new_providers = input_json["providers"]
    practice = input_json.get("referral_code", "")
    ingestion_blob_path = input_json.get("ingestion_blob_path", "")

    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{API_URL}/providers", headers=get_api_headers(input_json)
        ) as response:
            providers = await response.json()
            definitions = await get_definitions(input_json, session)
        for prov_num in new_providers:
            prov_data = next(
                (
                    prv
                    for prv in providers
                    if str(prv["ProvNum"]) == str(prov_num)
                    and prv.get("IsNotPerson", "false") == "false"
                ),
                None,
            )
            if not prov_data:
                continue
            specialty_key = str(prov_data.get("Specialty", ""))
            if specialty_key in definitions:
                prov_data["SpecialtyName"] = definitions[specialty_key]

            upload_path = construct_file_path(
                {
                    "referral_code": practice,
                    "ingestion_blob_path": ingestion_blob_path,
                    "resource_id": prov_num,
                },
                "providers",
            )
            await upload_file(json.dumps(get_redacted_phi(prov_data)), upload_path=upload_path)


def get_operation_data(input_json: dict, existing_info: dict):
    clinic_id = input_json.get("clinic_id")
    ingestion_id = input_json.get("ingestion_id")
    clinic_ingestion_id = input_json.get("clinic_ingestion_id")
    practice_clinics = input_json.get("practice_clinics", [])

    end_dt = None
    status = None
    info_to_update = {}
    pract_ingestion_complete = False
    if input_json.get("new_clinics"):
        info_to_update["new_clinics"] = input_json.get("new_clinics")
    if clinic_id:
        existing_orgs = existing_info.get("organizations", {})
        info_to_update["organizations"] = existing_orgs
        ingested_clinics = set(existing_orgs.keys())
        ingested_clinics.add(clinic_id)
        if ingested_clinics == set(practice_clinics):
            end_dt = input_json.get("end_dt", None)
            status = "complete"
            pract_ingestion_complete = True
        info_to_update["organizations"][clinic_id] = clinic_ingestion_id
    merged_info = existing_info | info_to_update
    data = (
        status,
        end_dt,
        json.dumps(merged_info),
        ingestion_id,
    )
    return UPDATE_INGESTION_HISTORY, data, pract_ingestion_complete


async def get_resource(
    session: aiohttp.ClientSession,
    inputjson: dict,
    req_url: str,
    segment: str,
    params=None,
    redact_phi=False,
    retries=5,
    delay=2,
    cb=None,
):
    clinic_id = clinic_id = inputjson.get("clinic_id", "")
    practice = inputjson.get("referral_code")
    for attempt in range(retries):
        try:
            pat_id = str(inputjson["patient_id"])
            params = params if params else {"PatNum": pat_id}
            async with session.get(
                req_url, params=params, headers=get_api_headers(inputjson)
            ) as response:
                content_type = response.headers.get("Content-Type", "")
                if "application/json" not in content_type:
                    raise ContentTypeError(
                        response.request_info,
                        response.history,
                        message=f"Unexpected Content-Type: {content_type}",
                    )

                logger.warning(
                    f"Practice {practice}, Clinic: {clinic_id}: Retrieving data for #{pat_id}/{segment}"
                )
                if response.status >= 400:
                    error_msg = await response.text()
                    logger.error(
                        f"Practice {practice}, Clinic: {clinic_id}: Error retrieving data for #{pat_id}/{segment}: {error_msg}"
                    )
                    return (
                        False,
                        segment,
                        {
                            "patient_id": pat_id,
                            "resource": segment,
                            "url": req_url,
                            "reason": str(exc),
                        },
                        None,
                    )
                data = await response.json()
                if isinstance(data, str) and "eConnector is not running" in data:
                    raise Exception(
                        f"Practice {practice}, Clinic: {clinic_id}: ConnectivityError: eConnector is not running"
                    )
                if data:
                    merge = []
                    if redact_phi:
                        data = get_redacted_phi(data)
                    if cb:
                        merge = await cb(session, inputjson, data)
                    file_path = construct_file_path(inputjson, segment)
                    return (True, segment, merge, {"data": data, "path": file_path})
                else:
                    logger.info(
                        f"Practice {practice}, Clinic: {clinic_id}: No data for #{pat_id}/{segment}"
                    )
                    return (True, segment, [], None)

        except Exception as exc:
            logger.warning(
                f"Practice {practice}, Clinic: {clinic_id}: Retrying info for #{pat_id}/{segment} - Attempt {attempt + 1} failed: {exc}"
            )
            if attempt + 1 == retries:
                logger.error(
                    f"Practice {practice}, Clinic: {clinic_id}: Max retries reached for {pat_id}/{segment}: {exc}"
                )
                return (
                    False,
                    segment,
                    {
                        "patient_id": pat_id,
                        "resource": segment,
                        "url": req_url,
                        "reason": str(exc),
                    },
                    None,
                )
            await asyncio.sleep(delay)


async def get_paginated_resource(
    session: aiohttp.ClientSession, inputjson: dict, url: str, offset=0
) -> dict:
    clinic_id = clinic_id = inputjson.get("clinic_id", "")
    practice = inputjson.get("referral_code")
    res_count = 1000
    records = []
    str_res = ""
    logger.warning(f"Retrieving paginated info for {url}")
    while res_count >= 1000:
        if isinstance(offset, int):
            delimiter = "&" if "?" in url else "?"
            req_url = f"{url}{delimiter}Offset={offset}"
        else:
            req_url = url
        async with session.get(
            req_url,
            headers=get_api_headers(inputjson),
        ) as response:
            if response.status >= 400:
                error_msg = await response.text()
                logger.error(
                    f"Practice {practice}, Clinic: {clinic_id}: Error retrieving paginated info: {error_msg}"
                )
                return []
            res = await response.json()
            if isinstance(offset, int):
                offset += 1000
            if isinstance(res, list):
                res_count = len(res)
                records.extend(res)
            else:
                str_res = res
                res_count = 0
    if "eConnector is not running" in str_res:
        logger.error(
            f"Practice {practice}, Clinic: {clinic_id}: Open Dental connector not running "
        )
        return records
    return records
