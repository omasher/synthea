import json
from datetime import timedelta
import azure.durable_functions as df
import azure.functions as func

from config import app
from constants import IngestionHistoryStatus, ADMIN_DB, OPEN_DENTAL_API_KEY
from logging_config import logger
from utils import is_ingestion_allowed, get_next_allowed_time, has_connectivity_error


@app.function_name(name="od-api-ingest-monitor")
@app.queue_trigger(
    arg_name="msg", queue_name="scheduled-ingestions", connection="QUEUE_STORAGE_ACCO"
)
@app.durable_client_input(client_name="client")
async def monitor(
    msg: func.QueueMessage, client: df.DurableOrchestrationClient
) -> None:
    logger.info("Ingest Monitor Function running")
    input_json = msg.get_body().decode("utf-8")
    await client.start_new("orchestrator", None, input_json)


@app.orchestration_trigger(context_name="context")
def orchestrator(context: df.DurableOrchestrationContext):
    input_json = json.loads(context.get_input())
    logger.info(f"Ingest Orchestrator Function running with input: {input_json}")
    retry_options = df.RetryOptions(
        first_retry_interval_in_milliseconds=5000, max_number_of_attempts=3
    )
    now = context.current_utc_datetime
    timestamp = now.strftime("%m-%d-%Y_%H-%M-%S")
    clinic_id = str(input_json.get("clinic_id", None))
    practice_id = str(input_json["practice_id"])
    referral_code = input_json.get("referral_code")
    ingestion_id = input_json.get("ingestion_id", None)
    ingestion_type = input_json.get("ingestion_type", None)
    new_clinics = input_json.get("new_clinics")
    ingestion_blob_path = input_json.get("ingestion_blob_path")
    input_json["dev_token"] = yield context.call_activity(
        "get_token_value", {"secret_name": OPEN_DENTAL_API_KEY}
    )
    input_json["mon_db"] = yield context.call_activity(
        "get_token_value", {"secret_name": ADMIN_DB}
    )
    ingestion_history = yield context.call_activity(
        "get_ingestion_history",
        {
            "ingestion_id": ingestion_id,
            "mon_db": input_json["mon_db"],
        },
    )
    ingestion_history_input = {
        "ingestion_type": ingestion_type,
        "practice_id": practice_id,
        "ingestion_id": ingestion_id,
        "start_dt": now.isoformat(),
        "ingestion_blob_path": ingestion_blob_path,
        "mon_db": input_json["mon_db"],
        "practice_token": input_json["practice_token"],
        "referral_code": referral_code,
        "dev_token": input_json["dev_token"],
        "new_clinics": new_clinics,
    }

    if not ingestion_history:
        yield context.call_activity(
            "save_ingestion_history",
            ingestion_history_input,
        )
    if ingestion_history and new_clinics:
        yield context.call_activity(
            "update_ingestion_history",
            {
                "ingestion_id": ingestion_id,
                "mon_db": input_json["mon_db"],
                "referral_code": referral_code,
                "dev_token": input_json["dev_token"],
                "practice_token": input_json["practice_token"],
                "ingestion_blob_path": ingestion_blob_path,
                "new_clinics": new_clinics,
            },
        )
    if not new_clinics:
        if ingestion_type == IngestionHistoryStatus.INCREMENTAL:
            past_clinic_ing_info = yield context.call_activity(
                "get_clinic_last_dt_stamp",
                {
                    "clinic_ing_id": input_json.get("last_clinic_ingestion_id"),
                    "mon_db": input_json["mon_db"],
                },
            )
            input_json["last_proc_date_t_stamp"] = past_clinic_ing_info.get(
                "last_proc_date_t_stamp", None
            )

        ingestion_metadata = yield context.call_activity(
            "get_clinic_ingestion_metadata",
            {
                "practice_id": practice_id,
                "clinic_id": clinic_id,
                "ingestion_type": ingestion_type,
                "mon_db": input_json["mon_db"],
            },
        )
        clinic_ingestion_id = ingestion_metadata.get("clinic_ingestion_id", None)
        current_patient_idx = ingestion_metadata.get("patients_ingested", 0)
        practice_clinics = ingestion_metadata.get("practice_clinics", [])

        if not clinic_ingestion_id:
            input_data = {
                "ingestion_type": ingestion_type,
                "clinic_id": clinic_id,
                "practice_id": practice_id,
                "start_dt": now.isoformat(),
                "ingestion_blob_path": ingestion_blob_path,
                "mon_db": input_json["mon_db"],
            }
            practice_clinics = input_json.get("practice_clinics", [])
            input_data["practice_clinics"] = practice_clinics
            if ingestion_type == IngestionHistoryStatus.INCREMENTAL:
                input_data["last_proc_date_t_stamp"] = input_json.get(
                    "last_proc_date_t_stamp", ""
                )

            clinic_ingestion_id = yield context.call_activity(
                "save_clinic_ingestion", input_data
            )
        else:
            yield context.call_activity(
                "update_clinic_ingestion_info",
                {
                    "clinic_ingestion_id": clinic_ingestion_id,
                    "status": "running",
                    "mon_db": input_json["mon_db"],
                },
            )
        if not is_ingestion_allowed(now):
            wait_until = get_next_allowed_time(now)
            logger.warning(
                f"Practice {referral_code}: Pausing ingestion for clinic {clinic_id}. Resuming at {wait_until}"
            )
            yield context.create_timer(wait_until)
            # If the ingestion is paused, we continue as new to ensure the state is saved
            context.continue_as_new(json.dumps(input_json))
        logger.warning(
            f"Practice {referral_code}: Processing ingestion for clinic {clinic_id}, current_patient_idx: {current_patient_idx}"
        )

        if current_patient_idx == 0:
            pats_and_provs = yield context.call_activity(
                "get_pats_and_provs", input_json
            )
            if not pats_and_provs.get("upload_success", True):
                wait_until = context.current_utc_datetime + timedelta(minutes=10)
                logger.error(
                    f"Practice {referral_code}: Error uploading providers, retrying at {wait_until}"
                )
                yield context.create_timer(wait_until)
                context.continue_as_new(json.dumps(input_json))
            input_json["last_proc_date_t_stamp"] = pats_and_provs.get(
                "last_proc_date_t_stamp", ""
            )
            input_json["providers"] = pats_and_provs.get("providers", [])
            pats = pats_and_provs.get("patients", [])
            yield context.call_activity(
                "update_clinic_ingestion_progress",
                {
                    "clinic_ingestion_id": clinic_ingestion_id,
                    "patients_ingested": current_patient_idx,
                    "total_patients": len(pats),
                    "patients": list(pats),
                    "mon_db": input_json["mon_db"],
                    "last_proc_date_t_stamp": input_json.get(
                        "last_proc_date_t_stamp", ""
                    ),
                },
            )
        else:
            pats = ingestion_metadata.get("patients", [])
        msg_json = {
            "ingestion_blob_path": input_json["ingestion_blob_path"],
            "practice_id": input_json["practice_id"],
            "clinic_id": input_json["clinic_id"],
            "dev_token": input_json["dev_token"],
            "practice_token": input_json["practice_token"],
            "referral_code": referral_code,
            "mon_db": input_json["mon_db"],
        }
        total_patients = len(pats)
        batch_size = 20

        for i in range(current_patient_idx, total_patients, batch_size):
            now = context.current_utc_datetime
            if not is_ingestion_allowed(now):
                wait_until = get_next_allowed_time(now)
                logger.warning(
                    f"Practice {referral_code}: Pausing ingestion for patient index {current_patient_idx}. Resuming at {wait_until}"
                )
                yield context.call_activity(
                    "update_clinic_ingestion_progress",
                    {
                        "clinic_ingestion_id": clinic_ingestion_id,
                        "patients_ingested": i,
                        "status": "paused",
                        "mon_db": input_json["mon_db"],
                    },
                )
                yield context.create_timer(wait_until)
                context.continue_as_new(json.dump)

            batch_patients = pats[i : i + batch_size]

            tasks = []
            for pat in batch_patients:
                pat_msg_json = {**msg_json, "patient_id": str(pat)}
                tasks.append(
                    context.call_activity_with_retry(
                        "get_patient_info", retry_options, pat_msg_json
                    )
                )
            batch_results = yield context.task_all(tasks)

            has_errors = any(result["errors"] is not None for result in batch_results)

            if has_errors:
                if has_connectivity_error(batch_results):
                    wait_until = context.current_utc_datetime + timedelta(
                        days=1
                    )  # wait until the next day
                    logger.error(
                        f"Practice {referral_code}: Connectivity error encountered. Retrying batch again at {wait_until}"
                    )
                    yield context.create_timer(wait_until)
                    context.continue_as_new(json.dumps(input_json))
            upload_result = yield context.call_activity_with_retry(
                "upload_blob", retry_options, batch_results
            )
            if not all(upload_result):
                wait_until = context.current_utc_datetime + timedelta(minutes=10)
                logger.error(
                    f"Practice {referral_code}: Error uploading patient data, retrying at {wait_until}"
                )
                yield context.create_timer(wait_until)
                context.continue_as_new(json.dumps(input_json))
            current_patient_idx = i + len(batch_patients)

            # Periodically update the ingestion progress
            yield context.call_activity(
                "update_clinic_ingestion_progress",
                {
                    "clinic_ingestion_id": clinic_ingestion_id,
                    "patients_ingested": current_patient_idx,
                    "mon_db": input_json["mon_db"],
                },
            )
            logger.info(
                f"Practice {referral_code}: Clinic {clinic_id}: Processed {current_patient_idx}/{total_patients} patients"
            )
        msg_providers = {**msg_json, "providers": input_json["providers"]}
        logger.warning(f"Practice {referral_code}: Ingesting providers")
        yield context.call_activity_with_retry(
            "get_providers", retry_options, msg_providers
        )
        msg_json["patients"] = pats
        if ingestion_type == "initial":
            logger.warning(
                f"Practice {referral_code}: Ingesting paginated recods (appointments and patients)"
            )
            yield context.call_activity_with_retry(
                "get_paginated_patient_record", retry_options, msg_json
            )
        logger.warning(
            f"Practice {referral_code}: Updating clinic_ingestion_log table with last ingestion state"
        )
        # Updates the table with the last ingestion_state
        now = context.current_utc_datetime
        timestamp = now.isoformat()
        yield context.call_activity_with_retry(
            "update_clinic_ingestion_progress",
            retry_options,
            {
                "clinic_ingestion_id": clinic_ingestion_id,
                "status": "complete",
                "end_dt": timestamp,
                "mon_db": input_json["mon_db"],
            },
        )
        logger.warning(f"Practice {referral_code}: Saving hashed patnum reference map")
        yield context.call_activity_with_retry(
            "insert_patient_ref_map",
            retry_options,
            {
                "mon_db": input_json["mon_db"],
                "referral_code": referral_code,
                "patnums": pats,
            },
        )
        logger.warning("Updating update_ingestion_history")
        pract_ing_compl = yield context.call_activity_with_retry(
            "update_ingestion_history",
            retry_options,
            {
                "ingestion_id": ingestion_id,
                "operation": "ingestion_complete",
                "clinic_ingestion_id": clinic_ingestion_id,
                "end_dt": timestamp,
                "dev_token": input_json["dev_token"],
                "practice_clinics": practice_clinics,
                "practice_token": input_json["practice_token"],
                "clinic_id": clinic_id,
                "mon_db": input_json["mon_db"],
            },
        )
        logger.warning(
            f"Practice {referral_code}: Ingestion complete for clinic %s", clinic_id
        )
        if pract_ing_compl:
            logger.warning(
                f"Practice {referral_code}: Sending queue message to provider reconciliation"
            )
            yield context.call_activity_with_retry(
                "notify_prov_rec",
                retry_options,
                {
                    "practice_id": practice_id,
                    "referral_code": referral_code,
                    "practice_token": input_json["practice_token"],
                    "date": timestamp,
                    "providers": input_json["providers"],
                },
            )
            logger.warning(f"Practice {referral_code}: Notifying transform function")
            yield context.call_activity_with_retry(
                "notify_transform",
                retry_options,
                {
                    "ingestion_path": input_json["ingestion_blob_path"],
                    "practice": input_json["referral_code"],
                },
            )
            logger.warning("Ingestion complete for practice %s", referral_code)
