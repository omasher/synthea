import json
import psycopg2
import psycopg2.extras


from config import app
from logging_config import logger
from ingestion_helpers import (
    get_operation_data,
    ingest_new_clinics,
)
from db import get_db_connection

from queries import (
    SAVE_INGESTION_HISTORY,
    GET_INGESTION_HISTORY_INFO,
    GET_CLINIC_INGESTION_INFO,
    UPDATE_CLINIC_INGESTION_PROGRESS,
    GET_CLINIC_INGESTION_METADATA,
    GET_INGESTION_HISTORY,
    UPDATE_CLINIC_INGESTION_INFO,
    SAVE_CLINIC_INGESTION,
    GET_PAST_CLINIC_ING_INFO,
    BULK_INSERT_PATIENT_REFERENCES,
)
from utils import get_hash_reference


@app.activity_trigger(input_name="inputjson")
def save_clinic_ingestion(inputjson: dict):
    practice_id = inputjson.get("practice_id")
    clinic_id = inputjson.get("clinic_id")
    ingestion_type = inputjson.get("ingestion_type")
    start_dt = inputjson.get("start_dt")
    ingestion_blob_path = inputjson.get("ingestion_blob_path", None)
    last_proc_date_t_stamp = inputjson.get("last_proc_date_t_stamp", None)
    practice_clinics = inputjson.get("practice_clinics", [])

    info_to_save = {}
    if ingestion_blob_path:
        info_to_save["ingestion_blob_path"] = ingestion_blob_path
    if last_proc_date_t_stamp:
        info_to_save["last_proc_date_t_stamp"] = last_proc_date_t_stamp
    if practice_clinics:
        info_to_save["practice_clinics"] = practice_clinics
    with get_db_connection(inputjson["mon_db"]) as conn:
        cur = conn.cursor()
        cur.execute(
            SAVE_CLINIC_INGESTION,
            (
                practice_id,
                clinic_id,
                ingestion_type,
                "running",
                json.dumps(info_to_save),
                start_dt,
            ),
        )
        ingestion_id = cur.fetchone()[0]
        conn.commit()
    return ingestion_id


@app.activity_trigger(input_name="inputjson")
def update_clinic_ingestion_info(inputjson: dict):
    ing_state = inputjson.get("ing_state")
    clinic_ingestion_id = inputjson.get("clinic_ingestion_id")
    with get_db_connection(inputjson["mon_db"]) as conn:
        cur = conn.cursor()
        cur.execute(
            UPDATE_CLINIC_INGESTION_INFO,
            (json.dumps(ing_state), clinic_ingestion_id),
        )
        conn.commit()
    logger.warning(f"Clinic {clinic_ingestion_id} state saved to database")


@app.activity_trigger(input_name="inputjson")
def get_clinic_ingestion_metadata(inputjson: dict):
    practice_id = inputjson.get("practice_id")
    clinic_id = inputjson.get("clinic_id")
    ingestion_type = inputjson.get("ingestion_type")
    with get_db_connection(inputjson["mon_db"]) as conn:
        cur = conn.cursor()
        cur.execute(
            GET_CLINIC_INGESTION_METADATA,
            (practice_id, clinic_id, ingestion_type),
        )
        result = cur.fetchone()
        if result:
            ingestion_id, info_json, status, start_dt, end_dt = result

            if isinstance(info_json, str):
                info = json.loads(info_json)
            elif isinstance(info_json, dict):
                info = info_json
            else:
                info = {}

            return {
                "clinic_ingestion_id": ingestion_id,
                "patients_ingested": info.get("patients_ingested", 0),
                "patients": info.get("patients", []),
                "practice_clinics": info.get("practice_clinics", []),
                "orchestrator_instance_id": info.get("orchestrator_instance_id", None),
                "status": status,
            }
        return {}


@app.activity_trigger(input_name="inputjson")
def get_ingestion_history(inputjson: dict):
    """
    Retrieves the ingestion history for a given practice and ingestion type.
    """
    history_id = inputjson.get("ingestion_id")

    with get_db_connection(inputjson["mon_db"]) as conn:
        cur = conn.cursor()
        cur.execute(
            GET_INGESTION_HISTORY,
            (history_id,),
        )
        results = cur.fetchone()

    if results:
        ingestion_id, start_dt, status = results
        return {
            "ingestion_id": ingestion_id,
            "start_dt": start_dt.isoformat(),
            "status": status,
        }
    return {}


@app.activity_trigger(input_name="inputjson")
def update_clinic_ingestion_progress(inputjson: dict):
    clinic_ingestion_id = inputjson["clinic_ingestion_id"]
    clinic_id = inputjson.get("clinic_id")
    patients_ingested = inputjson.get("patients_ingested", 0)
    status = inputjson.get("status", "running")  # Default to running
    total_patients = inputjson.get("total_patients", 0)  # New
    patients = inputjson.get("patients", [])
    last_proc_date_t_stamp = inputjson.get("last_proc_date_t_stamp", None)
    end_dt = inputjson.get("end_dt", None)

    info_to_update = {}
    if last_proc_date_t_stamp:
        info_to_update["last_proc_date_t_stamp"] = last_proc_date_t_stamp
    if patients_ingested:
        info_to_update["patients_ingested"] = patients_ingested
    if total_patients:
        info_to_update["total_patients"] = total_patients
    if patients:
        info_to_update["patients"] = list(patients)

    with get_db_connection(inputjson["mon_db"]) as conn:
        cur = conn.cursor()
        cur.execute(
            GET_CLINIC_INGESTION_INFO,
            (clinic_ingestion_id,),
        )
        row = cur.fetchone()
        existing_info = row[0] if row and row[0] else {}
        if status == "complete":
            existing_info.pop("patients", None)
        merged_info = {**existing_info, **info_to_update}

        cur.execute(
            UPDATE_CLINIC_INGESTION_PROGRESS,
            (json.dumps(merged_info), status, end_dt, clinic_ingestion_id),
        )
        conn.commit()
    logger.info(
        f"Ingestion progress for {clinic_id} updated: patients={patients_ingested}, status={status}"
    )


@app.activity_trigger(input_name="inputjson")
async def save_ingestion_history(inputjson: dict) -> None:
    """
    Saves the ingestion history to the database.
    """
    new_clinics = inputjson.get("new_clinics", [])
    data = (
        inputjson["ingestion_id"],
        inputjson["practice_id"],
        inputjson.get("ingestion_type", "initial"),
        inputjson.get("status", "running"),
        inputjson.get("start_dt"),
    )
    try:
        with get_db_connection(inputjson["mon_db"]) as conn:
            cur = conn.cursor()
            cur.execute(SAVE_INGESTION_HISTORY, data)
            conn.commit()
            if new_clinics:
                await ingest_new_clinics(inputjson)

        logger.info("Ingestion history saved successfully.")
    except KeyError as e:
        logger.warning(f"save_ingestion_history: Missing required input key: {e}")
    except psycopg2.errors.UniqueViolation as e:
        logger.warning(
            f"Ingestion history already exists for this practice and type: \n{e}"
        )
    except Exception as e:
        logger.exception(f"Failed to save ingestion history: {e}")


@app.activity_trigger(input_name="inputjson")
async def update_ingestion_history(inputjson: dict) -> None:
    """
    Updates the ingestion history in the database.
    """
    ingestion_id = inputjson["ingestion_id"]
    new_clinics = inputjson.get("new_clinics")
    pract_ing_compl = False

    try:
        if new_clinics:
            await ingest_new_clinics(inputjson)
        with get_db_connection(inputjson["mon_db"]) as conn:
            cur = conn.cursor()
            existing_info = {}
            cur.execute(GET_INGESTION_HISTORY_INFO, (ingestion_id,))
            row = cur.fetchone()
            existing_info = row[0] if row and row[0] else {}
            logger.debug(
                f"Existing info for ingestion_id {ingestion_id}: {existing_info}"
            )
            query, data, pract_ing_compl = get_operation_data(inputjson, existing_info)
            cur.execute(query, data)
            conn.commit()
    except KeyError as e:
        logger.warning(
            f"Missing required input key while updating ingestion history: \n {e}"
        )
    except Exception as e:
        logger.exception(f"Failed to update ingestion history: {e}")
    finally:
        return pract_ing_compl


@app.activity_trigger(input_name="inputjson")
def get_clinic_last_dt_stamp(inputjson: dict) -> None:
    """
    Retrieves the past ingestion info of a clinic.
    """
    clinic_ing_id = inputjson.get("clinic_ing_id")

    with get_db_connection(inputjson["mon_db"]) as conn:
        cur = conn.cursor()
        cur.execute(
            GET_PAST_CLINIC_ING_INFO,
            (clinic_ing_id,),
        )
        row = cur.fetchone()
        info = row[0] if row and row[0] else {}

    return {"last_proc_date_t_stamp": info.get("last_proc_date_t_stamp", None)}


@app.activity_trigger(input_name="inputjson")
def insert_patient_ref_map(inputjson: dict) -> None:
    referral_code = inputjson["referral_code"]
    patnums = inputjson["patnums"]
    patients = map(lambda p: (p, referral_code, get_hash_reference(p)), patnums)
    with get_db_connection(inputjson["mon_db"]) as conn:
        cur = conn.cursor()
        psycopg2.extras.execute_values(
            cur, BULK_INSERT_PATIENT_REFERENCES, patients, page_size=1000
        )
        conn.commit()
