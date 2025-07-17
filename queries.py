SAVE_INGESTION_HISTORY = """
INSERT INTO ingestion_history (history_id__5f4b45, practice, ingestion_type, status, start_dt)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (history_id__5f4b45) DO NOTHING
RETURNING info;"""

GET_INGESTION_HISTORY = """
            SELECT id, start_dt, status
            FROM ingestion_history
            WHERE history_id__5f4b45 = %s AND status = 'running'
            ORDER BY start_dt DESC
            LIMIT 1;
            """

UPDATE_INGESTION_HISTORY = """
UPDATE ingestion_history
SET status = COALESCE(%s, status), end_dt=%s, info=COALESCE(%s, info)
WHERE history_id__5f4b45 = %s;"""

GET_INGESTION_HISTORY_INFO = """
SELECT info FROM ingestion_history WHERE history_id__5f4b45 = %s;"""

GET_CLINIC_INGESTION_INFO = (
    "SELECT info FROM clinic_ingestion_log WHERE ingestion_id__5734b7 = %s;"
)
SAVE_CLINIC_INGESTION = """
                    INSERT INTO clinic_ingestion_log(practice, clinic, ingestion_type, status, info, start_dt)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING ingestion_id__5734b7;
                    """
UPDATE_CLINIC_INGESTION = """
            UPDATE clinic_ingestion_log SET status =COALESCE(%s, status), end_dt = COALESCE(%s, end_dt)
            WHERE ingestion_id__5734b7 = %s;
            """
UPDATE_CLINIC_INGESTION_PROGRESS = """
            UPDATE clinic_ingestion_log SET info = COALESCE(%s, info), status = %s, end_dt = COALESCE(%s, end_dt)
            WHERE ingestion_id__5734b7 = %s;
            """

UPDATE_CLINIC_INGESTION_INFO = """
                    UPDATE clinic_ingestion_log SET info = %s
                    WHERE ingestion_id__5734b7 = %s;
                    """
GET_CLINIC_INGESTION_METADATA = """
            SELECT ingestion_id__5734b7, info, status, start_dt, end_dt
            FROM clinic_ingestion_log
            WHERE practice = %s AND clinic = %s AND ingestion_type = %s AND status != 'complete'
            ORDER BY start_dt DESC
            LIMIT 1;
            """
GET_PAST_CLINIC_ING_INFO = """
            SELECT info FROM clinic_ingestion_log where ingestion_id__5734b7 = %s;
"""

BULK_INSERT_PATIENT_REFERENCES = """
    INSERT INTO patient_reference_map (patnum, practice_referral_code, hashed_reference)
    VALUES %s
    ON CONFLICT (patnum, practice_referral_code) DO NOTHING
"""
