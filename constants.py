class IngestionHistoryStatus(str):
    """
    Enum-like class for ingestion history statuses.
    """

    INITIAL = "initial"
    INCREMENTAL = "incremental"
    FAILED = "failed"
    RUNNING = "running"
    COMPLETE = "complete"
    SUCCESS = "success"

    @classmethod
    def is_valid(cls, status: str) -> bool:
        return status in cls.__members__.values()


INGESTION_START_YEAR = {"E73M1709": "2014-01-01 00:00:00"}

# We need to tune this based on the function app's plan and testing
MAX_CONCURRENT_UPLOADS = 100
ADMIN_DB = "conn-admin-portal-db"
OPEN_DENTAL_API_KEY = "open-dental-api-key"


REDACT_KEYS = {
    "SSN",
    "FName",
    "LName",
    "MiddleI",
    "Address",
    "Address2",
    "HmPhone",
    "WkPhone",
    "WirelessPhone",
    "Email",
    "MedicaidID",
}
