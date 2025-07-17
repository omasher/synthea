import psycopg2
from contextlib import contextmanager

@contextmanager
def get_db_connection(db_con_str: str):
    conn = None 
    try:
        conn = psycopg2.connect(db_con_str)
        yield conn 
    finally:
        if conn:
            conn.close()
