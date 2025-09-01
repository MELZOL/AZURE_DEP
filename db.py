import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()

SERVER   = os.getenv("AZURE_SQL_SERVER")
DB       = os.getenv("AZURE_SQL_DB")
USER     = os.getenv("AZURE_SQL_USERNAME")
PWD      = os.getenv("AZURE_SQL_PASSWORD")
DRIVER   = os.getenv("AZURE_SQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

if not all([SERVER, DB, USER, PWD]):
    raise RuntimeError("Missing one or more required env vars: AZURE_SQL_SERVER, AZURE_SQL_DB, AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD")

HOST = f"{SERVER}.database.windows.net"

odbc_str = (
    f"Driver={{{DRIVER}}};"
    f"Server=tcp:{HOST},1433;"
    f"Database={DB};"
    f"Uid={USER};"
    f"Pwd={PWD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)
connection_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"

engine: Engine = create_engine(
    connection_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=2,
    future=True,
)

def fetch_rows(sql: str, params: dict | None = None):
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        cols = result.keys()
        return [dict(zip(cols, row)) for row in result.fetchall()]
