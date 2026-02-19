"""
Snowflake Incremental Monthly Upload Script
Dataset: Sales/Invoice Data
Credentials loaded from environment variables (GitHub Secrets)
"""

import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file (if exists)
# ── Logging Setup ────────────────────────────────────────────────
LOG_FILE = f"D:\\snowflake-incremental-pipeline\\logs\\snowflake_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

os.makedirs("logs", exist_ok=True)

file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
console_handler = logging.StreamHandler()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[file_handler, console_handler]
)

log = logging.getLogger(__name__)


# ── Snowflake Config — reads from environment variables ──────────
SNOWFLAKE_CONFIG = {
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database":  os.getenv("SNOWFLAKE_DATABASE"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA")
}


TABLE_NAME  = "RAW_SALES"
CSV_FILE    = r"d:\snowflake-incremental-pipeline\python\online_sales_dataset.csv"
DATE_COLUMN = "InvoiceDate"


# ── Step 1: Connect to Snowflake ─────────────────────────────────
def get_connection():
    log.info("Connecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    log.info("✅ Connected successfully!")
    return conn


# ── Step 2: Create Table If Not Exists ───────────────────────────
def create_table_if_not_exists(conn):
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            InvoiceNo          VARCHAR,
            StockCode          VARCHAR,
            Description        VARCHAR,
            Quantity           NUMBER,
            InvoiceDate        VARCHAR,
            UnitPrice          FLOAT,
            CustomerID         FLOAT,
            Country            VARCHAR,
            Discount           FLOAT,
            PaymentMethod      VARCHAR,
            ShippingCost       FLOAT,
            Category           VARCHAR,
            SalesChannel       VARCHAR,
            ReturnStatus       VARCHAR,
            ShipmentProvider   VARCHAR,
            WarehouseLocation  VARCHAR,
            OrderPriority      VARCHAR
        )
    """)
    log.info(f"✅ Table '{TABLE_NAME}' ready.")
    cursor.close()


# ── Step 3: Get Already Loaded Months from Snowflake ─────────────
def get_existing_months(conn):
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT DISTINCT
                YEAR(TO_TIMESTAMP(InvoiceDate, 'YYYY-MM-DD HH24:MI:SS'))  AS yr,
                MONTH(TO_TIMESTAMP(InvoiceDate, 'YYYY-MM-DD HH24:MI:SS')) AS mn
            FROM {TABLE_NAME}
        """)
        results = cursor.fetchall()
        existing = set(results)
        log.info(f"📦 Months already in Snowflake: {existing if existing else 'None (empty table)'}")
        return existing
    except Exception as e:
        log.warning(f"Could not fetch existing months (table may be empty): {e}")
        return set()
    finally:
        cursor.close()


# ── Step 4: Load Dataset ─────────────────────────────────────────
def load_dataset(filepath):
    log.info(f"📂 Loading dataset from: {filepath}")
    df = pd.read_csv(filepath)

    # Parse date column — format: 2020-01-01 00:00
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], format="%Y-%m-%d %H:%M")

    # Add helper columns for grouping BEFORE converting to string
    df["_year"]  = df[DATE_COLUMN].dt.year
    df["_month"] = df[DATE_COLUMN].dt.month

    # Convert to string format Snowflake accepts: YYYY-MM-DD HH:MM:SS
    df[DATE_COLUMN] = df[DATE_COLUMN].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Uppercase column names for Snowflake compatibility
    df.columns = [c.upper() if not c.startswith("_") else c for c in df.columns]

    log.info(f"✅ Dataset loaded: {len(df)} rows | Date range: {df['INVOICEDATE'].min()} → {df['INVOICEDATE'].max()}")
    return df


# ── Step 5: Upload Only One New Month then Stop ───────────────────
def upload_new_months(conn, df):
    existing_months = get_existing_months(conn)

    month_groups   = df.groupby(["_year", "_month"])
    uploaded_count = 0
    skipped_count  = 0

    for (yr, mn), group_df in month_groups:
        yr, mn = int(yr), int(mn)
        month_label = f"{yr}-{mn:02d} ({datetime(yr, mn, 1).strftime('%B %Y')})"

        if (yr, mn) in existing_months:
            log.info(f"⏭️  SKIP   | {month_label} — already loaded ({len(group_df)} rows)")
            skipped_count += 1
            continue

        upload_df = group_df.drop(columns=["_year", "_month"]).reset_index(drop=True)
        log.info(f"⬆️  UPLOAD | {month_label} — {len(upload_df)} rows uploading...")

        try:
            success, _, num_rows, _ = write_pandas(conn, upload_df, TABLE_NAME, auto_create_table=False)
            if success:
                log.info(f"✅  DONE   | {month_label} — {num_rows} rows uploaded successfully!")
                log.info(f"🛑 STOPPED | One month per run. Next month uploads tomorrow.")
                uploaded_count += 1
            else:
                log.error(f"❌  FAILED | {month_label} — write_pandas returned failure")
        except Exception as e:
            log.error(f"❌  ERROR  | {month_label} — {str(e)}")

        break  # Stop after first new month

    log.info("=" * 60)
    log.info(f"📊 UPLOAD SUMMARY")
    log.info(f"   ✅ Uploaded : {uploaded_count} month(s)")
    log.info(f"   ⏭️  Skipped  : {skipped_count} month(s) (already existed)")
    log.info("=" * 60)

    if uploaded_count == 0:
        log.info("🎉 Snowflake is already up to date — nothing new to upload!")


# ── MAIN ─────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("🚀 Starting Snowflake Monthly Incremental Upload")
    log.info("=" * 60)

    conn = get_connection()
    try:
        create_table_if_not_exists(conn)
        df = load_dataset(CSV_FILE)
        upload_new_months(conn, df)
    finally:
        conn.close()
        log.info("🔒 Snowflake connection closed.")


if __name__ == "__main__":
    main()