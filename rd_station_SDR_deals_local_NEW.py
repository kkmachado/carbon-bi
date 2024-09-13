import logging
from datetime import datetime
from dateutil import parser
from dotenv import load_dotenv
import mysql.connector
import requests
import os
import sys
import argparse
from typing import List, Dict, Any
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Validate required environment variables
def validate_env_vars():
    required_vars = ['RD_CRM_TOKEN', 'LH_DB_USER', 'LH_DB_PASSWORD', 'LH_DB_HOST', 'LH_DB_NAME', 'RD_SDR_ID']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logging.error(f"Missing environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

validate_env_vars()

# Global variables
BASE_URL = "https://crm.rdstation.com/api/v1/deals"
TOKEN = os.getenv('RD_CRM_TOKEN')
DB_USER = os.getenv('LH_DB_USER')
DB_PASSWORD = os.getenv('LH_DB_PASSWORD')
DB_HOST = os.getenv('LH_DB_HOST')
DB_NAME = os.getenv('LH_DB_NAME')
RD_SDR_ID = os.getenv('RD_SDR_ID')

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='Process RD Station CRM deals.')
    parser.add_argument('--limit', type=int, default=200, help='Number of records per page.')
    parser.add_argument('--pipeline_id', type=str, default=RD_SDR_ID, help='Deal pipeline ID.')
    return parser.parse_args()

def get_field_value(field_value):
    """Process custom field values uniformly."""
    if isinstance(field_value, list):
        return ", ".join(map(str, field_value))
    elif field_value is not None:
        return str(field_value)
    else:
        return ""

def format_date_only(datetime_str):
    """Format datetime string to date in YYYY-MM-DD format."""
    if datetime_str:
        try:
            return parser.parse(datetime_str).date().isoformat()
        except (ValueError, TypeError) as e:
            logging.error(f"Error parsing date '{datetime_str}': {e}")
    return None

def convert_date(date_str):
    """Convert date string from DD/MM/YYYY to YYYY-MM-DD format."""
    if date_str:
        try:
            return parser.parse(date_str, dayfirst=True).date().isoformat()
        except (ValueError, TypeError) as e:
            logging.error(f"Error converting date '{date_str}': {e}")
    return None

def connect_to_db():
    """Connect to the MySQL database."""
    try:
        conn = mysql.connector.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            database=DB_NAME,
            connection_timeout=30
        )
        logging.info("Connected to the MySQL database.")
        return conn
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to the database: {err}")
        sys.exit(1)

def create_table_if_not_exists(conn):
    """Create the database table if it does not exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS rd_crm_sdr_deals (
        id VARCHAR(255) PRIMARY KEY,
        name VARCHAR(255),
        created_at DATE,
        win BOOLEAN,
        closed_at DATE,
        user_name VARCHAR(255),
        deal_stage_name VARCHAR(255),
        deal_lost_reason_name VARCHAR(255),
        deal_source_name VARCHAR(255),
        executivo_de_conta VARCHAR(255),
        foi_feito_handoff VARCHAR(255),
        data_handoff DATE,
        numero_proposta VARCHAR(255),
        marca_do_carro VARCHAR(255),
        modelo_do_carro VARCHAR(255),
        por_onde_chegou VARCHAR(255),
        como_conheceu_carbon VARCHAR(255),
        momento_de_compra VARCHAR(255)
    );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
            logging.info("Table 'rd_crm_sdr_deals' ensured to exist.")
    except mysql.connector.Error as err:
        logging.error(f"Error creating table: {err}")
        sys.exit(1)

def insert_or_update_data_to_db(conn, deals):
    """Insert or update deal data into the database."""
    upsert_query = """
        INSERT INTO rd_crm_sdr_deals (
            id, name, created_at, win, closed_at, user_name, deal_stage_name,
            deal_lost_reason_name, deal_source_name, executivo_de_conta,
            foi_feito_handoff, data_handoff, numero_proposta, marca_do_carro,
            modelo_do_carro, por_onde_chegou, como_conheceu_carbon, momento_de_compra
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            created_at = VALUES(created_at),
            win = VALUES(win),
            closed_at = VALUES(closed_at),
            user_name = VALUES(user_name),
            deal_stage_name = VALUES(deal_stage_name),
            deal_lost_reason_name = VALUES(deal_lost_reason_name),
            deal_source_name = VALUES(deal_source_name),
            executivo_de_conta = VALUES(executivo_de_conta),
            foi_feito_handoff = VALUES(foi_feito_handoff),
            data_handoff = VALUES(data_handoff),
            numero_proposta = VALUES(numero_proposta),
            marca_do_carro = VALUES(marca_do_carro),
            modelo_do_carro = VALUES(modelo_do_carro),
            por_onde_chegou = VALUES(por_onde_chegou),
            como_conheceu_carbon = VALUES(como_conheceu_carbon),
            momento_de_compra = VALUES(momento_de_compra);
    """
    try:
        with conn.cursor() as cursor:
            for deal in deals:
                custom_fields = {field["custom_field"]["label"]: field["value"] for field in deal.get("deal_custom_fields", [])}

                deal_id = deal.get("_id")
                name = deal.get("name")
                created_at = format_date_only(deal.get("created_at"))
                win = deal.get("win")
                closed_at = format_date_only(deal.get("closed_at"))
                user_name = deal.get("user", {}).get("name", "")
                deal_stage_name = deal.get("deal_stage", {}).get("name", "")
                deal_lost_reason_name = deal.get("deal_lost_reason", {}).get("name", "")
                deal_source_name = deal.get("deal_source", {}).get("name", "")

                executivo_de_conta = get_field_value(custom_fields.get("Executivo de conta"))
                foi_feito_handoff = get_field_value(custom_fields.get("Foi feito handoff?"))
                data_handoff = convert_date(custom_fields.get("Data Handoff"))
                numero_proposta = get_field_value(custom_fields.get("Número Proposta "))
                marca_do_carro = get_field_value(custom_fields.get("Marca do carro"))
                modelo_do_carro = get_field_value(custom_fields.get("Modelo do carro"))
                por_onde_chegou = get_field_value(custom_fields.get("Por onde chegou?"))
                como_conheceu_carbon = get_field_value(custom_fields.get("Como conheceu a Carbon?"))
                momento_de_compra = get_field_value(custom_fields.get("Momento de compra"))

                cursor.execute(
                    upsert_query,
                    (
                        deal_id,
                        name,
                        created_at,
                        win,
                        closed_at,
                        user_name,
                        deal_stage_name,
                        deal_lost_reason_name,
                        deal_source_name,
                        executivo_de_conta,
                        foi_feito_handoff,
                        data_handoff,
                        numero_proposta,
                        marca_do_carro,
                        modelo_do_carro,
                        por_onde_chegou,
                        como_conheceu_carbon,
                        momento_de_compra
                    )
                )
            conn.commit()
            logging.info("Database updated successfully.")
    except mysql.connector.Error as err:
        logging.error(f"Error inserting or updating data: {err}")
        sys.exit(1)

def fetch_rd_station_data(base_url, params):
    """Fetch deal data from RD Station CRM API."""
    all_deals = []
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('https://', adapter)
    page = 1
    while True:
        params['page'] = page
        try:
            response = session.get(base_url, params=params, timeout=10)
            if response.status_code == 200:
                logging.info(f"Received RD Station data successfully, page {page}")
                data = response.json()
                all_deals.extend(data['deals'])
                if data.get('has_more'):
                    page += 1
                else:
                    break
            else:
                logging.error(f"Error fetching data from RD Station: HTTP {response.status_code}")
                break
        except requests.exceptions.RequestException as e:
            logging.error(f"Request exception: {e}")
            break
    return all_deals

def main():
    """Main function to orchestrate data fetching and updating."""
    args = parse_arguments()

    # Prepare parameters for the API call
    params = {
        "token": TOKEN,
        "limit": args.limit,
        "deal_pipeline_id": args.pipeline_id
    }

    try:
        # Fetch data from RD Station
        logging.info("Downloading data from SDR funnel...")
        deals = fetch_rd_station_data(BASE_URL, params)
        if deals:
            # Connect to the database
            conn = connect_to_db()
            if conn:
                # Ensure the table exists
                create_table_if_not_exists(conn)
                # Insert or update the data
                insert_or_update_data_to_db(conn, deals)
                conn.close()
        else:
            logging.warning("No deals were fetched from RD Station.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()