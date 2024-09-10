from datetime import datetime
from dotenv import load_dotenv

import mysql.connector
import requests
import os

print("[LOCAL] Baixando dados do funil BDR...")

# Carrega as variáveis do arquivo .env
load_dotenv()

# Variáveis de configuração
base_url = "https://crm.rdstation.com/api/v1/deals"
token = os.getenv('RD_CRM_TOKEN')
params = {
    "token": token,
    "limit": 200,
    "page": 1,
    "deal_pipeline_id": os.getenv('RD_BDR_ID')
}

# Função para conectar ao banco de dados MySQL
def connect_to_db():
    try:
        conn = mysql.connector.connect(
            user = os.getenv('LH_DB_USER'),
            password = os.getenv('LH_DB_PASSWORD'),
            host = os.getenv('LH_DB_HOST'),
            database = os.getenv('LH_DB_NAME')
        )
        print("Conexão com o banco de dados MySQL estabelecida.")
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to the database: {err}")
        return None

# Função para criar a tabela, se ela não existir
def create_table_if_not_exists(conn):
    try:
        cursor = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS rd_crm_bdr_deals (
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
        cursor.execute(create_table_query)
        conn.commit()
        print("Tabela rd_crm_bdr_deals criada com sucesso.")
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")

# Função para formatar data, mantendo apenas o componente de data (sem horário)
def format_date_only(datetime_str):
    if datetime_str:
        # Tenta primeiro o formato com fuso horário
        try:
            return datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%f%z").strftime("%Y-%m-%d")
        except ValueError:
            # Se falhar, tenta o formato sem fuso horário (caso haja variação no formato)
            return datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d")
    return None

# Função para converter data no formato DD/MM/YYYY para YYYY-MM-DD
def convert_date(date_str):
    if date_str:
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    return None

# Função para inserir ou atualizar os dados no banco de dados
def insert_or_update_data_to_db(conn, deals):
    try:
        cursor = conn.cursor()
        upsert_query = """
            INSERT INTO rd_crm_bdr_deals (id, name, created_at, win, closed_at, user_name, deal_stage_name, deal_lost_reason_name, deal_source_name, executivo_de_conta, foi_feito_handoff, data_handoff, numero_proposta, marca_do_carro, modelo_do_carro, por_onde_chegou, como_conheceu_carbon, momento_de_compra)
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
        for deal in deals:
            custom_fields = {field["custom_field"]["label"]: field["value"] for field in deal.get("deal_custom_fields", [])}

            id = deal.get("_id")
            name = deal.get("name")
            created_at = format_date_only(deal.get("created_at"))  # Formata a data para YYYY-MM-DD
            win = deal.get("win")
            closed_at = format_date_only(deal.get("closed_at"))  # Formata a data para YYYY-MM-DD
            user_name = deal.get("user", {}).get("name", "")
            deal_stage_name = deal.get("deal_stage", {}).get("name", "")
            deal_lost_reason_name = deal.get("deal_lost_reason", {}).get("name", "")
            deal_source_name = deal.get("deal_source", {}).get("name", "")

            executivo_de_conta = custom_fields.get("Executivo de conta", [])
            if not isinstance(executivo_de_conta, list):
                executivo_de_conta = []
            executivo_de_conta = ", ".join(executivo_de_conta)

            foi_feito_handoff = custom_fields.get("Foi feito handoff?", "")
            data_handoff = convert_date(custom_fields.get("Data Handoff", ""))  # Esta já está no formato adequado
            numero_proposta = custom_fields.get("Número Proposta ", "")
            marca_do_carro = custom_fields.get("Marca do carro", [])
            if not isinstance(marca_do_carro, list):
                marca_do_carro = []
            marca_do_carro = ", ".join(marca_do_carro)

            modelo_do_carro = custom_fields.get("Modelo do carro", "")
            por_onde_chegou = custom_fields.get("Por onde chegou?", [])
            if not isinstance(por_onde_chegou, list):
                por_onde_chegou = []
            por_onde_chegou = ", ".join(por_onde_chegou)

            como_conheceu_carbon = custom_fields.get("Como conheceu a Carbon?", "")

            momento_de_compra = custom_fields.get("Momento de compra", "")

            cursor.execute(
                upsert_query,
                (
                    id, 
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
        print("Banco de dados atualizado com sucesso.")
    except mysql.connector.Error as err:
        print(f"Error inserting or updating data: {err}")

# Função para buscar dados do RD Station
def fetch_rd_station_data(base_url, params):
    all_deals = []
    while True:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            print(f"Dados do RD Station recebidos com sucesso {params['page']}")
            data = response.json()
            all_deals.extend(data['deals'])
            if data.get('has_more'):
                params['page'] += 1
            else:
                break
        else:
            print(f"Error fetching data from RD Station: {response.status_code}")
            break
    return all_deals

def main():
    # Buscar dados do RD Station
    deals = fetch_rd_station_data(base_url, params)
    if deals:
        # Conectar ao banco de dados
        conn = connect_to_db()
        if conn:
            # Criar a tabela, se ela não existir
            create_table_if_not_exists(conn)
            # Inserir ou atualizar os dados na tabela
            insert_or_update_data_to_db(conn, deals)
            conn.close()

if __name__ == "__main__":
    main()