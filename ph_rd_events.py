import json
import os
from dotenv import load_dotenv

import mysql.connector
import requests

# Carrega as variáveis do arquivo .env
load_dotenv()

# Configuração da API PostHog
api_url = "https://app.posthog.com/api/projects/41743/query/"
headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {os.environ["PH_TOKEN"]}'
}
payload = {
    "query": {
        "kind": "HogQLQuery",
        "query": """SELECT 
                        formatDateTime(timestamp, '%Y-%m-%d') AS data, 
                        properties.Origem AS origem, 
                        COUNT(*) AS total 
                    FROM events 
                    WHERE event = 'RD Station' 
                    AND data <= formatDateTime(today(),'%Y-%m-%d') 
                    AND data > formatDateTime(toStartOfYear(today()), '%Y-%m-%d') 
                    GROUP BY data, origem 
                    ORDER BY data DESC, total DESC 
                    LIMIT 10000"""
    }
}

# Função para conectar ao banco de dados MySQL
def connect_to_db():
    try:
        conn = mysql.connector.connect(
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            host=os.environ['DB_HOST'],
            database=os.environ['DB_NAME']
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
        CREATE TABLE IF NOT EXISTS ph_rd_events (
            data DATE,
            origem VARCHAR(255),
            total INT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        print("Tabela ph_rd_events criada com sucesso.")
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")

# Função para limpar a tabela
def truncate_table(conn):
    try:
        cursor = conn.cursor()
        truncate_query = "TRUNCATE TABLE ph_rd_events"
        cursor.execute(truncate_query)
        conn.commit()
        cursor.close()
        print("Tabela ph_rd_events limpa com sucesso.")
    except mysql.connector.Error as err:
        print(f"Error truncating table: {err}")

# Função para inserir os dados no banco de dados
def insert_data_to_db(conn, events_data):
    try:
        cursor = conn.cursor()
        insert_query = "INSERT INTO ph_rd_events (data, origem, total) VALUES (%s, %s, %s)"
        cursor.executemany(insert_query, events_data)
        conn.commit()
        cursor.close()
        print("Banco de dados atualizado com sucesso.")
    except mysql.connector.Error as err:
        print(f"Error inserting data: {err}")

# Função para buscar dados da API do PostHog
def fetch_posthog_data(api_url, headers, payload):
    response = requests.post(api_url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        print("Conexão com PostHog estabelecida.")
        return response.json()
    else:
        print(f"Error fetching data from PostHog: {response.status_code}")
        return None

def main():
    # Buscar dados da API do PostHog
    data = fetch_posthog_data(api_url, headers, payload)
    if data:
        results = data['results']
        events_data = [(result[0], result[1], result[2]) for result in results]

        # Conectar ao banco de dados
        conn = connect_to_db()
        if conn:
            # Criar a tabela, se ela não existir
            create_table_if_not_exists(conn)
            # Limpar a tabela
            truncate_table(conn)
            # Inserir os dados na tabela
            insert_data_to_db(conn, events_data)
            conn.close()

if __name__ == "__main__":
    main()
