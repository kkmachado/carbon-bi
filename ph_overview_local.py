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
    'Authorization': f'Bearer {os.getenv("PH_TOKEN")}'
}
payload = {
    "query": {
        "kind": "HogQLQuery",
        "query": """
        SELECT
            e.data,
            e.pageviews,
            s.sessions,
            u.users,
            d.avg_session_duration
        FROM
            (SELECT 
                formatDateTime(timestamp, '%Y-%m-%d') AS data,
                COUNT(*) AS pageviews
            FROM events
            WHERE event = '$pageview' 
              AND formatDateTime(timestamp, '%Y-%m-%d') <= formatDateTime(today(), '%Y-%m-%d')
              AND formatDateTime(timestamp, '%Y-%m-%d') > formatDateTime(toStartOfYear(today()), '%Y-%m-%d')
            GROUP BY data) e
        LEFT JOIN
            (SELECT 
                formatDateTime($start_timestamp, '%Y-%m-%d') AS data,
                COUNT(*) AS sessions
            FROM sessions
            WHERE formatDateTime($start_timestamp, '%Y-%m-%d') <= formatDateTime(today(), '%Y-%m-%d')
              AND formatDateTime($start_timestamp, '%Y-%m-%d') > formatDateTime(toStartOfYear(today()), '%Y-%m-%d')
            GROUP BY data) s
        ON e.data = s.data
        LEFT JOIN
            (SELECT 
                formatDateTime($start_timestamp, '%Y-%m-%d') AS data,
                COUNT(DISTINCT distinct_id) AS users
            FROM sessions
            WHERE formatDateTime($start_timestamp, '%Y-%m-%d') <= formatDateTime(today(), '%Y-%m-%d')
              AND formatDateTime($start_timestamp, '%Y-%m-%d') > formatDateTime(toStartOfYear(today()), '%Y-%m-%d')
            GROUP BY data) u
        ON e.data = u.data
        LEFT JOIN
            (SELECT 
                formatDateTime($start_timestamp, '%Y-%m-%d') AS data,
                AVG($session_duration) AS avg_session_duration
            FROM sessions
            WHERE formatDateTime($start_timestamp, '%Y-%m-%d') <= formatDateTime(today(), '%Y-%m-%d')
              AND formatDateTime($start_timestamp, '%Y-%m-%d') > formatDateTime(toStartOfYear(today()), '%Y-%m-%d')
            GROUP BY data) d
        ON e.data = d.data
        ORDER BY e.data DESC
        LIMIT 10000
        """
    }
}

# Função para conectar ao banco de dados MySQL
def connect_to_db():
    try:
        conn = mysql.connector.connect(
            user=os.getenv('LH_DB_USER'),
            password=os.getenv('LH_DB_PASSWORD'),
            host=os.getenv('LH_DB_HOST'),
            database=os.getenv('LH_DB_NAME')
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
        CREATE TABLE IF NOT EXISTS ph_overview (
            data DATE,
            pageviews INT,
            sessions INT,
            users INT,
            avg_session_duration FLOAT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        print("Tabela ph_overview criada com sucesso.")
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")

# Função para limpar a tabela
def truncate_table(conn):
    try:
        cursor = conn.cursor()
        truncate_query = "TRUNCATE TABLE ph_overview"
        cursor.execute(truncate_query)
        conn.commit()
        cursor.close()
        print("Tabela ph_overview limpa com sucesso.")
    except mysql.connector.Error as err:
        print(f"Error truncating table: {err}")

# Função para inserir os dados no banco de dados
def insert_data_to_db(conn, overview_data):
    try:
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO ph_overview (
            data, pageviews, sessions, users, avg_session_duration
        ) VALUES (%s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, overview_data)
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
        overview_data = [(result[0], result[1], result[2], result[3], result[4]) for result in results]

        # Conectar ao banco de dados
        conn = connect_to_db()
        if conn:
            # Criar a tabela, se ela não existir
            create_table_if_not_exists(conn)
            # Limpar a tabela
            truncate_table(conn)
            # Inserir os dados na tabela
            insert_data_to_db(conn, overview_data)
            conn.close()

if __name__ == "__main__":
    main()