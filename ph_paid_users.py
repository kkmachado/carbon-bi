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
        "query": "SELECT\n    arrayMap(number -> plus(toStartOfMonth(assumeNotNull(toDateTime('2024-01-01 00:00:00'))), toIntervalMonth(number)), range(0, plus(coalesce(dateDiff('month', toStartOfMonth(assumeNotNull(toDateTime('2024-01-01 00:00:00'))), toStartOfMonth(assumeNotNull(toDateTime('2024-12-31 23:59:59'))))), 1))) AS date,\n    arrayMap(_match_date -> arraySum(arraySlice(groupArray(count), indexOf(groupArray(day_start) AS _days_for_count, _match_date) AS _index, plus(minus(arrayLastIndex(x -> equals(x, _match_date), _days_for_count), _index), 1))), date) AS total\nFROM\n    (SELECT\n        sum(total) AS count,\n        day_start\n    FROM\n        (SELECT\n            count(DISTINCT e.person_id) AS total,\n            toStartOfMonth(timestamp) AS day_start\n        FROM\n            events AS e SAMPLE 1\n        WHERE\n            and(greaterOrEquals(timestamp, toStartOfMonth(assumeNotNull(toDateTime('2024-01-01 00:00:00')))), lessOrEquals(timestamp, assumeNotNull(toDateTime('2024-12-31 23:59:59'))), equals(event, '$pageview'), ifNull(not(match(toString(properties.$host), '^(localhost|127\\\\.0\\\\.0\\\\.1)($|:)')), 1), notILike(properties.$pathname, '%/landing-pages/previa/%'), notILike(properties.$pathname, '%OneDrive%'), notILike(properties.$pathname, '%C:/%'), notILike(properties.$pathname, '%/render2%'), notILike(properties.$current_url, '%https://carbon-blindados.webflow.io/%'), notILike(properties.$user_id, '%carbonblindados.com.br%'), notILike(properties.$user_id, '%carbon.cars%'), or(notEquals(properties.gclid, NULL), notEquals(properties.fbclid, NULL), notEquals(properties.utm_source, NULL)))\n        GROUP BY\n            day_start)\n    GROUP BY\n        day_start\n    ORDER BY\n        day_start ASC)\nORDER BY\n    arraySum(total) DESC\nLIMIT 50000"
    }
}

# Função para conectar ao banco de dados MySQL
def connect_to_db():
    try:
        conn = mysql.connector.connect(
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME')
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
        CREATE TABLE IF NOT EXISTS ph_paid_users (
            date DATE,
            total INT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        print("Tabela ph_paid_users criada com sucesso.")
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")

# Função para limpar a tabela
def truncate_table(conn):
    try:
        cursor = conn.cursor()
        truncate_query = "TRUNCATE TABLE ph_paid_users"
        cursor.execute(truncate_query)
        conn.commit()
        cursor.close()
        print("Tabela ph_paid_users limpa com sucesso.")
    except mysql.connector.Error as err:
        print(f"Error truncating table: {err}")

# Função para inserir os dados no banco de dados
def insert_data_to_db(conn, overview_data):
    try:
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO ph_paid_users (
            date, total
        ) VALUES (%s, %s)
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
        dates = results[0][0]
        totals = results[0][1]
        
        # Combinar as datas e os totais em pares
        overview_data = list(zip(dates, totals))

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