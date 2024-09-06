from dotenv import load_dotenv

import mysql.connector
from mysql.connector import Error
import requests
import os

# Carrega as variáveis do arquivo .env
load_dotenv()

# Your Trello API credentials
API_KEY = f'{os.getenv("TRELLO_API_KEY")}'
TOKEN = f'{os.getenv("TRELLO_TOKEN")}'
BOARD_ID = f'{os.getenv("TRELLO_BOARD_ID")}'

# Base URL for Trello API
BASE_URL = "https://api.trello.com/1/"

# MySQL connection details
MYSQL_HOST = os.getenv('DB_HOST')
MYSQL_DATABASE = os.getenv('DB_NAME')
MYSQL_USER = os.getenv('DB_USER')
MYSQL_PASSWORD = os.getenv('DB_PASSWORD')

# Create the table if it doesn't exist
def create_table_if_not_exists(cursor):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS trello_cards (
        card_id VARCHAR(255) PRIMARY KEY,
        card_name VARCHAR(255),
        due_date DATETIME,
        list_name VARCHAR(255),
        member_id VARCHAR(255),
        member_name VARCHAR(255)
    );
    """
    cursor.execute(create_table_query)

# Get the lists on the board
def get_lists_on_board(board_id):
    url = f"{BASE_URL}boards/{board_id}/lists"
    query = {
        'key': API_KEY,
        'token': TOKEN,
    }
    response = requests.get(url, params=query)
    return response.json() if response.status_code == 200 else []

# Get all cards on a specific list with manual pagination
def get_all_cards_from_list(list_id, limit=1000):
    all_cards = []
    before = None

    while True:
        query = {
            'key': API_KEY,
            'token': TOKEN,
            'limit': limit
        }
        if before:
            query['before'] = before

        url = f"{BASE_URL}lists/{list_id}/cards"
        response = requests.get(url, params=query)
        cards = response.json()

        if not cards:
            break  # Exit loop if no more cards are returned

        all_cards.extend(cards)
        before = cards[-1]['id']  # Use the ID of the last card to get the next set

        # If fewer than the limit was returned, we reached the end
        if len(cards) < limit:
            break

    return all_cards

# Get the members of a specific card
def get_card_members(card_id):
    url = f"{BASE_URL}cards/{card_id}/members"
    query = {
        'key': API_KEY,
        'token': TOKEN,
    }
    response = requests.get(url, params=query)
    return response.json() if response.status_code == 200 else []

# Insert data into MySQL database
def insert_data_to_mysql(board_id):
    try:
        # Connect to MySQL database
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            database=MYSQL_DATABASE,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Create the table if it doesn't exist
            create_table_if_not_exists(cursor)

            # Retrieve and insert cards and associated data
            lists = get_lists_on_board(board_id)
            for trello_list in lists:
                list_name = trello_list['name']
                cards = get_all_cards_from_list(trello_list['id'])
                
                for card in cards:
                    card_id = card['id']
                    card_name = card['name']
                    due_date = card.get('due')  # Some cards might not have a due date

                    # Retrieve and insert members for each card
                    members = get_card_members(card_id)
                    if not members:  # Handle cards with no members
                        cursor.execute("""
                            INSERT INTO trello_cards (card_id, card_name, due_date, list_name, member_id, member_name) 
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE 
                                card_name=%s, due_date=%s, list_name=%s, member_id=%s, member_name=%s
                        """, 
                        (card_id, card_name, due_date, list_name, None, None, 
                         card_name, due_date, list_name, None, None))
                    else:
                        for member in members:
                            member_id = member['id']
                            member_name = member['fullName']
                            cursor.execute("""
                                INSERT INTO trello_cards (card_id, card_name, due_date, list_name, member_id, member_name) 
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE 
                                    card_name=%s, due_date=%s, list_name=%s, member_id=%s, member_name=%s
                            """, 
                            (card_id, card_name, due_date, list_name, member_id, member_name, 
                             card_name, due_date, list_name, member_id, member_name))

            connection.commit()
            print("Data inserted successfully into MySQL database")

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

if __name__ == "__main__":
    insert_data_to_mysql(BOARD_ID)