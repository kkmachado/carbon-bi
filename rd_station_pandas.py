import requests
import pandas as pd
import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

url = "https://crm.rdstation.com/api/v1/deals"

token = os.getenv('RD_CRM_TOKEN')

params = {
    "token": token,
    "limit": 200,
    "page": 1
}

def get_rd_station_data (url):
    response = requests.get(url, params=params)
    return response

rd_station_response = get_rd_station_data(url)

rd_station_data = pd.DataFrame(rd_station_response.json()['deals'], columns=['deal_source'])

print(rd_station_data)