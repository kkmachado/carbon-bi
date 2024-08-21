from dotenv import load_dotenv

import mysql.connector
from mysql.connector import Error
import requests
import os

print(os.getenv("TRELLO_API_KEY"))