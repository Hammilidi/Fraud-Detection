import logging
from pyhive import hive
import requests
import json
import os
from datetime import datetime

# Set up Logging Function
def setup_api_logging():
    log_directory = "Log/HIVE_Log_Files"
    os.makedirs(log_directory, exist_ok=True)

    # Create a log file with a timestamp in its name
    log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
    log_filepath = os.path.join(log_directory, log_filename)

    # Configure logging to write to the log file
    logging.basicConfig(filename=log_filepath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Get the logger for this script
    api_logger = logging.getLogger(__name__)

    return api_logger

# Connexion à Hive
conn = hive.Connection(host='localhost', port=10000, username='')

# Fonction pour vérifier si une table existe dans Hive
def table_exists(table_name):
    with conn.cursor() as cursor:
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        return bool(cursor.fetchall())

# Fonctions pour récupérer les données depuis les API
def get_transactions():
    try:
        response = requests.get('http://localhost:5500/api/transactions')
        return response.json()
    except Exception as e:
        logging.error(f"Failed to fetch transactions: {e}")
        return []

def get_customers():
    try:
        response = requests.get('http://localhost:5500/api/customers')
        return response.json()
    except Exception as e:
        logging.error(f"Failed to fetch customers: {e}")
        return []

def get_external_data():
    try:
        response = requests.get('http://localhost:5500/api/externalData')
        return response.json()
    except Exception as e:
        logging.error(f"Failed to fetch external data: {e}")
        return []

# Définition des données des API
transactions_data = get_transactions()
customers_data = get_customers()
external_data = get_external_data()


# Insertions des données dans les tables Hive si elles existent ou ont été créées
if table_exists('transactions'):
    with conn.cursor() as cursor:
        try:
            for transaction in transactions_data:
                insert_query = f"INSERT INTO TABLE transactions VALUES ({', '.join(map(str, transaction.values()))})"
                cursor.execute(insert_query)
            logging.info("Transactions data inserted successfully.")
        except Exception as e:
            logging.error(f"Failed to insert transactions data: {e}")

if table_exists('customers'):
    with conn.cursor() as cursor:
        try:
            for customer in customers_data:
                insert_query = f"INSERT INTO TABLE customers VALUES ({', '.join(map(str, customer.values()))})"
                cursor.execute(insert_query)
            logging.info("Customers data inserted successfully.")
        except Exception as e:
            logging.error(f"Failed to insert customers data: {e}")

if table_exists('external_data'):
    with conn.cursor() as cursor:
        try:
            insert_query = f"INSERT INTO TABLE external_data VALUES ('{json.dumps(external_data)}')"
            cursor.execute(insert_query)
            logging.info("External data inserted successfully.")
        except Exception as e:
            logging.error(f"Failed to insert external data: {e}")
