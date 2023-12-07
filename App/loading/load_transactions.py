import requests, logging
from datetime import datetime
from pyhive import hive

def extract_year_month_day(date_time_str):
    date_time = datetime.fromisoformat(date_time_str)
    return date_time.year, date_time.month, date_time.day


def create_database_if_not_exists(database_name, cursor):
    # Check if the database exists
    cursor.execute(f"SHOW DATABASES LIKE '{database_name}'")
    databases = cursor.fetchall()

    # If the database does not exist, create it
    if not databases:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"Database '{database_name}' created successfully.")


def load_transactions_to_hive():
    try:
        # Fetch data from the API
        api_url = "http://localhost:5500/api/transactions"
        response = requests.get(api_url)
        response.raise_for_status()  # Vérifie si la requête a échoué
        
        transactions_data = response.json()

        # Établir la connexion à Hive
        conn = hive.Connection(host='localhost', port=10000, database='FinTech')
        cursor = conn.cursor()

        # Créer la base de données 'FinTech' si elle n'existe pas
        create_database_if_not_exists('FinTech', cursor)

        # Passer à la base de données 'FinTech'
        cursor.execute('USE FinTech')

        # Configurer Hive en mode de partitionnement dynamique non strict
        cursor.execute("SET hive.exec.dynamic.partition.mode=nonstrict")

        # Créer une table Hive si elle n'existe pas (remplacer les espaces réservés par les noms de colonnes et les types de données réels)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id STRING,
                date_time STRING,
                amount DOUBLE,
                currency STRING,
                merchant_details STRING,
                customer_id STRING,
                transaction_type STRING,
                location STRING
            )
            PARTITIONED BY (year INT, month INT, day INT)
         ''')

        # Préparer une requête d'insertion par lot
        batch_insert_query = '''
            INSERT INTO TABLE transactions PARTITION (year, month, day)
            VALUES {}
        '''

        # Préparer les valeurs pour l'insertion par lot
        values = []
        for transaction in transactions_data:
            year, month, day = extract_year_month_day(transaction['date_time'])
            values.append(f"('{transaction['transaction_id']}', '{transaction['date_time']}', {transaction['amount']}, '{transaction['currency']}', '{transaction['merchant_details']}', '{transaction['customer_id']}', '{transaction['transaction_type']}', '{transaction['location']}', {year}, {month}, {day})")

        # Exécuter l'insertion par lot
        if values:
            values_str = ', '.join(values)
            insert_query = batch_insert_query.format(values_str)
            cursor.execute(insert_query)

        # Commit et fermeture de la connexion
        conn.commit()
        conn.close()

        logging.info("Données des transactions chargées avec succès dans Hive.")
    
    except requests.HTTPError as http_err:
        logging.error(f"Erreur HTTP lors de la récupération des données de l'API: {http_err}")
    
    except Exception as err:
        logging.error(f"Erreur lors du chargement des données dans Hive: {err}")

if __name__ == "__main__":
    load_transactions_to_hive()
    