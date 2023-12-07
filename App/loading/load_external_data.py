import requests
from pyhive import hive

def load_data_to_hive():
    # Fetch data from the API
    api_url = "http://localhost:5500/api/externalData"
    response = requests.get(api_url)
    external_data = response.json()

    # Establish connection to Hive
    conn = hive.Connection(host='localhost', port=10000, database='FinTech')
    cursor = conn.cursor()

    # Create Blacklist table if not exists
    cursor.execute('CREATE TABLE IF NOT EXISTS blacklist (id INT, merchant_name STRING) STORED AS ORC')

    # Generate unique id for each record in Blacklist
    id_counter = 1

    # Load data into Blacklist table
    for merchant_name in external_data['blacklist_info']:
        insert_query = f"INSERT INTO TABLE blackliste VALUES ({id_counter}, '{merchant_name}')"
        cursor.execute(insert_query)
        id_counter += 1
    
    # Create ExternalData table if not exists
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS externalData (
            customer_id STRING,
            credit_scores INT,
            fraud_reports INT
        )
        PARTITIONED BY (year INT, month INT, day INT)
        STORED AS ORC
    ''')

    # Load data into ExternalData table
    for customer_id in external_data['credit_scores']:
        credit_score_key = external_data['credit_scores'][customer_id]
        fraud_report_key = external_data['fraud_reports'][customer_id]

        credit_score = credit_score_key if credit_score_key else 0
        fraud_report = fraud_report_key if fraud_report_key else 0

        insert_query = f'''
            INSERT INTO TABLE externalDatae PARTITION (year=2023, month=11, day=29)
            VALUES ('{customer_id}', {credit_score}, {fraud_report})
        '''
        cursor.execute(insert_query)
    
    # Commit and close the connection
    conn.commit()
    conn.close()

if __name__ == "__main__":
    load_data_to_hive()