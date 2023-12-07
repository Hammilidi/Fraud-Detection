import requests
from pyhive import hive

def load_customers_to_hive():
    # Fetch data from the API
    api_url = "http://localhost:5500/api/customers"
    response = requests.get(api_url)
    customers_data = response.json()

    # Establish connection to Hive
    conn = hive.Connection(host='localhost', port=10000, database='FinTech')
    cursor = conn.cursor()

    # Create Hive table if not exists
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            customer_id STRING,
            age INT,
            location STRING,
            avg_transaction_value DOUBLE,
            account_history ARRAY<STRING>
        )
        STORED AS ORC
    ''')

    # Load data into Hive table using SELECT statement
    for customer in customers_data:
        # Replace placeholders with actual column names and values
        insert_query = f'''
            INSERT INTO TABLE customers
            SELECT
                '{customer['customer_id']}',
                {customer['demographics']['age']},
                '{customer['demographics']['location']}',
                {customer['behavioral_patterns']['avg_transaction_value']},
                ARRAY('{",".join(customer['account_history'])}')
        '''
        cursor.execute(insert_query)
    
    # Commit and close the connection
    conn.commit()
    conn.close()

if __name__ == "__main__":
    load_customers_to_hive()