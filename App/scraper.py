import json
import requests

# Fetch transactions data
def get_transactions():
    try:
        response = requests.get('http://localhost:5500/api/transactions')
        return response.json()
    except Exception as e:
        print(f"Failed to fetch transactions: {e}")
        return []

# Fetch customers data
def get_customers():
    try:
        response = requests.get('http://localhost:5500/api/customers')
        return response.json()
    except Exception as e:
        print(f"Failed to fetch customers: {e}")
        return []

# Fetch external data
def get_external_data():
    try:
        response = requests.get('http://localhost:5500/api/externalData')
        return response.json()
    except Exception as e:
        print(f"Failed to fetch external data: {e}")
        return []

# Store data in JSON files
def store_data():
    transactions_data = get_transactions()
    customers_data = get_customers()
    external_data = get_external_data()

    # Store transactions data
    with open('Data/transactions_data.json', 'w') as transactions_file:
        json.dump(transactions_data, transactions_file, indent=4)

    # Store customers data
    with open('Data/customers_data.json', 'w') as customers_file:
        json.dump(customers_data, customers_file, indent=4)

    # Store external data
    with open('Data/external_data.json', 'w') as external_file:
        json.dump(external_data, external_file, indent=4)

# Execute the data storing process
if __name__ == "__main__":
    store_data()
