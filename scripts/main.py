import requests
import threading
from loading import insert_customers_into_hive, insert_transactions_into_hive, insert_external_data_into_hive

# Define your API endpoints
customers_endpoint = "http://127.0.0.1:5000/customers"
transactions_endpoint = "http://127.0.0.1:5000/transactions"
external_data_endpoint = "http://127.0.0.1:5000/external_data"

# Function to fetch data from an endpoint
def fetch_data(endpoint):
    try:
        response = requests.get(endpoint)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {endpoint}: {e}")
        return None

def main():
    # Fetch data from API endpoints
    customers_data = fetch_data(customers_endpoint)
    transactions_data = fetch_data(transactions_endpoint)
    external_data = fetch_data(external_data_endpoint)

    if customers_data and transactions_data and external_data:
        # Set up threading for parallel execution
        threads = []

        # Define your database name and table names
        database_name = "finaldb"
        customers_table_name = "customers"
        transactions_table_name = "transactions"
        cust_external_data_table = "cust_external_data"
        blacklist_table = "blacklist"

        # Create threads for each insertion function
        customers_thread = threading.Thread(target=insert_customers_into_hive, args=(database_name, customers_table_name, customers_data))
        transactions_thread = threading.Thread(target=insert_transactions_into_hive, args=(database_name, transactions_table_name, transactions_data))
        external_data_thread = threading.Thread(target=insert_external_data_into_hive, args=(database_name, cust_external_data_table, blacklist_table, external_data))

        # Start the threads
        customers_thread.start()
        transactions_thread.start()
        external_data_thread.start()

        # Wait for all threads to finish
        customers_thread.join()
        transactions_thread.join()
        external_data_thread.join()

if __name__ == "__main__":
    main()
