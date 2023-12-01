import json
import os
from datetime import datetime, timedelta
import random
from dotenv import load_dotenv
import hashlib
import time

load_dotenv()

base_project_location = os.getenv('BASE_PROJECT_LOCATION')
data_folder = base_project_location + "api/data/raw/"
last_treated_file_path = data_folder + "last_treated_file.txt"

def save_data(data, file_path):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=2)

def load_data(file_path):
    if os.path.exists(file_path):
        return json.load(open(file_path, 'r'))
    return None

def merge_external_data(old_external_data, new_external_data):
    if not old_external_data:
        return new_external_data

    merged_external_data = {
        "blacklist_info": list(set(old_external_data.get("blacklist_info", []) + new_external_data.get("blacklist_info", []))),
        "credit_scores": old_external_data.get("credit_scores", {}),
        "fraud_reports": old_external_data.get("fraud_reports", {})
    }

    # Merge credit_scores
    for customer_id, credit_score in new_external_data.get("credit_scores", {}).items():
        merged_external_data["credit_scores"][customer_id] = credit_score

    # Merge fraud_reports
    for customer_id, fraud_report in new_external_data.get("fraud_reports", {}).items():
        if customer_id in merged_external_data["fraud_reports"]:
            merged_external_data["fraud_reports"][customer_id] += fraud_report
        else:
            merged_external_data["fraud_reports"][customer_id] = fraud_report

    return merged_external_data

def random_date(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())))

def generate_high_frequency_transactions(customer, current_time, num_transactions):
    transactions = []
    for _ in range(num_transactions):
        transaction_time = random_date(current_time - timedelta(minutes=5), current_time)
        transaction_id = hashlib.md5((transaction_time.isoformat() + customer['customer_id']).encode()).hexdigest()
        transactions.append({
            "transaction_id": f"T{transaction_id}",
            "date_time": transaction_time.isoformat(),
            "amount": random.uniform(10, 1000),
            "currency": random.choice(["USD", "EUR", "GBP"]),
            "merchant_details": f"Merchant{random.randint(1, 20)}",
            "customer_id": customer['customer_id'],
            "transaction_type": random.choice(["purchase", "withdrawal"]),
            "location": f"City{random.randint(11, 20)}"  # Different from customer's city
        })
    return transactions

def generate_data(num_transactions, num_customers, old_customers=[], old_external_data=None):
    current_time = datetime.now()
    customers = old_customers.copy()  # Include old customers in the starting list

    for i in range(len(customers), num_customers + len(old_customers)):
        customer_id = None
        while customer_id is None or any(c['customer_id'] == customer_id for c in customers):
            customer_id = f"C{i:03}"

        customer_city = f"City{random.randint(1, 10)}"
        customers.append({
            "customer_id": customer_id,
            "account_history": [],
            "demographics": {"age": random.randint(18, 70), "location": customer_city},
            "behavioral_patterns": {"avg_transaction_value": random.uniform(50, 500)}
        })

    for customer in random.sample(customers, k=int(0.1 * len(customers))):  # 10% of users
        transactions = generate_high_frequency_transactions(customer, current_time, 10)
        customer['account_history'].extend(t['transaction_id'] for t in transactions)

    all_transactions = []
    for i in range(num_transactions):
        customer = random.choice(customers)
        transaction_time = random_date(current_time - timedelta(minutes=5), current_time)
        transaction_id = hashlib.md5((transaction_time.isoformat() + customer['customer_id']).encode()).hexdigest()
        transaction = {
            "transaction_id": f"T{transaction_id}",
            "date_time": transaction_time.isoformat(),
            "amount": random.uniform(10, 1000) * (10 if random.random() < 0.4 else 1),  # 5% chance of high amount
            "currency": random.choice(["USD", "EUR", "GBP"]),
            "merchant_details": f"Merchant{random.randint(1, 20)}",
            "customer_id": customer['customer_id'],
            "transaction_type": random.choice(["purchase", "withdrawal"]),
            "location": f"City{random.randint(1, 10)}"
        }
        customer['account_history'].append(transaction['transaction_id'])
        all_transactions.append(transaction)

    external_data = {
        "blacklist_info": [f"Merchant{random.randint(21, 30)}" for _ in range(10)],
        "credit_scores": {},
        "fraud_reports": {}
    }

    for customer in customers:
        external_data["credit_scores"][customer['customer_id']] = random.randint(300, 850)
        external_data["fraud_reports"][customer['customer_id']] = random.randint(0, 5)

    # Merge new external data with old external data
    merged_external_data = merge_external_data(old_external_data, external_data)

    return all_transactions, customers, merged_external_data

if __name__ == '__main__':
    while True:
        # Load the old customers data if it exists
        old_customers = []
        if os.path.exists(data_folder + "customers/customers.json"):
            old_customers = json.load(open(data_folder + "customers/customers.json", 'r'))

        # Load the old external data if it exists
        old_external_data = load_data(data_folder + "external_data/external_data.json")

        # Generate 10 new transactions and 5 new users every 10 minutes
        new_transactions, new_customers, new_external_data = generate_data(10, 5, old_customers=old_customers, old_external_data=old_external_data)

        # Save new customers data
        customers_file_name = f"customers.json"
        save_data(new_customers, data_folder + "customers/" + customers_file_name)

        # Save new transactions data
        transactions_file_name = f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        save_data(new_transactions, data_folder + "transactions/" + transactions_file_name)

        # Save new external data
        external_data_file_name = f"external_data.json"
        save_data(new_external_data, data_folder + "external_data/" + external_data_file_name)

        print(f"Generated {len(new_transactions)} transactions and {len(new_customers)} customers at {datetime.now()}")

        # Sleep for 10 minutes before generating new data
        time.sleep(600)
