from flask import Flask, jsonify
import os
import json
from dotenv import load_dotenv

load_dotenv()

base_project_location = os.getenv('BASE_PROJECT_LOCATION')
data_folder = base_project_location + "api/data/raw/"
last_treated_file_path = data_folder + "last_treated_files.txt"

app = Flask(__name__)

@app.route('/customers', methods=['GET'])
def get_customers():
    customers_file = get_latest_file(data_folder + "customers/")
    if customers_file:
        customers_data = json.load(open(customers_file, 'r'))
        return jsonify(customers_data)
    else:
        return jsonify({"error": "No customers data available."}), 404

@app.route('/transactions', methods=['GET'])
def get_transactions():
    last_treated_file = load_last_treated_file()
    transactions_file = get_next_file(data_folder + "transactions/", last_treated_file['last_transactions'].split('/')[-1])
    
    if transactions_file:
        transactions_data = json.load(open(transactions_file, 'r'))
        update_last_treated_file(transactions_file, last_treated_file['last_external_data'])
        return jsonify(transactions_data)
    else:
        return jsonify({"error": "No transactions data available."}), 404

@app.route('/external_data', methods=['GET'])
def get_external_data():
    last_treated_file = load_last_treated_file()
    external_data_file = get_next_file(data_folder + "external_data/", last_treated_file['last_external_data'].split('/')[-1])
    
    if external_data_file:
        external_data = json.load(open(external_data_file, 'r'))
        update_last_treated_file(last_treated_file['last_transactions'], external_data_file)
        return jsonify(external_data)
    else:
        return jsonify({"error": "No external data available."}), 404

def get_latest_file(directory):
    files = [f for f in os.listdir(directory) if f.endswith(".json")]
    if files:
        return os.path.join(directory, sorted(files)[-1])
    return None

def get_next_file(directory, last_file):
    files = [f for f in os.listdir(directory) if f.endswith(".json")]
    if last_file:
        idx = files.index(last_file) + 1
        if idx < len(files):
            return os.path.join(directory, files[idx])
    elif files:
        return os.path.join(directory, files[0])
    return None

def load_last_treated_file():
    last_treated_data = {"last_transactions": "", "last_external_data": ""}
    if os.path.exists(last_treated_file_path):
        with open(last_treated_file_path, 'r') as file:
            last_treated_data = json.load(file)
    return last_treated_data

def update_last_treated_file(last_transactions, last_external_data):
    last_treated_data = {"last_transactions": last_transactions, "last_external_data": last_external_data}
    with open(last_treated_file_path, 'w') as file:
        json.dump(last_treated_data, file, indent=2)

if __name__ == '__main__':
    app.run(debug=True)
