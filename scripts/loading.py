from pyhive import hive
from transformations import transaction_transformations, customers_transformation

# Insert data into customer table
def insert_customers_into_hive(database_name, table_name, customers):
    # Connect to Hive
    connection = hive.Connection(host='localhost', port=10000, username='hive')
    print("Connected to Hive.")
    cursor = connection.cursor()

    try:
        # Use the specified database
        cursor.execute(f"USE {database_name}")

        # Transform customers data
        transformed_customers = customers_transformation(customers)

        # Drop all rows in the specified table
        cursor.execute(f"TRUNCATE TABLE {table_name}")

        # Insert transformed customers into the specified table
        for customer in transformed_customers:
            values = (
                customer["customer_id"],
                customer["account_history"],
                customer["age"],
                customer["location"],
                customer["behavioral_pattern_avg"]
            )
            cursor.execute(f"""
                INSERT INTO TABLE {table_name} VALUES
                ('{values[0]}', '{values[1]}', {values[2]}, '{values[3]}', {values[4]})
            """)

        print(f"Inserted {len(transformed_customers)} customers into Hive table '{table_name}'.")

    except Exception as e:
        print(f"Error inserting customers into Hive table: {e}")

    finally:
        cursor.close()
        connection.close()

# Insert data into transaction table
def insert_transactions_into_hive(database_name, table_name, transactions):
    # Connect to Hive
    connection = hive.Connection(host='localhost', port=10000, username='hive')
    print("Connected to Hive.")
    cursor = connection.cursor()

    try:
        # Use the specified database
        cursor.execute(f"USE {database_name}")

        # Insert transactions into the specified table
        for transaction in transactions:
            # Perform transaction transformations
            transformed_transaction = transaction_transformations([transaction])[0]

            values = (
                transformed_transaction["transaction_id"],
                transformed_transaction["date_time"],
                transformed_transaction["amount"],
                transformed_transaction["currency"],
                transformed_transaction["merchant_details"],
                transformed_transaction["customer_id"],
                transformed_transaction["transaction_type"],
                transformed_transaction["location"]
            )

            cursor.execute(f"""
                INSERT INTO TABLE {table_name} VALUES
                ('{values[0]}', '{values[1]}', {values[2]}, '{values[3]}', '{values[4]}', '{values[5]}', '{values[6]}', '{values[7]}')
            """)

        print(f"Inserted {len(transactions)} transactions into Hive table '{table_name}'.")

    except Exception as e:
        print(f"Error inserting transactions into Hive table: {e}")

    finally:
        cursor.close()
        connection.close()

# Insert data into cust_external_data and blacklist tables
def insert_external_data_into_hive(database_name, cust_external_data_table, blacklist_table, external_data):
    # Connect to Hive
    connection = hive.Connection(host='localhost', port=10000, username='hive')
    print("Connected to Hive.")
    cursor = connection.cursor()

    try:
        # Use the specified database
        cursor.execute(f"USE {database_name}")

        # Truncate existing data from cust_external_data table
        cursor.execute(f"TRUNCATE TABLE {cust_external_data_table}")

        # get credit_scores and fraud_reports from external_data
        credit_scores = external_data['credit_scores']
        fraud_reports = external_data['fraud_reports']

        # Combine credit_scores and fraud_reports into one dictionary
        credit_fraud = {k: (credit_scores[k], fraud_reports[k]) for k in credit_scores}

        # Insert new data into cust_external_data table
        for customer_id, (credit_score, fraud_report) in credit_fraud.items():
            cursor.execute(f"""
                INSERT INTO TABLE {cust_external_data_table} VALUES
                ('{customer_id}', {credit_score}, {fraud_report})
            """)

        # Truncate existing data from blacklist table
        cursor.execute(f"TRUNCATE TABLE {blacklist_table}")

        # Insert new data into blacklist table
        for merchant_info in external_data['blacklist_info']:
            cursor.execute(f"""
                INSERT INTO TABLE {blacklist_table} VALUES
                ('{merchant_info}')
            """)

        print(f"Inserted external data into Hive tables '{cust_external_data_table}' and '{blacklist_table}'.")

    except Exception as e:
        print(f"Error inserting external data into Hive tables: {e}")

    finally:
        cursor.close()
        connection.close()