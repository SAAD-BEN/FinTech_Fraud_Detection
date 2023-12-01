from datetime import datetime

def transaction_transformations(transactions):
    transformed_data = []

    for transaction in transactions:
        # Round the amount
        amount = round(transaction["amount"], 2)

        # Format date_time to be compatible with Hive timestamp or datetime format
        date_time_str = transaction["date_time"]
        formatted_date_time = datetime.strptime(date_time_str, "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")

        # unify currency"USD", "EUR", "GBP" to "USD"
        if transaction["currency"] in ["EUR", "GBP"]:
            if transaction["currency"] == "EUR":
                amount = round(amount * 1.2, 2)
            elif transaction["currency"] == "GBP":
                amount = round(amount * 1.4, 2)
            transaction["currency"] = "USD"
        # Create a dictionary with the transformed data
        transformed_transaction = {
            "transaction_id": transaction["transaction_id"],
            "date_time": formatted_date_time,
            "amount": amount,
            "currency": transaction["currency"],
            "merchant_details": transaction["merchant_details"],
            "customer_id": transaction["customer_id"],
            "transaction_type": transaction["transaction_type"],
            "location": transaction["location"]
        }

        transformed_data.append(transformed_transaction)

    return transformed_data

def customers_transformation(customers):
    transformed_data = []

    for customer in customers:
        # Flatten the account history list into one string
        account_history_str = ",".join(customer["account_history"])

        # Split demographics into two variables (age and location)
        age = customer["demographics"]["age"]
        location = customer["demographics"]["location"]

        # Merge behavioral_patterns and avg_transaction_value into one variable
        behavioral_pattern_avg = customer["behavioral_patterns"]["avg_transaction_value"]

        # Create a dictionary with the transformed data
        transformed_customer = {
            "customer_id": customer["customer_id"],
            "account_history": account_history_str,
            "age": age,
            "location": location,
            "behavioral_pattern_avg": behavioral_pattern_avg
        }

        transformed_data.append(transformed_customer)

    return transformed_data
# export functions
__all__ = ["transaction_transformations", "customers_transformation"]