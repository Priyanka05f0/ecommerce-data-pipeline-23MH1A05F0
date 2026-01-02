import os
import json
import random
from datetime import datetime
import pandas as pd
from faker import Faker
import yaml

fake = Faker()

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.yaml")

os.makedirs(RAW_DATA_DIR, exist_ok=True)

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def generate_customers(n):
    data = []
    for i in range(1, n + 1):
        data.append({
            "customer_id": f"CUST{i:04d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "city": fake.city()
        })
    return pd.DataFrame(data)

def generate_products(n):
    data = []
    for i in range(1, n + 1):
        price = round(random.uniform(10, 500), 2)
        data.append({
            "product_id": f"PROD{i:04d}",
            "product_name": fake.word(),
            "category": "General",
            "price": price
        })
    return pd.DataFrame(data)

def generate_transactions(customers, n):
    data = []
    for i in range(1, n + 1):
        data.append({
            "transaction_id": f"TXN{i:05d}",
            "customer_id": random.choice(customers["customer_id"]),
            "transaction_date": fake.date_this_year(),
            "total_amount": 0.0
        })
    return pd.DataFrame(data)

def generate_transaction_items(transactions, products):
    items = []
    counter = 1

    for idx, txn in transactions.iterrows():
        chosen = products.sample(random.randint(1, 3))
        total = 0.0

        for _, prod in chosen.iterrows():
            qty = random.randint(1, 3)

            # ✅ EXACT TEST EXPECTATION
            line_total = round(qty * prod["price"], 2)
            total += line_total

            items.append({
                "item_id": f"ITEM{counter:05d}",
                "transaction_id": txn["transaction_id"],
                "product_id": prod["product_id"],
                "quantity": qty,
                "unit_price": prod["price"],
                "discount_percentage": 0,
                "line_total": line_total
            })
            counter += 1

        transactions.at[idx, "total_amount"] = round(total, 2)

    return pd.DataFrame(items), transactions

def main():
    # 🔥 REMOVE OLD FILES (CRITICAL)
    for f in os.listdir(RAW_DATA_DIR):
        os.remove(os.path.join(RAW_DATA_DIR, f))

    config = load_config()

    customers = generate_customers(config["data_generation"]["customers"])
    products = generate_products(config["data_generation"]["products"])
    transactions = generate_transactions(customers, config["data_generation"]["transactions"])
    items, transactions = generate_transaction_items(transactions, products)

    customers.to_csv(f"{RAW_DATA_DIR}/customers.csv", index=False)
    products.to_csv(f"{RAW_DATA_DIR}/products.csv", index=False)
    transactions.to_csv(f"{RAW_DATA_DIR}/transactions.csv", index=False)
    items.to_csv(f"{RAW_DATA_DIR}/transaction_items.csv", index=False)

    with open(f"{RAW_DATA_DIR}/generation_metadata.json", "w") as f:
        json.dump({"status": "ok"}, f)

    print("✅ Data generation completed successfully")

if __name__ == "__main__":
    main()
