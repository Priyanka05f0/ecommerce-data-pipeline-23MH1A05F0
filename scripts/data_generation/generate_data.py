import os
import json
import random
from datetime import datetime
import pandas as pd
from faker import Faker
import yaml

fake = Faker()

# Paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.yaml")

os.makedirs(RAW_DATA_DIR, exist_ok=True)


def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def generate_customers(n):
    customers = []
    emails = set()

    for i in range(1, n + 1):
        email = fake.email()
        while email in emails:
            email = fake.email()
        emails.add(email)

        customers.append({
            "customer_id": f"CUST{i:04d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": email,
            "phone": fake.phone_number(),
            "registration_date": fake.date_between(start_date="-2y", end_date="today"),
            "city": fake.city(),
            "state": fake.state(),
            "country": fake.country(),
            "age_group": random.choice(
                ["18-25", "26-35", "36-45", "46-60", "60+"]
            )
        })

    return pd.DataFrame(customers)


def generate_products(n):
    categories = {
        "Electronics": ["Mobile", "Laptop", "Accessories"],
        "Clothing": ["Men", "Women", "Kids"],
        "Home & Kitchen": ["Furniture", "Decor", "Appliances"],
        "Books": ["Fiction", "Education", "Comics"],
        "Sports": ["Indoor", "Outdoor", "Fitness"],
        "Beauty": ["Skincare", "Makeup", "Haircare"]
    }

    products = []

    for i in range(1, n + 1):
        category = random.choice(list(categories.keys()))
        sub_category = random.choice(categories[category])
        price = round(random.uniform(10, 500), 2)
        cost = round(price * random.uniform(0.6, 0.9), 2)

        products.append({
            "product_id": f"PROD{i:04d}",
            "product_name": fake.word().capitalize(),
            "category": category,
            "sub_category": sub_category,
            "price": price,
            "cost": cost,
            "brand": fake.company(),
            "stock_quantity": random.randint(10, 500),
            "supplier_id": f"SUP{random.randint(1, 50):03d}"
        })

    return pd.DataFrame(products)


def generate_transactions(customers_df, n):
    transactions = []
    payment_methods = [
        "Credit Card",
        "Debit Card",
        "UPI",
        "Cash on Delivery",
        "Net Banking"
    ]

    customer_ids = customers_df["customer_id"].tolist()

    for i in range(1, n + 1):
        transactions.append({
            "transaction_id": f"TXN{i:05d}",
            "customer_id": random.choice(customer_ids),
            "transaction_date": fake.date_between(start_date="-1y", end_date="today"),
            "transaction_time": fake.time(),
            "payment_method": random.choice(payment_methods),
            "shipping_address": fake.address().replace("\n", ", "),
            "total_amount": 0.0
        })

    return pd.DataFrame(transactions)


def generate_transaction_items(transactions_df, products_df):
    items = []
    counter = 1

    for idx, txn in transactions_df.iterrows():
        num_items = random.randint(1, 5)
        selected_products = products_df.sample(num_items)

        total = 0.0

        for _, prod in selected_products.iterrows():
            quantity = random.randint(1, 3)
            discount = random.choice([0, 5, 10, 15])

            line_total = round(
                quantity * prod["price"] * (1 - discount / 100), 2
            )
            total += line_total

            items.append({
                "item_id": f"ITEM{counter:05d}",
                "transaction_id": txn["transaction_id"],
                "product_id": prod["product_id"],
                "quantity": quantity,
                "unit_price": prod["price"],
                "discount_percentage": discount,
                "line_total": line_total
            })

            counter += 1

        transactions_df.at[idx, "total_amount"] = round(total, 2)

    return pd.DataFrame(items), transactions_df


def validate_referential_integrity(customers, products, transactions, items):
    orphan_customers = transactions[
        ~transactions["customer_id"].isin(customers["customer_id"])
    ]
    orphan_products = items[
        ~items["product_id"].isin(products["product_id"])
    ]
    orphan_transactions = items[
        ~items["transaction_id"].isin(transactions["transaction_id"])
    ]

    violations = (
        len(orphan_customers)
        + len(orphan_products)
        + len(orphan_transactions)
    )

    return {
        "orphan_records": violations,
        "constraint_violations": violations,
        "data_quality_score": 100 if violations == 0 else 90
    }


def main():
    config = load_config()

    customers = generate_customers(config["data_generation"]["customers"])
    products = generate_products(config["data_generation"]["products"])
    transactions = generate_transactions(
        customers, config["data_generation"]["transactions"]
    )
    items, transactions = generate_transaction_items(transactions, products)

    customers.to_csv(os.path.join(RAW_DATA_DIR, "customers.csv"), index=False)
    products.to_csv(os.path.join(RAW_DATA_DIR, "products.csv"), index=False)
    transactions.to_csv(os.path.join(RAW_DATA_DIR, "transactions.csv"), index=False)
    items.to_csv(os.path.join(RAW_DATA_DIR, "transaction_items.csv"), index=False)

    metadata = {
        "generated_at": datetime.now().isoformat(),
        "record_counts": {
            "customers": len(customers),
            "products": len(products),
            "transactions": len(transactions),
            "transaction_items": len(items)
        },
        "validation": validate_referential_integrity(
            customers, products, transactions, items
        )
    }

    with open(os.path.join(RAW_DATA_DIR, "generation_metadata.json"), "w") as f:
        json.dump(metadata, f, indent=4)

    print("✅ Data generation completed successfully")


if __name__ == "__main__":
    main()
