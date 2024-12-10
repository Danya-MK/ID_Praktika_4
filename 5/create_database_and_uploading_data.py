import sqlite3
import pandas as pd
import json

customers_file = "shopping_trends.csv"
transactions_file = "retail_sales_dataset.csv"
reviews_file = "modcloth_final_data.json"

conn = sqlite3.connect("ecommerce.db")
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS Customers (
        "Customer ID" INTEGER PRIMARY KEY,
        "Age" INTEGER,
        "Gender" TEXT,
        "Item Purchased" TEXT,
        "Category" TEXT,
        "Purchase Amount (USD)" REAL,
        "Location" TEXT,
        "Size" TEXT,
        "Color" TEXT,
        "Season" TEXT,
        "Review Rating" REAL,
        "Subscription Status" TEXT,
        "Payment Method" TEXT,
        "Shipping Type" TEXT,
        "Discount Applied" TEXT,
        "Promo Code Used" TEXT,
        "Previous Purchases" INTEGER,
        "Preferred Payment Method" TEXT,
        "Frequency of Purchases" TEXT
    )
"""
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS Transactions (
        "Transaction ID" INTEGER PRIMARY KEY,
        "Date" TEXT,
        "Customer ID" INTEGER,
        "Gender" TEXT,
        "Age" INTEGER,
        "Product Category" TEXT,
        "Quantity" INTEGER,
        "Price per Unit" REAL,
        "Total Amount" REAL
    )
"""
)

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS ProductReviews (
        "item_id" TEXT,
        "waist" TEXT,
        "size" INTEGER,
        "quality" INTEGER,
        "cup size" TEXT,
        "hips" TEXT,
        "bra size" TEXT,
        "category" TEXT,
        "bust" TEXT,
        "height" TEXT,
        "user_name" TEXT,
        "length" TEXT,
        "fit" TEXT,
        "user_id" TEXT,
        PRIMARY KEY ("item_id", "user_id")
    )
"""
)

conn.commit()

customers_df = pd.read_csv(customers_file)

for _, row in customers_df.iterrows():
    cursor.execute(
        """
        INSERT OR IGNORE INTO Customers (
            "Customer ID", "Age", "Gender", "Item Purchased", "Category", "Purchase Amount (USD)",
            "Location", "Size", "Color", "Season", "Review Rating", "Subscription Status", 
            "Payment Method", "Shipping Type", "Discount Applied", "Promo Code Used", 
            "Previous Purchases", "Preferred Payment Method", "Frequency of Purchases"
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        tuple(row),
    )

transactions_df = pd.read_csv(transactions_file)

for _, row in transactions_df.iterrows():
    cursor.execute(
        """
        INSERT OR IGNORE INTO Transactions (
            "Transaction ID", "Date", "Customer ID", "Gender", "Age", "Product Category", 
            "Quantity", "Price per Unit", "Total Amount"
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        tuple(row),
    )

with open(reviews_file, "r", encoding="utf-8") as f:
    reviews = [json.loads(line) for line in f]

reviews_df = pd.DataFrame(reviews)

reviews_df = reviews_df[
    [
        "item_id",
        "waist",
        "size",
        "quality",
        "cup size",
        "hips",
        "bra size",
        "category",
        "bust",
        "height",
        "user_name",
        "length",
        "fit",
        "user_id",
    ]
]

for _, row in reviews_df.iterrows():
    cursor.execute(
        """
        INSERT OR IGNORE INTO ProductReviews (
            "item_id", "waist", "size", "quality", "cup size", "hips", "bra size", 
            "category", "bust", "height", "user_name", "length", "fit", "user_id"
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        tuple(row),
    )

conn.commit()

conn.close()

print("База данных успешно создана, таблицы созданы, данные загружены.")
