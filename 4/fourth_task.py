import sqlite3
import pandas as pd
import msgpack
import json

csv_file_path = "_product_data.csv"
msgpack_file_path = "_update_data.msgpack"
output_csv_path = "updated_products.csv"

products_df = pd.read_csv(csv_file_path, delimiter=";")

products_df["views"] = products_df["views"].fillna(0).astype(int)

conn = sqlite3.connect(":memory:")
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        price REAL,
        quantity INTEGER,
        category TEXT,
        fromCity TEXT,
        isAvailable BOOLEAN,
        views INTEGER,
        update_counter INTEGER DEFAULT 0
    )
"""
)

insert_product_query = """
    INSERT INTO products (name, price, quantity, category, fromCity, isAvailable, views)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""
for _, row in products_df.iterrows():
    cursor.execute(
        insert_product_query,
        (
            row["name"],
            float(row["price"]),
            int(row["quantity"]),
            row["category"],
            row["fromCity"],
            row["isAvailable"] == "True",
            int(row["views"]),
        ),
    )
conn.commit()

with open(msgpack_file_path, "rb") as f:
    updates = msgpack.unpack(f)

for update in updates:
    name = update.get("name")
    method = update.get("method")
    param = update.get("param")

    if method == "price_abs":
        new_price = float(param)
        cursor.execute(
            """
            UPDATE products
            SET price = ?, update_counter = update_counter + 1
            WHERE name = ?
        """,
            (new_price, name),
        )

    elif method == "price_percent":
        percent = float(param)
        cursor.execute(
            """
            UPDATE products
            SET price = price * (1 + ? / 100), update_counter = update_counter + 1
            WHERE name = ?
        """,
            (percent, name),
        )

    elif method == "quantity_add":
        quantity_add = int(param)
        cursor.execute(
            """
            UPDATE products
            SET quantity = quantity + ?, update_counter = update_counter + 1
            WHERE name = ?
        """,
            (quantity_add, name),
        )

    elif method == "quantity_sub":
        quantity_sub = int(param)
        cursor.execute(
            """
            UPDATE products
            SET quantity = quantity - ?, update_counter = update_counter + 1
            WHERE name = ?
        """,
            (quantity_sub, name),
        )

    elif method == "available":
        available = param == "True"
        cursor.execute(
            """
            UPDATE products
            SET isAvailable = ?, update_counter = update_counter + 1
            WHERE name = ?
        """,
            (available, name),
        )

    elif method == "remove":
        cursor.execute(
            """
            DELETE FROM products
            WHERE name = ?
        """,
            (name,),
        )
conn.commit()

query = (
    "SELECT name, price, quantity, category, fromCity, isAvailable, views FROM products"
)
cursor.execute(query)
updated_data = cursor.fetchall()

columns = [desc[0] for desc in cursor.description]

updated_df = pd.DataFrame(updated_data, columns=columns)

updated_df.to_csv(output_csv_path, index=False, sep=";")

conn.close()

print(f"Изменённый CSV файл сохранён в {output_csv_path}")
