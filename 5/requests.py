import sqlite3
import json

conn = sqlite3.connect("ecommerce.db")
cursor = conn.cursor()

# Вывод первых 10 отсортированных по "Purchase Amount" строк в формате JSON
cursor.execute(
    """
    SELECT "Customer ID", "Age", "Gender", "Item Purchased", "Category", "Purchase Amount (USD)",
           "Location", "Size", "Color", "Season", "Review Rating", "Subscription Status", 
           "Payment Method", "Shipping Type", "Discount Applied", "Promo Code Used", 
           "Previous Purchases", "Preferred Payment Method", "Frequency of Purchases"
    FROM Customers
    ORDER BY "Purchase Amount (USD)" DESC
    LIMIT 10
"""
)

customers_data = cursor.fetchall()

customers_columns = [
    "Customer ID",
    "Age",
    "Gender",
    "Item Purchased",
    "Category",
    "Purchase Amount (USD)",
    "Location",
    "Size",
    "Color",
    "Season",
    "Review Rating",
    "Subscription Status",
    "Payment Method",
    "Shipping Type",
    "Discount Applied",
    "Promo Code Used",
    "Previous Purchases",
    "Preferred Payment Method",
    "Frequency of Purchases",
]

customers_json = [dict(zip(customers_columns, row)) for row in customers_data]

with open("customers_top_10.json", "w", encoding="utf-8") as f:
    json.dump(customers_json, f, ensure_ascii=False, indent=4)

# Подсчёт объектов по условию для таблицы
cursor.execute(
    """
    SELECT "Category", COUNT(*) 
    FROM Customers 
    GROUP BY "Category"
"""
)

category_counts = cursor.fetchall()

category_counts_json = [
    {"Category": row[0], "Count": row[1]} for row in category_counts
]

with open("category_counts.json", "w", encoding="utf-8") as f:
    json.dump(category_counts_json, f, ensure_ascii=False, indent=4)

# Анализ данных (сумма, мин, макс, среднее по Total Amount)
cursor.execute(
    """
    SELECT 
        SUM("Total Amount") AS total_sum,
        MIN("Total Amount") AS min_amount,
        MAX("Total Amount") AS max_amount,
        AVG("Total Amount") AS avg_amount
    FROM Transactions
"""
)

transaction_analysis = cursor.fetchone()

transaction_analysis_json = {
    "Total Sum": transaction_analysis[0],
    "Min Amount": transaction_analysis[1],
    "Max Amount": transaction_analysis[2],
    "Avg Amount": transaction_analysis[3],
}

with open("transaction_analysis.json", "w", encoding="utf-8") as f:
    json.dump(transaction_analysis_json, f, ensure_ascii=False, indent=4)

# Вывод всех товаров с количеством, количеством покупок и средней ценой
cursor.execute(
    """
    SELECT 
        "Item Purchased", COUNT(*) AS purchase_count, AVG("Purchase Amount (USD)") AS avg_purchase_amount
    FROM Customers
    GROUP BY "Item Purchased"
    ORDER BY purchase_count DESC
    LIMIT 10
"""
)

item_stats = cursor.fetchall()

item_stats_json = [
    {"Item Purchased": row[0], "Purchase Count": row[1], "Avg Purchase Amount": row[2]}
    for row in item_stats
]

with open("item_stats.json", "w", encoding="utf-8") as f:
    json.dump(item_stats_json, f, ensure_ascii=False, indent=4)

# Количество заказов по каждому методу оплаты
cursor.execute(
    """
    SELECT "Payment Method", COUNT(*) AS order_count
    FROM Transactions
    GROUP BY "Payment Method"
"""
)

payment_method_counts = cursor.fetchall()

payment_method_counts_json = [
    {"Payment Method": row[0], "Order Count": row[1]} for row in payment_method_counts
]

with open("payment_method_counts.json", "w", encoding="utf-8") as f:
    json.dump(payment_method_counts_json, f, ensure_ascii=False, indent=4)

# Средний рейтинг товаров по категориям
cursor.execute(
    """
    SELECT "Category", AVG("Review Rating") AS avg_rating
    FROM Customers
    GROUP BY "Category"
"""
)

category_ratings = cursor.fetchall()

category_ratings_json = [
    {"Category": row[0], "Avg Rating": row[1]} for row in category_ratings
]

with open("category_ratings.json", "w", encoding="utf-8") as f:
    json.dump(category_ratings_json, f, ensure_ascii=False, indent=4)

conn.close()

print("Запросы выполнены и результаты сохранены в JSON файлы.")
