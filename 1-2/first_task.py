import sqlite3
import json

file_path = "item.json"
with open(file_path, "r", encoding="utf-8") as f:
    data = json.load(f)

conn = sqlite3.connect(":memory:")
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE buildings (
        id INTEGER PRIMARY KEY,
        name TEXT,
        street TEXT,
        city TEXT,
        zipcode INTEGER,
        floors INTEGER,
        year INTEGER,
        parking BOOLEAN,
        prob_price INTEGER,
        views INTEGER
    )
"""
)

insert_query = """
    INSERT INTO buildings (id, name, street, city, zipcode, floors, year, parking, prob_price, views)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
for item in data:
    cursor.execute(
        insert_query,
        (
            item["id"],
            item["name"],
            item["street"],
            item["city"],
            item["zipcode"],
            item["floors"],
            item["year"],
            item["parking"],
            item["prob_price"],
            item["views"],
        ),
    )
conn.commit()

VAR = 14
LIMIT = VAR + 10

sorted_data_query = f"""
    SELECT * FROM buildings
    ORDER BY prob_price ASC
    LIMIT {LIMIT}
"""
cursor.execute(sorted_data_query)
sorted_data = cursor.fetchall()

columns = [column[0] for column in cursor.description]
sorted_data_json = [dict(zip(columns, row)) for row in sorted_data]

stats_query = """
    SELECT SUM(views) AS sum_views, MIN(views) AS min_views, MAX(views) AS max_views, AVG(views) AS avg_views
    FROM buildings
"""
cursor.execute(stats_query)
stats = cursor.fetchone()

frequency_query = """
    SELECT city, COUNT(*) AS count
    FROM buildings
    GROUP BY city
    ORDER BY count DESC
"""
cursor.execute(frequency_query)
frequency_data = cursor.fetchall()

filtered_sorted_query = f"""
    SELECT * FROM buildings
    WHERE floors > 5
    ORDER BY views DESC
    LIMIT {LIMIT}
"""
cursor.execute(filtered_sorted_query)
filtered_sorted_data = cursor.fetchall()

filtered_sorted_data_json = [dict(zip(columns, row)) for row in filtered_sorted_data]


sorted_data_path = "sorted_data.json"
with open(sorted_data_path, "w", encoding="utf-8") as f:
    json.dump(sorted_data_json, f, ensure_ascii=False, indent=4)

stats_path = "stats.json"
stats_data = {
    "sum_views": stats[0],
    "min_views": stats[1],
    "max_views": stats[2],
    "avg_views": stats[3],
}
with open(stats_path, "w", encoding="utf-8") as f:
    json.dump(stats_data, f, ensure_ascii=False, indent=4)

frequency_data_path = "frequency_data.json"
frequency_data_json = [{"city": row[0], "count": row[1]} for row in frequency_data]
with open(frequency_data_path, "w", encoding="utf-8") as f:
    json.dump(frequency_data_json, f, ensure_ascii=False, indent=4)

filtered_sorted_data_path = "filtered_sorted_data.json"
with open(filtered_sorted_data_path, "w", encoding="utf-8") as f:
    json.dump(filtered_sorted_data_json, f, ensure_ascii=False, indent=4)

conn.close()
