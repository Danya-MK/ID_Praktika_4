import sqlite3
import json

main_file_path = "item.json"
sub_file_path = "subitem.json"

with open(main_file_path, "r", encoding="utf-8") as f:
    main_data = json.load(f)

with open(sub_file_path, "r", encoding="utf-8") as f:
    sub_data = json.load(f)

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

insert_buildings_query = """
    INSERT INTO buildings (id, name, street, city, zipcode, floors, year, parking, prob_price, views)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
for item in main_data:
    cursor.execute(
        insert_buildings_query,
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

cursor.execute(
    """
    CREATE TABLE ratings (
        name TEXT,
        rating REAL,
        convenience INTEGER,
        security INTEGER,
        functionality INTEGER,
        comment TEXT,
        FOREIGN KEY (name) REFERENCES buildings(name)
    )
"""
)

insert_ratings_query = """
    INSERT INTO ratings (name, rating, convenience, security, functionality, comment)
    VALUES (?, ?, ?, ?, ?, ?)
"""
for item in sub_data:
    cursor.execute(
        insert_ratings_query,
        (
            item["name"],
            item["rating"],
            item["convenience"],
            item["security"],
            item["functionality"],
            item["comment"],
        ),
    )

conn.commit()

query1 = """
    SELECT b.city, AVG(r.rating) AS avg_rating
    FROM buildings b
    JOIN ratings r ON b.name = r.name
    GROUP BY b.city
    ORDER BY avg_rating DESC
"""
cursor.execute(query1)
result1 = cursor.fetchall()

query2 = """
    SELECT DISTINCT b.name, b.floors, r.rating
    FROM buildings b
    JOIN ratings r ON b.name = r.name
    WHERE b.floors > 5 AND r.rating > 4.0
    ORDER BY r.rating DESC
"""
cursor.execute(query2)
result2 = cursor.fetchall()

query3 = """
    SELECT b.city, AVG(r.convenience) AS avg_convenience, AVG(r.security) AS avg_security
    FROM buildings b
    JOIN ratings r ON b.name = r.name
    GROUP BY b.city
    ORDER BY avg_convenience DESC, avg_security DESC
"""
cursor.execute(query3)
result3 = cursor.fetchall()

results = {"query1": result1, "query2": result2, "query3": result3}

output_path = "second_task_results.json"
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=4)

conn.close()

print(f"Результаты сохранены в файл: {output_path}")
