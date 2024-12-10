import sqlite3
import pandas as pd
import json

pkl_file_path = "_part_1.pkl"
json_file_path = "_part_2.json"

pkl_data = pd.read_pickle(pkl_file_path)
with open(json_file_path, "r", encoding="utf-8") as f:
    json_data = json.load(f)

pkl_data = pd.DataFrame(pkl_data)

pkl_data = pkl_data.rename(
    columns={
        "song": "song",
        "duration_ms": "duration_ms",
        "year": "year",
        "tempo": "tempo",
        "genre": "genre",
        "popularity": "popularity",
    }
)

json_data = pd.DataFrame(json_data)

combined_data = pd.concat([pkl_data, json_data], ignore_index=True)

combined_data["duration_ms"] = combined_data["duration_ms"].astype(int)
combined_data["year"] = combined_data["year"].astype(int)
combined_data["popularity"] = combined_data["popularity"].astype(int)

conn = sqlite3.connect(":memory:")
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE songs (
        artist TEXT,
        song TEXT,
        duration_ms INTEGER,
        year INTEGER,
        tempo REAL,
        genre TEXT,
        popularity INTEGER
    )
"""
)

insert_query = """
    INSERT INTO songs (artist, song, duration_ms, year, tempo, genre, popularity)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""

for _, row in combined_data.iterrows():
    cursor.execute(
        insert_query,
        (
            row["artist"],
            row["song"],
            row["duration_ms"],
            row["year"],
            row["tempo"],
            row["genre"],
            row["popularity"],
        ),
    )
conn.commit()

query1 = """
    SELECT * FROM songs
    ORDER BY popularity DESC
    LIMIT 24
"""
cursor.execute(query1)
result1 = cursor.fetchall()

query2 = """
    SELECT SUM(duration_ms) AS sum_duration, 
           MIN(duration_ms) AS min_duration, 
           MAX(duration_ms) AS max_duration, 
           AVG(duration_ms) AS avg_duration
    FROM songs
"""
cursor.execute(query2)
result2 = cursor.fetchone()

query3 = """
    SELECT genre, COUNT(*) AS count
    FROM songs
    GROUP BY genre
    ORDER BY count DESC
"""
cursor.execute(query3)
result3 = cursor.fetchall()

query4 = """
    SELECT * FROM songs
    WHERE popularity > 50
    ORDER BY duration_ms DESC
    LIMIT 29
"""
cursor.execute(query4)
result4 = cursor.fetchall()

output_files = {
    "sorted_songs.json": result1,
    "duration_stats.json": {
        "sum_duration": result2[0],
        "min_duration": result2[1],
        "max_duration": result2[2],
        "avg_duration": result2[3],
    },
    "genre_frequency.json": [{"genre": row[0], "count": row[1]} for row in result3],
    "filtered_sorted_songs.json": result4,
}

for filename, data in output_files.items():
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

conn.close()

print("Результаты сохранены в файлы:")
for filename in output_files:
    print(filename)
