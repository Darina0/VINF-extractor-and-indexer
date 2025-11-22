import os
import sys
import json
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#spark inicialization
spark = SparkSession.builder \
    .appName("merge") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .config("spark.sql.autoBroadcastJoinThreshold", "104857600") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#normalize titles
def normalize_title(title):
    if not title:
        return ""
    
    #remove noise patterns
    title = str(title).lower().strip()
    title = re.sub(r'\([^)]*\)', '', title)
    title = re.sub(r'\b(the|a|an)\b', '', title)
    title = re.sub(r'[^a-z0-9\s]', '', title)
    title = re.sub(r'\s+', ' ', title).strip()
    
    return title

#normalize year
def normalize_year(year):
    if not year:
        return None
    year_str = str(year).strip()
    match = re.search(r'\b(19\d{2}|20\d{2})\b', year_str)
    if match:
        return match.group(1)
    
    return None

#load both datasets
tmdb_data = []
if os.path.exists("tmdb_left_join.jsonl"):
    with open("tmdb_left_join.jsonl", "r", encoding="utf-8") as f:
        for line in f:
            item = json.loads(line)
            item["normalized"] = normalize_title(item["title"])
            item["normalized_year"] = normalize_year(item.get("year"))
            if item["normalized"]:
                tmdb_data.append(item)
    print(f"{len(tmdb_data)} TMDB entries")
else:
    print("tmdb_data.jsonl not found")
    spark.stop()
    sys.exit(1)

wiki_data = []
if os.path.exists("merged_data.jsonl"):
    with open("merged_data.jsonl", "r", encoding="utf-8") as f:
        for line in f:
            item = json.loads(line)
            item["normalized"] = normalize_title(item["title"])
            item["normalized_year"] = normalize_year(item.get("year"))
            if item["normalized"]:
                wiki_data.append(item)
    print(f"{len(wiki_data)} Wiki entries")
else:
    print("wiki_data.jsonl not found")
    spark.stop()
    sys.exit(1)


print("\nCreating dataframes")

#tmdb df
tmdb_df = spark.createDataFrame(tmdb_data)
wiki_df = spark.createDataFrame(wiki_data)
print(f"TMDB DF: {tmdb_df.count()} rows \nWiki DF: {wiki_df.count()} rows")

print("\nJoining")

#join datasets - left
joined_df = tmdb_df.alias("t").join(
    broadcast(wiki_df.alias("w")),           
    (col("t.normalized_title") == col("w.normalized_title")) & 
    (col("t.normalized_year") == col("w.normalized_year")),
    "left"                                    
)
print(f"Completed \nSelecting matched records")

#select only records with added wiki data
matched_df = joined_df.filter(col("w.title").isNotNull())

#final columns
final_df = matched_df.select(
    col("t.title").alias("title"),
    col("t.year").alias("year"),
    col("t.certification").alias("certification"),
    col("t.genres").alias("genres"),
    col("t.score").alias("score"),
    col("t.creator").alias("creator"),
    col("t.status").alias("status"),
    col("t.type").alias("type"),
    col("t.language").alias("language"),
    col("t.keywords").alias("keywords"),
    col("t.cast").alias("cast"),
    
    col("w.country").alias("country"),
    col("w.num_seasons").alias("num_seasons"),
    col("w.num_episodes").alias("num_episodes"),
    col("w.runtime").alias("runtime"),
    col("w.network").alias("network"),
    col("w.plot").alias("plot")
)

matched_count = final_df.count()
print(f"{matched_count} exact matches")

results = final_df.collect()

#save as jsonl
print("\nSaving data_all.jsonl")
with open("data_all.jsonl", "w", encoding="utf-8") as f:
    for row in results:
        record = {
            "title": row.title,
            "year": row.year,
            "certification": row.certification,
            "genres": row.genres if row.genres else [],
            "score": row.score,
            "creator": row.creator if row.creator else [],
            "status": row.status,
            "type": row.type,
            "language": row.language,
            "keywords": row.keywords if row.keywords else [],
            "cast": row.cast if row.cast else [],
            
            "country": row.country,
            "num_seasons": row.num_seasons,
            "num_episodes": row.num_episodes,
            "runtime": row.runtime,
            "network": row.network,
            "plot": row.plot,
        }
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

#stats
print("\nMerge Statistics:")
print(f"Total Wiki entries: {len(wiki_data)}")
print(f"Total TMDB entries: {len(tmdb_data)}")
print(f"Successfully matched: {len(matched_count)}")
print(f"Match rate: {len(matched_count)/len(tmdb_data)*100:.1f}% of TMDB entries")

spark.stop()