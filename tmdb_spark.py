import os
import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import glob
import json

#env fix for Windows
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#spark session initialization
spark = SparkSession.builder \
    .appName("tmdb_extractor") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#regex patterns - from previous extractor
TITLE = re.compile(r'<h2[^>]*>\s*<a[^>]*href="/tv/[^"]+"[^>]*>([^<]+)</a>', re.DOTALL | re.IGNORECASE)
YEAR = re.compile(r'<span class="tag release_date">\((\d{4})\)</span>')
CERTIFICATION = re.compile(r'<span class="certification">\s*([^<\n]+)\s*</span>')
GENRES = re.compile(r'<span class="genres">(.+?)</span>', re.DOTALL | re.IGNORECASE)
LINK_GENRE = re.compile(r'>([^<]+)</a>')
CREATORS = re.compile(r'<li[^>]*class="profile"[^>]*>\s*<p><a[^>]*>([^<]+)</a></p>\s*<p class="character">\s*Creator\s*</p>', re.IGNORECASE | re.DOTALL)
USER_SCORE = re.compile(r'<div[^>]*class="user_score_chart"[^>]*data-percent="(\d+)"', re.IGNORECASE)
STATUS = re.compile(r'<strong><bdi>Status</bdi></strong>\s*([^<]+)')
TYPE = re.compile(r'<strong><bdi>Type</bdi></strong>\s*([^<]+)')
LANG = re.compile(r'<strong><bdi>Original Language</bdi></strong>\s*([^<]+)')
KEYWORDS = re.compile(r'<section[^>]*class="keywords[^>]*>.*?<ul>(.*?)</ul>', re.DOTALL)
LINK_KEYWORD = re.compile(r'<a[^>]*>([^<]+)</a>')
CAST = re.compile(r'<div id="cast_scroller"[^>]*>.*?<ol[^>]*>(.*?)</ol>', re.DOTALL)
ACTOR = re.compile(r'<p><a href="[^"]+">([^<]+)</a></p>')

#process html file 
def extract_data(file_content):
    html = file_content
    try:
        title = TITLE.search(html)
        year = YEAR.search(html)
        certification = CERTIFICATION.search(html)
        genres = GENRES.search(html)
        score = USER_SCORE.search(html)
        creators = CREATORS.findall(html)
        status = STATUS.search(html)
        tv_type = TYPE.search(html)
        language = LANG.search(html)
        keywords = KEYWORDS.search(html)
        cast = CAST.search(html)

        genres_list = LINK_GENRE.findall(genres.group(1)) if genres else []
        keywords_list = []
        if keywords:
            if "No keywords have been added." not in keywords.group(0):
                keywords_list = LINK_KEYWORD.findall(keywords.group(1))

        cast_list = ACTOR.findall(cast.group(1)) if cast else []

        return {
            "title": title.group(1).strip() if title else None,
            "year": int(year.group(1)) if year else None,
            "certification": certification.group(1).strip() if certification else None,
            "genres": genres_list,
            "score": int(score.group(1)) if score else None,
            "creator": creators if creators else [],
            "status": status.group(1).strip() if status else None,
            "type": tv_type.group(1).strip() if tv_type else None,
            "language": language.group(1).strip() if language else None,
            "keywords": keywords_list,
            "cast": cast_list
        }
    except Exception as e:
        return {
            "title": None,
            "year": None,
            "certification": None,
            "genres": [],
            "score": None,
            "creator": [],
            "status": None,
            "type": None,
            "language": None,
            "keywords": [],
            "cast": [],
            "error": str(e)
        }

#get html files
html_files = glob.glob("pages/*.html")
total_files = len(html_files)

if total_files == 0:
    spark.stop()
    sys.exit(1)

#config for batch size
BATCH_SIZE = 500
batches = [html_files[i:i + BATCH_SIZE] for i in range(0, total_files, BATCH_SIZE)]
num_batches = len(batches)

print(f"Processing in {num_batches} batches of {BATCH_SIZE} files each\n")

#define schema
schema = StructType([
    StructField("title", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("certification", StringType(), True),
    StructField("genres", ArrayType(StringType()), True),
    StructField("score", IntegerType(), True),
    StructField("creator", ArrayType(StringType()), True),
    StructField("status", StringType(), True),
    StructField("type", StringType(), True),
    StructField("language", StringType(), True),
    StructField("keywords", ArrayType(StringType()), True),
    StructField("cast", ArrayType(StringType()), True),
])

#output file
output_file = "tmdb_data.jsonl"
total_processed = 0
total_success = 0

#delete existing output file
if os.path.exists(output_file):
    os.remove(output_file)

#process batches
for batch_i, batch_files in enumerate(batches, 1):
    print(f"Processing batch {batch_i}/{num_batches}")
    
    #load files as batches
    file_contents = []
    for filepath in batch_files:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                file_contents.append((filepath, f.read()))
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
            continue
    
    if not file_contents:
        continue
    
    #create rdd from batch and parse
    rdd = spark.sparkContext.parallelize(file_contents)
    parsed_rdd = rdd.map(lambda x: extract_data(x[1]))
    
    #create dataframe from parsed rdd
    df = spark.createDataFrame(parsed_rdd, schema=schema)
    batch_data = df.collect()

    #append data of batch to output file    
    with open(output_file, 'a', encoding='utf-8') as f:
        for row in batch_data:
            data = row.asDict()
            f.write(json.dumps(data, ensure_ascii=False) + '\n')
    
    total_processed += len(batch_data)
    
    print(f"Batch {batch_i} done")
    print(f"Progress: {total_processed}/{total_files} files processed\n")


print(f"Data saved as: {output_file}")
spark.stop()