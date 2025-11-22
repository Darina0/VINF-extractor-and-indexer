# VINF-extractor-and-indexer
A school project for the subject VINF at FIIT STU. Created an extractor using Apache Spark and an indexer using PyLucene.

The topic of this project is about TV Series, extracting data about them and then indexing it and searching in it via query.

## Project structure
```md
.
├── index_lucene.py      # indexer and search made with PyLucene                 
├── merge.py             # script for joining both datasets into one
├── tmdb_spark.py        # script for extracting data from crawled pages
├── wiki_spark.py        # script for extracting data from wiki dump
```

## How to run the project

I recommend to run one of these scripts first:
- `tmdb_spark.py`
- `wiki_spark.py`

After getting all the wanted data, you can run this script to merge all the data from both sources:
- `merge.py`

After merging the data, you can run this code to index it and then search in it:
- `index_lucene.py`

If you already have the needed data, you can skip scripts that use Spark and only run the last script.

**NOTE**: to be able to run the `index_lucene.py`, you should download and run the Docker image from here https://hub.docker.com/r/coady/pylucene
