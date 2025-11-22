import os
import sys
from pyspark.sql import SparkSession
import re
import json
import html

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#spark initialization
spark = SparkSession.builder \
    .appName("wiki_extractor") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.driver.maxResultSize", "4g") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#regex patterns for wiki dump
HTML_COMMENT = re.compile(r'<!--.*?-->', re.DOTALL)
NESTED_TEMPLATE = re.compile(r'\{\{[^{}]*\}\}')
REF_TAG = re.compile(r'<ref[^>]*>.*?</ref>', re.DOTALL | re.IGNORECASE)
REF_SELF = re.compile(r'<ref[^>]*/?>', re.IGNORECASE)
HTML_TAG = re.compile(r'<[^>]+>')
WIKI_LINK = re.compile(r'\[\[(?:[^|\]]+\|)?([^\]]+)\]\]')
BOLD = re.compile(r"'''([^']+)'''")
ITALIC = re.compile(r"''([^']+)''")
WHITESPACE = re.compile(r'\s+')
INFOBOX_START = re.compile(r'\{\{Infobox television', re.IGNORECASE)

#cleaning the extracted data 
def clean_text(text):
    if not text:
        return ""
    
    text = str(text).strip()
    text = HTML_COMMENT.sub('', text)
    
    for _ in range(5):
        text = NESTED_TEMPLATE.sub('', text)

    text = REF_TAG.sub('', text)
    text = REF_SELF.sub('', text)
    text = HTML_TAG.sub('', text)
    text = html.unescape(text)
    
    #html entities
    text = text.replace('&nbsp;', ' ')
    text = text.replace('&quot;', '"')
    text = text.replace('&#39;', "'")
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('<br>', ' ')
    text = text.replace('<br/>', ' ')
    text = text.replace('<br />', ' ')
    
    text = WIKI_LINK.sub(r'\1', text)
    text = BOLD.sub(r'\1', text)
    text = ITALIC.sub(r'\1', text)
    text = WHITESPACE.sub(' ', text).strip()

    if text.lower() in ['', 'yes', 'no', 'n/a', 'various', 'tba', 'tbd', 'none', 'present']:
        return ""
    
    if text.startswith('|') or text.startswith('{{') or text.startswith('<!--'):
        return ""
    
    if 'ref' in text.lower() and len(text) < 50:
        return ""
    
    return text

#extracting the infobox data 
def extract_infobox(wikitext):
    start = INFOBOX_START.search(wikitext)
    if not start:
        return None
    
    depth = 0
    i = start.start()
    in_infobox = False
    
    for j in range(i, len(wikitext)):
        if wikitext[j:j+2] == '{{':
            depth += 1
            in_infobox = True
        elif wikitext[j:j+2] == '}}':
            depth -= 1
            if depth == 0 and in_infobox:
                return wikitext[start.end():j]
    
    return None

#getting the raw data from fields
def get_field_raw(infobox, field_name):
    if not infobox:
        return None
    
    variants = [
        field_name,
        field_name.replace('_', ' '),
        field_name.replace('num_', 'num ')
    ]
    
    for variant in variants:
        pattern = rf'^\s*\|\s*{re.escape(variant)}\s*=\s*(.+?)(?=^\s*\||$)'
        match = re.search(pattern, infobox, re.IGNORECASE | re.MULTILINE | re.DOTALL)
        
        if match:
            return match.group(1).strip()
    
    return None

#getting the useful fields
def get_field(infobox, field_name):
    raw = get_field_raw(infobox, field_name)
    if not raw:
        return None

    first_line = raw.split('\n')[0].strip()
    cleaned = clean_text(first_line)
    
    if not cleaned or len(cleaned) < 1:
        return None

    if '|' in cleaned[:20] and '=' in cleaned[:20]:
        return None
    
    #validate num fields
    if field_name in ['num_seasons', 'num_episodes']:
        if not any(char.isdigit() for char in cleaned):
            return None
    
    return cleaned

def extract_year(infobox):
    if not infobox:
        return None
    
    field_names = ['first_aired', 'released', 'first aired']
    
    for field_name in field_names:
        raw = get_field_raw(infobox, field_name)
        if raw:
            year_match = re.search(r'\b(19\d{2}|20\d{2})\b', raw)
            if year_match:
                return year_match.group(1)
    
    return None

#finding if it is a TVserie or not
def is_tv_series(wikitext):
    if not wikitext:
        return False
    
    text_lower = wikitext.lower()
    
    #must be television
    if not INFOBOX_START.search(text_lower):
        return False
    
    #exclude these patterns
    exclusions = [
        r'\{\{infobox (?:person|comedian|actor|actress|singer)',
        r'\(born \d{4}\)',
        r'is an? (?:american|british|canadian)?\s*(?:actor|actress|comedian|singer|musician)',
        r'\{\{infobox (?:video game|film|book)',
    ]
    
    for pattern in exclusions:
        if re.search(pattern, text_lower):
            return False
    
    #check titles
    title_match = re.search(r'<title>(.+?)</title>', wikitext)
    if title_match:
        title = title_match.group(1).lower()
        season_patterns = [
            r'\((?:series|season)\s+\d+\)',
            r'\([a-z\s]+\s+(?:series|season)\s+\d+\)',
            r'^(?:season|series)\s+\d+$',
            r'\s+(?:season|series)\s+\d+$',
        ]
        for pattern in season_patterns:
            if re.search(pattern, title):
                return False
    
    return True

#extraction of a brief plot/summary
def extract_plot(wikitext):
    if not wikitext:
        return ""
    
    text = re.sub(r'\{\{Infobox[^}]*\}\}', '', wikitext, flags=re.DOTALL | re.IGNORECASE)
    text = REF_TAG.sub('', text)
    text = REF_SELF.sub('', text)

    first_section = re.split(r'\n==', text)[0]
    lines = first_section.split('\n')
    
    for line in lines:
        line = line.strip()
        
        if any([
            not line,
            line.startswith('{{'),
            line.startswith('[[Category'),
            line.startswith('[[File'),
            line.startswith('[[Image'),
            line.startswith('='),
            line.startswith('|'),
            line.startswith('#'),
            line.startswith('*'),
            line.startswith(':'),
            line.startswith('{|'),
            line.startswith('<!--'),
            len(line) < 100,
        ]):
            continue
        
        cleaned = clean_text(line)
        
        if any(marker in cleaned for marker in ['<ref', '{{', '[[Category', '<!--']):
            continue
        
        if len(cleaned) > 100:
            return cleaned[:500]
    
    return ""

#parsing each page
def parse_page(page_xml):
    try:
        title_match = re.search(r'<title>(.+?)</title>', page_xml)
        if not title_match:
            return None
        title = title_match.group(1)
        
        skip = ['List of', 'Wikipedia:', 'Template:', 'Category:', 'File:']
        if any(title.startswith(p) for p in skip) or ':' in title:
            return None
        
        #skiping sub-pages for seasons/series of a main TVserie
        season_patterns = [
            r'\((?:series|season)\s+\d+\)',
            r'\([a-z\s]+\s+(?:series|season)\s+\d+\)',
            r'^(?:season|series)\s+\d+$',
            r'\s+(?:season|series)\s+\d+$',
        ]
        for pattern in season_patterns:
            if re.search(pattern, title, re.IGNORECASE):
                return None
        
        ns_match = re.search(r'<ns>(\d+)</ns>', page_xml)
        if ns_match and ns_match.group(1) != '0':
            return None
        
        text_match = re.search(r'<text[^>]*>(.*?)</text>', page_xml, re.DOTALL)
        if not text_match:
            return None
        wikitext = text_match.group(1)
        
        if wikitext.strip().lower().startswith('#redirect'):
            return None
        
        if not is_tv_series(wikitext):
            return None
        
        infobox = extract_infobox(wikitext)
        if not infobox:
            return None
        
        #extract defined fields
        data = {
            "title": title,
            "year": extract_year(infobox),
            "country": get_field(infobox, 'country'),
            "num_seasons": get_field(infobox, 'num_seasons'),
            "num_episodes": get_field(infobox, 'num_episodes'),
            "runtime": get_field(infobox, 'runtime'),
            "network": get_field(infobox, 'network'),
            "plot": extract_plot(wikitext),
        }
        
        #validate data
        useful = [
            bool(data['year']),
            bool(data['country']),
            bool(data['num_seasons']),
            bool(data['num_episodes']),
            bool(data['network']),
            len(data['plot']) > 100,
        ]
        
        has_episode_data = bool(data['num_seasons']) or bool(data['num_episodes'])
  
        return data
        
    except Exception:
        return None

#spliting pages into separate ones
def split_pages(lines):
    page = []
    in_page = False
    for line in lines:
        if '<page>' in line:
            in_page = True
            page = [line]
        elif '</page>' in line and in_page:
            page.append(line)
            yield '\n'.join(page)
            page = []
            in_page = False
        elif in_page:
            page.append(line)

#find wiki dump in same folder
def find_wiki_dump():
    patterns = [
        "enwiki-latest-pages-articles.xml.bz2",
        "enwiki-latest-pages-articles.xml",
    ]
    
    for pattern in patterns:
        if os.path.exists(pattern):
            return pattern
    
    for file in os.listdir('.'):
        if 'wiki' in file.lower() and (file.endswith('.xml') or file.endswith('.bz2') or file.endswith('.gz')):
            return file
    
    return None

#save wiki dump
WIKI_DUMP = find_wiki_dump()

#error if no wiki dump
if not WIKI_DUMP:
    print("Wiki dump not found")
    spark.stop()
    sys.exit(1)

print(f"Processing: {WIKI_DUMP}")
file_size_gb = os.path.getsize(WIKI_DUMP) / (1024**3)
print(f"File size: {file_size_gb:.2f} GB")

#saving wiki dump into rdd and mapping them
wiki_lines = spark.sparkContext.textFile(WIKI_DUMP)
pages_rdd = wiki_lines.mapPartitions(split_pages)
tv_rdd = pages_rdd.map(parse_page).filter(lambda x: x is not None)

#deciding how to process data - if file is bigger than 5gb than partitioning it
if file_size_gb < 5:
    tv_rdd.cache()
    count = tv_rdd.count()
    
    if count > 0:
        tv_data = tv_rdd.collect()
        
        with open("wiki_data.jsonl", 'w', encoding='utf-8') as f:
            for item in tv_data:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
        
        print(f"\nTotal extracted: {count} TV series")
        print(f"Output file: wiki_data.jsonl")
    else:
        print("\nNo data extracted")

#here is the partitioning, to prevent from collapsing
else:
    def save_partition(partition_id, records):
        with open(f"wiki_data_part_{partition_id}.jsonl", 'w', encoding='utf-8') as f:
            count = 0
            for item in records:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
                count += 1
            if count > 0:
                print(f"Partition {partition_id}: {count} records")
        return iter([count])

    partition_counts = tv_rdd.mapPartitionsWithIndex(save_partition).collect()
    total_count = sum(partition_counts)

    #merging partitioned .jsonl data
    if total_count > 0:
        print(f"\nMerging {len(partition_counts)} partitions...")
        with open("wiki_data.jsonl", 'w', encoding='utf-8') as outfile:
            for i in range(len(partition_counts)):
                part_file = f"wiki_data_part_{i}.jsonl"
                if os.path.exists(part_file):
                    with open(part_file, 'r', encoding='utf-8') as infile:
                        outfile.write(infile.read())
                    os.remove(part_file)
        
        print(f"\nTotal extracted: {total_count} TV series")
        print(f"Output file: wiki_data.jsonl")
    else:
        print("\nNo data extracted")

spark.stop()
