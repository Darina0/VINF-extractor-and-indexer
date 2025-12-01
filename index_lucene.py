import json
import lucene
from pathlib import Path

from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, TextField, StringField
from org.apache.lucene.index import IndexOptions, IndexWriter, IndexWriterConfig, DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser, MultiFieldQueryParser
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.search import IndexSearcher, BooleanQuery


#load data into a dictionary
def load_data(file):
    all_series = {}
    with open(file, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if line.strip():
                series = json.loads(line)
                series_id = str(i)
                all_series[series_id] = series
    return all_series

#create a doc for each record
def create_doc(id, series):
    doc = Document()
    
    #id
    field_type = FieldType()
    field_type.setStored(True)
    field_type.setIndexOptions(IndexOptions.NONE)
    doc.add(Field("id", str(id), field_type))
    
    #title
    if series.get("title"):
        doc.add(TextField("title", series["title"], Field.Store.NO))
    
    #year - indexed as string
    if series.get("year"):
        doc.add(StringField("year", str(series["year"]), Field.Store.NO))
    
    #certification
    if series.get("certification"):
        doc.add(TextField("certification", series["certification"], Field.Store.NO))
    
    #genres - as one string
    if series.get("genres"):
        genres_str = ' '.join(series["genres"])
        doc.add(TextField("genres", genres_str, Field.Store.NO))
    
    #keywords - as one string
    if series.get("keywords"):
        keywords_str = ' '.join(series["keywords"])
        doc.add(TextField("keywords", keywords_str, Field.Store.NO))
    
    #cast - as one string
    if series.get("cast"):
        cast_str = ' '.join(series["cast"])
        doc.add(TextField("cast", cast_str, Field.Store.NO))
    
    #creator - as one string
    if series.get("creator"):
        creator_str = ' '.join(series["creator"])
        doc.add(TextField("creator", creator_str, Field.Store.NO))
    
    #language
    if series.get("language"):
        doc.add(TextField("language", series["language"], Field.Store.NO))
    
    #status
    if series.get("status"):
        doc.add(StringField("status", series["status"], Field.Store.NO))
    
    #type
    if series.get("type"):
        doc.add(TextField("type", series["type"], Field.Store.NO))
    
    #score
    if series.get("score"):
        doc.add(TextField("score", str(series["score"]), Field.Store.NO))
    
    #WIKI DATA ADDITIONS
    #country
    if series.get("country"):
        doc.add(TextField("country", series["country"], Field.Store.NO))
    
    #network
    if series.get("network"):
        doc.add(TextField("network", series["network"], Field.Store.NO))
    
    #plot
    if series.get("plot"):
        doc.add(TextField("plot", series["plot"], Field.Store.NO))
    
    #num of seasons
    if series.get("num_seasons"):
        doc.add(StringField("num_seasons", str(series["num_seasons"]), Field.Store.NO))
    
    #num of episodes
    if series.get("num_episodes"):
        doc.add(StringField("num_episodes", str(series["num_episodes"]), Field.Store.NO))
    
    #runtime
    if series.get("runtime"):
        doc.add(TextField("runtime", str(series["runtime"]), Field.Store.NO))
    
    return doc

#index creation
def create_index(file):
    store = MMapDirectory(Paths.get("series_index"))
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(store, config)
    
    count = 0
    with open(file, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if line.strip():
                series = json.loads(line)
                doc = create_doc(str(i), series)
                writer.addDocument(doc)
                count += 1
    
    writer.commit()
    writer.close()
    print(f"Index created with: {count} records")


#search - top 10 results
def search_lucene(query_string):
    directory = MMapDirectory(Paths.get("series_index"))
    searcher = IndexSearcher(DirectoryReader.open(directory))
    analyzer = StandardAnalyzer()
    
    fields = [
        "title", "year", "certification", "genres", "keywords",
        "cast", "creator", "language", "status", "type", "score",
        "country", "network", "plot", "num_seasons", "num_episodes", "runtime"
    ]

    #boost values
    from java.util import HashMap
    boosts = HashMap()
    boosts.put("title", 2)
    boosts.put("keywords", 1.7)
    boosts.put("genres", 2)
    boosts.put("cast", 1.5)
    boosts.put("creator", 1.2)
    boosts.put("country", 0.6)
    boosts.put("network", 0.6)
    boosts.put("plot", 0.6)
    boosts.put("num_seasons", 0.5)
    boosts.put("num_episodes", 0.5)
    boosts.put("runtime", 0.5)


    query_parser = MultiFieldQueryParser(fields, analyzer)
    query = query_parser.parse(query_parser, query_string)
    
    return searcher.search(query, 10).scoreDocs, searcher


#print results
def print_results(results, searcher, all_series):
    if not results:
        print("\nNo results\n")
        return
    
    print(f"\nTop {len(results)} results\n")
    
    for i, result in enumerate(results, 1):
        doc = searcher.storedFields().document(result.doc)
        series = all_series[doc.get("id")]
        
        print(f"{i}. ID: {doc.get("id")}, Score: {result.score:.2f}")
        print(f"   Title: {series.get("title", "N/A")} ({series.get("year", "N/A")})")
        print(f"   Certification: {series.get("certification", "N/A")} | Language: {series.get("language", "N/A")}")
        print(f"   Genres: {', '.join(series.get("genres", [])) if series.get("genres") else "N/A"}")
        print(f"   User Rating: {series.get("score", "N/A")} | Status: {series.get("status", "N/A")} | Type: {series.get("type", "N/A")}")
        
        if series.get("creator"):
            print(f"   Creator: {', '.join(series["creator"])}")
        
        if series.get("cast"):
            cast_list = series["cast"]
            print(f"   Cast: {', '.join(cast_list)}")
        
        if series.get("keywords"):
            print(f"   Keywords: {', '.join(series["keywords"])}")
        
        print()

#main function
def main():
    file_name = "data.jsonl"
    lucene.initVM(vmargs=["-Djava.awt.headless=true"])
    
    option = "none"
    while option not in ["1", "2"]:
        print("Create index? 1 for YES, 2 for NO")
        option = str(input("Choice: "))
        if option not in ["1", "2"]:
            print("Wrong input")
    
    if option == "1":
        create_index(file_name)
    
    all_series = load_data(file_name)
    print(f"Loaded {len(all_series)} records\n")
    
    while True:
        try:
            print("\nTV SHOW SEARCH - LUCENE\n")
            print("\nFulltext search: you can search across all fields")
            print("\nSearchable fields: title, year, certification, genres, keywords, \
                    \ncast, creator, language, status, type,\
                    \nscore, country, network, plot, num_seasons, \
                    \nnum_episodes, runtime")
            print("\nTo end write 'exit'\n")
            
            query_string = input('Search: ').strip()
            
            if query_string.lower() == "exit":
                print("\nEnding")
                break
            
            if query_string == '':
                continue
            
            print(f"\nSearching: '{query_string}'")
            results, searcher = search_lucene(query_string)
            print_results(results, searcher, all_series)
            
        except KeyboardInterrupt:
            print("\n\nEnded")
            break
        except Exception as e:
            print(f'\n{e}')


if __name__ == "__main__":
    main()