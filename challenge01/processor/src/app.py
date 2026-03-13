import json
import os
import customtkinter as ctk
from pathlib import Path
from typing import List, Dict, Any
from unidecode  import unidecode
from rich import print as rprint
from rich.panel import Panel
from rich.console import Console


from elasticsearch import Elasticsearch, helpers
from sentence_splitter import SentenceSplitter
from sentence_transformers import SentenceTransformer

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
ELASTIC_USER = os.getenv("ELASTIC_USER", "username")
ELASTIC_PASS = os.getenv("ELASTIC_PASS", "password")
INPUT_DIR = os.getenv("INPUT_DIR", "/app/input")
INDEX_NAME = os.getenv("INDEX_NAME", "documents")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

# Simple sentence embeddings model
model = SentenceTransformer(EMBEDDING_MODEL)

# Sentence Splitter
splitter = SentenceSplitter(language="en")


def create_index(es: Elasticsearch, index_name: str) -> None:
    # Create the index if it does not exist.
    if es.indices.exists(index=index_name):
        return

    mapping = {
        "mappings": {
            "properties": {
                "doc_id": {"type": "keyword"},
                "chunk_id": {"type": "keyword"},
                "doc_title": {"type": "text", 
                                 "fields": {
                                     "subjects": {"type": "keyword"}
                                     }
                                 },
                "doc_description": {"type": "text"},
                "doc_authors": {"type": "text"},
                "doc_first_publish_year": {"type": "integer"},
                "doc_subjects": {"type": "text", 
                                 "fields": {
                                     "subjects": {"type": "keyword"}
                                     }
                                 },
                "doc_language": {"type": "keyword"},
                "doc_openlibrary_url": {"type": "keyword"},
                "embedding": {
                    "type": "dense_vector",
                    "dims": 384 # Adjust the dimensions where required.
                }
            }
        }
    }

    es.indices.create(index=index_name, body=mapping)
    print(f"Created index: {index_name}")


def load_json_files(input_dir: str) -> List[Dict[str, Any]]:
    documents = []
    for path in Path(input_dir).glob("*.json"):
        with open(path, "r", encoding="utf-8") as f:
            documents.append(json.load(f))
    return documents


def split_into_chunks(text: str, max_sentences: int = 5) -> List[str]:
    # Split the text into small chunks.
    sentences = splitter.split(text)
    chunks = []

    for i in range(0, len(sentences), max_sentences):
        chunk = " ".join(sentences[i:i + max_sentences]).strip()
        if chunk:
            chunks.append(chunk)
    return chunks


def generate_embedding(text: str) -> List[float]:
    embeddings = model.encode(text)
    return embeddings.tolist()


def proccess_documents(document: Dict[str, Any]) -> List[Dict[str, Any]]:

    doc_id = document.get("id")
    title = clean_non_ascii(document.get("title", ""))
    description = clean_non_ascii(unidecode(document.get("description", "")))
    authors = list(map(clean_non_ascii, document.get("authors", "")))
    year = document.get("first_publish_year", "")
    subjects = ensure_capitalize(document.get("subjects", ""))
    language = document.get("language", "")
    url = document.get("openlibrary_url", "")

    if not doc_id or not description:
        raise ValueError("Document must contain at least 'id' and 'description'")

    chunks = split_into_chunks(description)
    actions = []

    for idx, chunk in enumerate(chunks):
        actions.append({
            "_index": INDEX_NAME,
            "_id": f"{doc_id}-{idx}",
            "_source": {
                "doc_id": str(doc_id),
                "chunk_id": f"{doc_id}-{idx}",
                "doc_title": title,
                "doc_description": chunk,
                "doc_authors": authors,
                "doc_first_publish_year": year,
                "doc_subjects": subjects,
                "doc_language": language,
                "doc_openlibrary_url": url,
                "embedding": generate_embedding(chunk)
                }})
        
    return actions

def ensure_capitalize(subjects: list[str]) -> list[str]:
    return [subject.capitalize() for subject in subjects]

def clean_non_ascii(text: str) -> str:
    return unidecode(text)


def semantic_search(es: Elasticsearch, index_name: str, query_text: str, k: int = 5, candidates: int = 50) -> Dict[str, Any]:
    # Query to perform semantic search
    query_vector = generate_embedding(query_text)

    body = {
        "knn": {
            "field": "embedding",
            "query_vector": query_vector,
            "k": k,
            "num_candidates": candidates
        },
        "_source": ["doc_id", "chunk_id", "doc_title", "content"]
    }

    return es.search(index=index_name, body=body).body

def tkinter_app(es):
    app = ctk.CTk()
    app.title("Book shelf tool")
    app.geometry("1000x1000")

    ctk.CTkLabel(app, text="Enter the query: ").pack(pady=(10, 0))
    txt_input = ctk.CTkEntry(app, width=350)
    txt_input.pack(pady=5)
    
    ctk.CTkLabel(app, text="Enter the number of neighbors: ").pack(pady=(10, 0))
    neighbors_input = ctk.CTkEntry(app, width=350)
    neighbors_input.pack(pady=5)
    
    ctk.CTkLabel(app, text="Enter the number of candidates: ").pack(pady=(10, 0))
    cand_input = ctk.CTkEntry(app, width=350)
    cand_input.pack(pady=5)
    
    result_box = ctk.CTkTextbox(app, width=700, height=250)
    result_box.pack(pady=5)

    def on_click():
        query = txt_input.get()
        n = neighbors_input.get()
        c = cand_input.get()
        if n.isdigit() and c.isdigit():
            result = semantic_search(es, index_name=INDEX_NAME, query_text=query, k=int(n), candidates=int(c))
            hits = result['hits']['hits']
            
            text = ""
            for i, hit in enumerate(hits, 1):
                
                text = text + f"Result number #{i}\nScore: {hit['_score']}\nData: {hit['_source']}\n\n"
                
            result_box.delete("0.0", "end")
            result_box.insert("0.0", text)
        else:
            result_box.delete("0.0", "end")
            result_box.insert("0.0", "ERROR: You need to enter a integer as a value for neighbors or candidates.")

    btn = ctk.CTkButton(app, text="Buscar", command=on_click)
    btn.pack(pady=20)

    app.mainloop()
            

def terminal_querys(es: Elasticsearch):
    queries = ["A group of children who come across paranormal events",
              "Person who suffers from psychological disorders",
              "Narratives of ancient fantastic tales",
              "Oriental poems",
              "Horror and paranormal tales",
              "Stories of the high seas"]
    
    console = Console()
    for query in queries:
        result = semantic_search(es, index_name=INDEX_NAME, query_text=query)
        hits = result['hits']['hits']

        console.print(f"[bold magenta]The result for: {query}[/bold magenta]")
        
        for i, hit in enumerate(hits, 1):
            
            rprint(Panel(
                f"[bold cyan]Score:[/bold cyan] {hit['_score']}\n"
                f"[bold cyan]Data:[/bold cyan] {hit['_source']}",
                title=f"Result number #{i}",
                border_style="green",
                expand=False
            ))


def main() -> None:
    es = Elasticsearch(ELASTICSEARCH_URL)
    create_index(es, INDEX_NAME)
    documents = load_json_files(INPUT_DIR)

    if not documents:
        print("No JSON files found.")
        return

    built_docs = []
    for document in documents:
        built_docs.extend(proccess_documents(document))
    
    
    helpers.bulk(es, built_docs)
    terminal_querys(es)
    #tkinter_app(es)
            
    


if __name__ == "__main__":
    main()