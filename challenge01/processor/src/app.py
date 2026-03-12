import json
import os
from pathlib import Path
from typing import List, Dict, Any
from unidecode  import unidecode

from elasticsearch import Elasticsearch
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
    result = []

    for idx, chunk in enumerate(chunks):
        embedding = generate_embedding(chunk)
        result.append({
            "doc_id": str(doc_id),
            "chunk_id": f"{doc_id}-{idx}",
            "doc_title": title,
            "doc_description": chunk,
            "doc_authors": authors,
            "doc_first_publish_year": year,
            "doc_subjects": subjects,
            "doc_language": language,
            "doc_openlibrary_url": url,
            "embedding": embedding
        })

    return result

def ensure_capitalize(subjects: list[str]) -> list[str]:
    return [subject.capitalize() for subject in subjects]

def clean_non_ascii(text: str) -> str:
    return unidecode(text)
    

def index_documents(es: Elasticsearch, index_name: str, docs: List[Dict[str, Any]]) -> None:
    
    for doc in docs:
        doc_id = doc.get("chunk_id")
        es.index(index=index_name, document=doc, id=doc_id)
    return


def semantic_search(es: Elasticsearch, index_name: str, query_text: str, k: int = 3) -> Dict[str, Any]:
    # Query to perform semantic search
    query_vector = generate_embedding(query_text)

    body = {
        "knn": {
            "field": "embedding",
            "query_vector": query_vector,
            "k": k,
            "num_candidates": 10
        },
        "_source": ["doc_id", "chunk_id", "doc_title", "content"]
    }

    return es.search(index=index_name, body=body).body


def main() -> None:
    es = Elasticsearch(ELASTICSEARCH_URL, basic_auth=(ELASTIC_USER, ELASTIC_PASS))
    create_index(es, INDEX_NAME)
    documents = load_json_files(INPUT_DIR)

    if not documents:
        print("No JSON files found.")
        return

    for document in documents:
        built_docs = proccess_documents(document)
        index_documents(es, INDEX_NAME, built_docs)

    print("Semantic search: examples")

    # TODO: Create several semantic search queries and print the results.
    # Use the function semantic_search()


if __name__ == "__main__":
    main()