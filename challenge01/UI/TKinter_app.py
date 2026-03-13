import os
import customtkinter as ctk

from elasticsearch import Elasticsearch
from processor.src.app import semantic_search

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
ELASTIC_USER = os.getenv("ELASTIC_USER", "username")
ELASTIC_PASS = os.getenv("ELASTIC_PASS", "password")
INDEX_NAME = os.getenv("INDEX_NAME", "documents")

es = Elasticsearch(ELASTICSEARCH_URL, basic_auth=(ELASTIC_USER, ELASTIC_PASS))

def tkinter_app():
    
    app = ctk.CTk()
    app.title("Book shelf tool")
    app.geometry("1000x680")

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
            
            n = int(n)
            c = int(c)
            
            result_box.delete("0.0", "end")
            
            if n > c:
            
                result_box.insert("0.0", "ERROR: The number of neighbors needs to be equal to or less than the number of candidates.")
            
            else:
                result = semantic_search(es, index_name=INDEX_NAME, query_text=query, k=n, candidates=c)
                hits = result['hits']['hits']
                
                text = ""
                print(len(hits))
                print(hits)
                for i, hit in enumerate(hits, 1):
                    
                    text = text + f"Result number #{i}\nScore: {hit['_score']}\nData: {hit['_source']}\n\n"
                    
                result_box.insert("0.0", text)
            
        else:
            result_box.delete("0.0", "end")
            result_box.insert("0.0", "ERROR: You need to enter a integer as a value for neighbors or candidates.")

    btn = ctk.CTkButton(app, text="Buscar", command=on_click)
    btn.pack(pady=20)

    app.mainloop()

if __name__ == "__main__":
    tkinter_app()