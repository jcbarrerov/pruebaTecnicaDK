import io
import os

def read_file(filepath: str) -> str:
    with io.open(filepath, "r", encoding="utf-8") as f:
        contenido = f.read()
    print("File read successfully!")
    return contenido
    


