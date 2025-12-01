import io
import os

def read_file(filepath: str) -> str:

    """
    Lee y retorna el contenido completo de un archivo de texto.

    Esta función abre un archivo en modo lectura utilizando codificación UTF-8,
    lee todo su contenido como una cadena, imprime un mensaje de confirmación
    y retorna dicho contenido al llamador.

    Parámetros
    ----------
    filepath : str
        Ruta completa del archivo que se desea leer.

    Retorna
    -------
    str
        Cadena que contiene el contenido completo del archivo.

    Excepciones
    -----------
    FileNotFoundError
        Si el archivo no existe en la ruta especificada.
    OSError
        Si ocurre un error al intentar abrir o leer el archivo.
    """

    with io.open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    print("File read successfully!")
    return content
    


