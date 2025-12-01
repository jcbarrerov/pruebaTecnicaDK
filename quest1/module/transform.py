from typing import List
import re

def extract_date(file:str) -> str:
    """
    Extrae una fecha en formato YYYY-MM-DD desde un texto.

    Esta función busca en la cadena proporcionada una fecha con el formato 
    'YYYY-MM-DD'. Si encuentra una coincidencia, retorna la fecha encontrada; 
    de lo contrario, retorna None. También imprime un mensaje indicando que la 
    extracción fue exitosa.

    Parámetros
    ----------
    file : str
        Texto en el cual se buscará la fecha.

    Retorna
    -------
    str o None
        La fecha encontrada en formato 'YYYY-MM-DD', o None si no existe.
    """
    date = re.search(r'\d{4}-\d{2}-\d{2}', file)
    print("Date extracted successfully!")
    return date.group() if date else None
    

def get_rows(file:str) -> List[List[str]]:
    """
    Procesa un texto estructurado y extrae filas basadas en agentes y líneas 
    marcadas con ' D'.

    El texto se divide por bloques usando el separador '\\n\\n\\nAGENTE: '. 
    Cada bloque corresponde a un agente e incluye múltiples líneas asociadas. 
    Para cada línea del bloque, si el segundo campo separado por comas es ' D', 
    se construye una fila con el nombre del agente y los valores de la línea.

    Parámetros
    ----------
    file : str
        Texto completo que contiene agentes y sus respectivos datos.

    Retorna
    -------
    List[List[str]]
        Una lista de listas, donde cada sublista representa una fila con el 
        agente y los valores de la línea que cumple la condición.

    Advertencias
    ------------
    Imprime un mensaje de advertencia si alguna línea no cumple el formato 
    esperado (es decir, si produce un IndexError).
    """
    rows = []
    for block in file.split("\n\n\nAGENTE: "):
        agente = block.split("\n")[0]

        for line in block.split("\n")[1:]:
            try:
                if line.split(",")[1] == " D":
                    complete_line = [agente + " ," + line]
                    rows.append(complete_line[0].split(","))
            except IndexError:
                print(f"WARNING! line being excepted:'{line}'")
    print("Rows obtained successfully!")
    return rows

