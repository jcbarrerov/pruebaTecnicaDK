import pandas as pd
from typing import List

def create_dataframe(rows:List[str], columns:List[str]) -> pd.DataFrame:
     """
    Crea un DataFrame de pandas a partir de una lista de filas y nombres de columnas.

    Esta función construye un DataFrame utilizando las filas y columnas recibidas. 
    Luego aplica un mapeo para eliminar espacios en blanco al inicio y final de cada 
    valor que sea una cadena. Finalmente, retorna el DataFrame limpio y preparado 
    para su uso posterior.

    Parámetros
    ----------
    rows : List[List[str]]
        Lista de filas donde cada fila es una lista de valores.
    columns : List[str]
        Lista con los nombres de las columnas del DataFrame.

    Retorna
    -------
    pandas.DataFrame
        DataFrame creado y con los valores de texto limpiados.
    """
    df = pd.DataFrame(rows, columns=columns)
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    print("DataFrame created successfully!")
    return df

def load_csv(df:pd.DataFrame, date:str, path:str | None = None) -> None:
    """
    Exporta un DataFrame a un archivo CSV con un nombre basado en una fecha.

    El archivo se guardará como 'OFFEI_cleansed_<date>.csv'. Si se proporciona 
    un directorio en el parámetro `path`, el archivo se guarda allí; de lo contrario, 
    se guarda en el directorio actual. El CSV se genera sin la columna de índice.

    Parámetros
    ----------
    df : pandas.DataFrame
        DataFrame que será exportado a CSV.
    date : str
        Fecha que se incluirá en el nombre del archivo.
    path : str o None, opcional
        Ruta del directorio donde se guardará el archivo. Si es None, se usa 
        el directorio actual.

    Retorna
    -------
    None
        Esta función no retorna ningún valor. Solo genera el archivo CSV.
    """
    if path == None:
        path_csv = f"OFFEI_cleansed_{date}.csv"
    else: 
        path_csv = f"{path}/OFFEI_cleansed_{date}.csv" 

    df.to_csv(path_csv, index=False)
    print("CSV file loaded successfully!")
    






