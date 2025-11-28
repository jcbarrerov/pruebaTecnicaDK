"""
Módulo para cargar, depurar y procesar el archivo OFEI1204.txt.

Este módulo lee el dataset, filtra únicamente los registros de tipo 'D'
y genera una tabla con las columnas:
Agente, Planta, Hora_1, Hora_2, ..., Hora_24.

Incluye las funciones necesarias para transformar el archivo de entrada
y producir la tabla final requerida.
"""
import os

from module.extract import read_file
from module.transform import extract_date, get_rows
from module.load import create_dataframe, load_csv

PATH = os.path.abspath("../data/OFEI1204.txt")

if __name__ == '__main__':

    file = read_file(PATH)
    date = extract_date(file)
    rows = get_rows(file)
    columns = ["Agente", "Planta", "Tipo"] + ["Hora_{}".format(i) for i in range(1, 25)]
    df = create_dataframe(rows, columns)
    load_csv(df,date)
