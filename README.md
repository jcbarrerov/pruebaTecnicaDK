<!-- ===================== -->
<!--        PORTADA        -->
<!-- ===================== -->

#  **Prueba Técnica – Ingeniería de Datos**

**Autor:** Juan Camilo Barrero Velásquez  
**Correo:** jcbarrerov@unal.edu.co  
**Fecha:** 01/12/2025  

---

# 📂 Índice

1. [Quest 1: Carga de Información](#Quest-1:-Carga-de-Información)
    - [Solución](#Solución)
        - [Extracción - Lectura del archivo](#Extracción---Lectura-del-archivo)
        - [Transformación - Procesamiento de la información](#Transformación---Procesamiento-de-la-información)
        - [Carga - Creación del DataFrame y exportación del documento CSV](#Carga---Creación-del-DataFrame-y-exportación-del-documento-CSV)
        - [Ejecucción del ETL](#Ejecucción-del-ETL)
2. [Objetivo de la Prueba](#objetivo-de-la-prueba)
3. [Arquitectura y Herramientas Usadas](#arquitectura-y-herramientas-usadas)
4. [Desarrollo y Transformaciones](#desarrollo-y-transformaciones)  
   - [Lectura del CSV](#lectura-del-csv)
   - [Limpieza de Datos](#limpieza-de-datos)
   - [Transformaciones Aplicadas](#transformaciones-aplicadas)
   - [Escritura del Resultado](#escritura-del-resultado)
5. [Conclusiones](#conclusiones)
6. [Anexos](#anexos)

---

# **Quest 1: Carga de Información**
Cargar un data set, realizar el cargue y depuración del archivo OFEI1204.txt. Se debe entregar una tabla con las siguentes columnas:

| Agente | Planta | Hora_1 | Hora_2 | Hora_3 | ... | Hora_24 |
|--------|---------|---------|---------|---------|-----|-----------|

Solamente procesar los registros Tipo D. Enviar junto con la tabla resultante el código utilizado. Explicar el paso a paso en un archivo de texto (.doc o .pdf).

## **Solución**

Para la solución de este punto implementó de una filosofía de programación modular en la que se establecieron tres procesos principales: extracción, transformación y carga.

### **Extracción - Lectura del archivo**

Para este proceso de lectura se diseñó la función _read file_ que se encarga de usar la ruta del archivo a leer, `filepath`, para luego abrir el archivo utilizando un modo de lectura _"r"_ con condificación _"utf-8"_, almacenar el contenido en la variable `content`, imprimir un mensaje de indicando el éxito de la operación y finalmente retornarlo

```python
def read_file(filepath: str) -> str:
    with io.open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    print("File read successfully!")
    return content
```
### **Transformación - Procesamiento de la información**

En este módulo del código se establecen dos funciones,  `extract_date` y `get_rows`.

En la función `extract_date` se recibe el parametro `file`, que será el string resultante del proceso de extracción sobre el cual se usa la expresión regular para buscar la fecha de formato "YYYY-MM-DD". En caso de encontrar la fecha imprime un mensaje de éxito y retorna el valor de la fecha en la variable `date`, en caso de no encontrarla, retona `None`. Este dato será utilizado más adelante en el nombre del archivo generado.

```python
def extract_date(file:str) -> str:
    date = re.search(r'\d{4}-\d{2}-\d{2}', file)
    print("Date extracted successfully!")
    return date.group() if date else None
```

En la función _get_rows_ se recibe nuevamente el parametro `file`, que se le asignará de la misma manera que en el proceso anterior. Luego, divide el texto en bloques usando el patrón "\n\n\nAGENTE:" presente en el texto. Almacena el nombre del agente que estará presente en las filas venideras correspondientes al bloque e itera por las lineas del bloque.

En la iteración intenta dividir la información de la fila en las respectivas columnas usando la coma como patrón, si el segundo elemento es " D" entonces almanecena la fila en la variable `complete_line` teniendo como primer elemento el agente y lo agrega a la lista `rows` luego de dividir cada elemento de la cadena por comas. `rows` es por lo tanto una lista de listas donde cada elemento dentro de la lista principal es la lista correspondiente a la información de una fila con sus elementos (que son cadenas de texto) separados.

Todo lo descrito en el parrafo anterior se encuentra dentro de un `try`, que en caso de un `IndexError` se encargará de imprimir una advertencia en consola estableciendo la linea que ha de ser excluida y continuando la iteración. Todo esto, con el fin de poder hacer una revisión completa de la cadena.

Finalmente la función imprime un mensaje de éxito y retorna la variable `rows` con las filas que cumplen la condición establecida. 

```python
def get_rows(file:str) -> List[List[str]]:
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
```
### **Carga - Creación del DataFrame y exportación del documento CSV**

Este módulo está compuesto de dos funciones `create_dataframe` y `load_csv`.

En la función `create_dataframe` recibe los parametros rows (una lista de listas de strings) resultante del módulo de extracción y columns correspondiente a una lista que contiene los caracteres correspondientes a los nombres de las columnas.

Luego se crea un dataFrame de pandas con las filas y columnas establecidas en los parámentros, utiliza el metodo map que permite retirar los espacios extra al inicio y fin de cada valor si es un string y finalmente imprime un mensaje estableciendo el éxito de la ejecución y retorna el dataFrame.

```python
def create_dataframe(rows:List[List[str]], columns:List[str]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=columns)
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    print("DataFrame created successfully!")
    return df
```

La función `load_csv` tiene los parámetros `df` (el dataFrame retornado en la función anterior), `date` (la fecha obtenida en la función `extract_date`) y `path` (la ruta donde el archivo va a ser guardado). En este caso el path no es un parámetro obligatorio, de ser especificado lo usa para guardar el archivo dentro del directorio.

Esta función genera el archivo CSV sin índices con el nombre `"OFFEI_cleansed_{date}.csv"`, donde `{date}` es reemplazado por la fecha. Finalmente imprime un mensaje indicando que el archivo se generó correctamente. 

```python
def load_csv(df:pd.DataFrame, date:str, path:str | None = None) -> None:
    if path == None:
        path_csv = f"OFFEI_cleansed_{date}.csv"
    else: 
        path_csv = f"{path}/OFFEI_cleansed_{date}.csv" 
    df.to_csv(path_csv, index=False)
    print("CSV file loaded successfully!")
```
### **Ejecucción del ETL**

Finalmente se realiza la importacíon de las funciones de los módulos y se establece la ruta dónde se encuentra el archivo. Se llaman las funciones con los argumentos correspondientes y asignamos el retorno de las funciones a las variables necesarias para seguir el proceso de ETL

```python
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
```

---

# **Quest 2: Manipulación de datos**
1. Cargar un data set, del archivo Excel Master Data, únicamente las siguientes
columnas:
    - Nombre visible Agente
    - AGENTE (OFEI)
    - CENTRAL (dDEC, dSEGDES, dPRU…)
    - Tipo de central (Hidro, Termo, Filo, Menor)

2. Seleccionar los registros que pertenecen al agente EMGESA ó EMGESA S.A. y adicionalmente que el Tipo de Central sea ‘H’ o ‘T’.
3. Cargar el archivo dDEC1204.TXT que viene por Central.
4. Realizar el merge de los dos data sets por Central.
5. Calcular la suma horizontal de todas las horas para cada planta.
6. Seleccionar solamente los registros de las plantas cuya suma horizontal sea mayor que cero.
7. Los resultados deben ser entregados en un dataset.
8. Enviar junto con la tabla resultante el código utilizado.
9. Explicar el paso a paso en un archivo de texto (.doc o .pdf).

## **Solución**

Para trabajar este problema se utilizó la ayuda de _Jupyter notebooks_ debido a su facilidad para seguir el flujo de las tareas solicitadas y visualizar los dataframes. Para este programa usaremos la librería _os_, para poder interpretar las rutas relativas de los archivos, y _pandas_ para trabajar con los datos tabulares. Adicionalmente definimos las rutas relativas de los archivos necesarios para el código.

```python
import os
import pandas as pd

PATH_EXCEL = os.path.abspath("../data/Datos Maestros VF.xlsx")
PATH_TXT = os.path.abspath("../data/dDEC1204.TXT")
PATH_TO_SAVE = os.path.abspath("./Dataset.csv") 
```

### **1. Cargar el data set**

Para cargar el dataset se utilizó el metodo `read_excel` de pandas en modo binario `'rb'` especificando en nombre de la hoja que contiene la información y el DataFrame resultante se asignó a la variable `df_excel_raw`. 

Adicionalmente se creó un diccionario que contiene pares clave-valor con los nombres de las columnas del DataFrame y nombres simplificados sin espacio respectivamente para posteriormente renombrar las columnas.

Utilizando un `for` que recorre cada par del diccionario se renombran las columnas del DataFrame `df_excel_raw` sin necesidad de crear un nuevo DataFrame gracias al `inplace=True`.

Luego, se seleccionan los datos del DataFrame de las columnas:
 - Nombre visible Agente que es `AGENTE_VISIBLE`
 - AGENTE (OFEI) que es `AGENTE_OFEI`
 - CENTRAL (dDEC, dSEGDES, dPRU…) que es `CENTRAL`
 - Tipo de central (Hidro, Termo, Filo, Menor) que es `TIPO_CENTRAL`

utilizando la variable `select_columns_excel` que contiene los valores de las columnas a seleccionar del DataFrame se realiza la selección de los valores del DataFrame `df_excel_raw` creando una copia. El nuevo DataFrame es asignado a la variable `df_excel_selected`.

```python
df_excel_raw = pd.read_excel(open(PATH_EXCEL, 'rb'),
              sheet_name='Master Data Oficial')

dic_columns = { 'Nombre visible Agente':'AGENTE_VISIBLE'
                ,'AGENTE (OFEI)':'AGENTE_OFEI'
                ,'CENTRAL (dDEC, dSEGDES, dPRU…)':'CENTRAL'
                ,'Tipo de central (Hidro, Termo, Filo, Menor)':'TIPO_CENTRAL'}

for name, rename in dic_columns.items():
    df_excel_raw.rename(columns={name: rename}, inplace=True)

select_columns_excel = list(dic_columns.values())
df_excel_selected = df_excel_raw[select_columns_excel].copy()
```

### **2. Filtrar por `AGENTE_VISIBLE` y `CENTRAL` **

Para esta sección se filtró el DataFrame df_excel_selected por tres condiciones:
1. `['AGENTE_VISIBLE'] == "EMGESA"` Selecciona filas donde el agente tiene nombre visible "EMGESA".
2. `['AGENTE_OFEI'] == "EMGESA S.A."` Selecciona filas donde el `AGENTE_OFEI` coincide con "EMGESA S.A.".
3. `['TIPO_CENTRAL'].isin(['H', 'T'])` Filtra solo los registros cuyo tipo de central sea H o T utilizando `isin` y la lista con los valores deseados.

La condición 1 y 2 están vinculadas por una condicón `or`, mientras que estas dos están ligadas por una condición `and` con la 3. Finalmente El DataFrame resultate se asigna a la variable `df_excel_filtered`.

```python
df_excel_filtered = df_excel_selected[((df_excel_selected['AGENTE_VISIBLE'] == "EMGESA")
                                        | (df_excel_selected['AGENTE_OFEI'] == "EMGESA S.A."))
                                        & (df_excel_selected['TIPO_CENTRAL'].isin(['H', 'T']))]
```

### **3. Cargar el archivo dDEC1204.TXT**

Para cargar el archivo dDEC1204.TXT que contine los datos por central se creó primero una lista con las columnas correspondientes a el archivo a cargar haciendo uso de un `for` para crear los 24 elementos correspondientes a las horas, luego, se utilizó el metodo `read_csv` de pandas para leer el archivo utilizando `encoding="latin1"` y se asignó al DataFrame `df_text`. Finalmente, se asignaron las columnas almacenadas en la lista columns al DataFrame.

```python
columns = ["CENTRAL"] + ["Hora_{}".format(i) for i in range(1, 25)]
df_text = pd.read_csv(PATH_TXT, encoding="latin1")
df_text.columns = columns
```

### **4. Realizar el merge de los dos data sets por Central**

Para realizar el merge de los DataFrames utilizamos el método de pandas `merge` que nos permite hacer una unión de dos DataFrames. Establecemos el parámetro `on="CENTRAL"` que indica que la unión se hace por la columna "CENTRAL" presente en ambos DataFrames y establecemos que el tipo de unión como `how="left"` que es similar al de un _left join_ en el cual se toman todas las filas de `df_excel_filtered` y solamente se agregan los valores de las filas correspondientes de `df_text` si el valor de la columna "CENTRAL" coincide. De esta manera se asegura de que no se pierden valores de `df_excel_filtered` en caso de que no haya coincidencia. Finalmente se asigna el DataFrame resultante a `df_merged`.

```python
df_merged = pd.merge(df_excel_filtered, df_text, on="CENTRAL", how="left")
```

### **5. Calcular la suma horizontal de todas las horas para cada planta**

Para realizar la suma horizontal se creó la variable tipo lista `columns_to_sum` que almacena los valores de las columnas que queremos sumar (en este caso las correspondientes a las 24 horas). A continuación, utilizamos `df_merged[columns_to_sum]` para seleccionar las columnas y aplicamos el metodo `sum(axis=1)` para realizar la suma, donde `axis=1` establece que la suma debe ser realizada a lo largo de las filas (suma horizontal). Finalmente, el vector resultante es almacenado en la columna "SUM_OF_HOURS" que se asigna dentro del mísmo dataframe `df_merged`.

```python
columns_to_sum = ["Hora_{}".format(i) for i in range(1, 25)]
df_merged["SUM_OF_HOURS"]=df_merged[columns_to_sum].sum(axis=1)
```

### **6. Seleccionar los registros de las plantas con suma horizontal mayor que cero**


---

# 🛠️ **Arquitectura y Herramientas Usadas**
| Componente | Descripción |
|-----------|-------------|
| Python | Procesamiento del CSV |
| Pandas | Transformaciones |
| VS Code / Jupyter | Entorno de desarrollo |
| Git | Control de versiones |

---

# 🧪 **Desarrollo y Transformaciones**

## 📥 Lectura del CSV

### 📌 Código
```python
import pandas as pd

df = pd.read_csv("data/input.csv")
df.head()
