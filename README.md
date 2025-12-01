<!-- ===================== -->
<!--        PORTADA        -->
<!-- ===================== -->

#  **Prueba Técnica – Ingeniería de Datos**

**Autor:** Juan Camilo Barrero Velásquez  
**Correo:** jcbarrerov@unal.edu.co  
**Fecha:** 01/12/2025  
**Cargo al que aplicas:** Ingeniero de Datos

---

# 📂 Índice

1. [Carga de Información](#Carga-de-Información)
    - [Solución](#Solución)
        - [Extracción - Lectura del archivo](Extracción---Lectura-del-archivo)
        - [Transformación - Procesamiento de la información](Transformación---Procesamiento-de-la-información)
        - [Carga - creación del DataFrame y exportación del documento CSV](Carga---creación-del-DataFrame-y-exportación-del-documento-CSV)
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
### **Carga - creación del DataFrame y exportación del documento CSV**

Este módulo está compuesto de dos funciones `create_dataframe` y `load_csv`.

En la función `create_dataframe` recibe los parametros rows (una lista de listas de strings) resultante del módulo de extracción y columns correspondiente a una lista que contiene los caracteres correspondientes a los nombres de las columnas.

Luego se crea un dataframe de pandas con las filas y columnas establecidas en los parámentros, utiliza el metodo map que permite retirar los espacios extra al inicio y fin de cada valor si es un string y finalmente imprime un mensaje estableciendo el éxito de la ejecución y retorna el dataframe.

```python
def create_dataframe(rows:List[List[str]], columns:List[str]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=columns)
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    print("DataFrame created successfully!")
    return df
```

La función `load_csv` tiene los parámetros `df` (el dataframe retornado en la función anterior), `date` (la fecha obtenida en la función `extract_date`) y `path` (la ruta donde el archivo va a ser guardado). En este caso el path no es un parámetro obligatorio, de ser especificado lo usa para guardar el archivo dentro del directorio.

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
---


# 🎯 **Objetivo de la Prueba**
Explica lo que se busca lograr, por ejemplo:

- Transformar un archivo CSV usando Python  
- Aplicar limpieza, normalización, enriquecimiento, etc.  

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
