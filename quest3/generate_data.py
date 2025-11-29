from sqlalchemy import create_engine, text
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker('es_CO')

# -----------------------------
# CONFIGURACIÓN DE LA CONEXIÓN
# -----------------------------
server = "localhost,1433"
database = "WEATHER"
username = "sa"
password = "MiClaveSegura123*"

# Cadena de conexión SQLAlchemy (pyodbc)
conn_str = (
    "mssql+pyodbc:///?odbc_connect="
    "Driver={ODBC Driver 18 for SQL Server};"
    f"Server={server};Database={database};"
    f"UID={username};PWD={password};"
    "Encrypt=no;TrustServerCertificate=yes;"
)

engine = create_engine(conn_str)
import random
from datetime import datetime, timedelta

localidades = [
    "Medellín", "Envigado", "Sabaneta", "Bello", "Itagüí",
    "La Estrella", "Copacabana", "Girardota", "Barbosa"
]

cobertura_opciones = ["Minima", "Parcial", "Total"]

def generar_serie_completa(dias=1, fecha_inicio=None):
    registros = []
    
    if fecha_inicio is None:
        fecha_inicio = datetime.now().replace(minute=0, second=0, microsecond=0)

    # Recorremos días
    for d in range(dias):
        fecha_dia = fecha_inicio - timedelta(days=d)

        # Recorremos 24 horas por día
        for h in range(24):

            fecha_hora = fecha_dia.replace(hour=h)

            # Recorremos localidades
            for localidad in localidades:

                registros.append({
                    "LOCALIDAD": localidad,
                    "PAIS": "Colombia",
                    "TEMP_CELCIUS": round(random.uniform(15, 32), 2),
                    "FECHA_HORA": fecha_hora,
                    "COVERTURA": random.choice(cobertura_opciones),
                    "INDICE_UV": random.randint(0, 11),
                    "PRESION_ATM": round(random.uniform(900, 1050), 2),
                    "VEL_VIENTO_NUDOS": round(random.uniform(0.5, 20), 2)
                })

    return registros


# -----------------------------
# QUERY DE INSERCIÓN
# -----------------------------
query = text("""
INSERT INTO CLIMA (
    LOCALIDAD, PAIS, TEMP_CELCIUS, FECHA_HORA,
    COVERTURA, INDICE_UV, PRESION_ATM, VEL_VIENTO_NUDOS
) VALUES (
    :LOCALIDAD, :PAIS, :TEMP_CELCIUS, :FECHA_HORA,
    :COVERTURA, :INDICE_UV, :PRESION_ATM, :VEL_VIENTO_NUDOS
)
""")

# -----------------------------
# INSERTAR LOS DATOS
# -----------------------------
registros = generar_serie_completa(dias=365)

with engine.begin() as conn:
    conn.execute(query, registros)

print("Datos insertados correctamente en CLIMA.")
