
import csv
import random
import math
from datetime import datetime, timedelta

# Configuracion del generador
TOTAL_REGISTROS = 500000
TOTAL_VEHICULOS = 100
ARCHIVO_SALIDA = "datos_movilidad.csv"

# Coordenadas de referencia de Nueva York
ESTACIONES = [
    {
        "id": "STATION_01",
        "lat": 40.7128,
        "lon": -74.0060
    },
    {
        "id": "STATION_02",
        "lat": 40.7210,
        "lon": -73.9990
    },
    {
        "id": "STATION_03",
        "lat": 40.7300,
        "lon": -73.9950
    },
    {
        "id": "STATION_04",
        "lat": 40.7410,
        "lon": -73.9890
    },
    {
        "id": "STATION_05",
        "lat": 40.7484,
        "lon": -73.9857
    }
]

# Crear recorridos entre estaciones
def generar_datos():

    print("Iniciando generacion de datos...")

    fecha_inicial = datetime(2026, 1, 1)

    with open(ARCHIVO_SALIDA, "w", newline="", encoding="utf-8") as archivo:

        campos = [
            "id",
            "vehiculo_id",
            "ruta_id",
            "latitud",
            "longitud",
            "velocidad",
            "fecha_hora",
            "estacion_id"
        ]

        escritor = csv.DictWriter(archivo, fieldnames=campos)
        escritor.writeheader()

        for i in range(TOTAL_REGISTROS):

            vehiculo = random.randint(1, TOTAL_VEHICULOS)

            origen = random.choice(ESTACIONES)
            destino = random.choice(ESTACIONES)

            while origen["id"] == destino["id"]:
                destino = random.choice(ESTACIONES)

            progreso = random.random()

            latitud = (
                origen["lat"] +
                (destino["lat"] - origen["lat"]) * progreso
            )

            longitud = (
                origen["lon"] +
                (destino["lon"] - origen["lon"]) * progreso
            )

            latitud += random.uniform(-0.0005, 0.0005)
            longitud += random.uniform(-0.0005, 0.0005)

            velocidad = random.uniform(5, 35)

            # Simular diferentes condiciones de trafico
            if random.random() < 0.25:
                velocidad = random.uniform(0, 8)

            fecha = fecha_inicial + timedelta(
                seconds=i * 10
            )

            registro = {
                "id": i + 1,
                "vehiculo_id": f"VEHICLE_{vehiculo:03d}",
                "ruta_id": f"ROUTE_{random.randint(1, 10):02d}",
                "latitud": round(latitud, 6),
                "longitud": round(longitud, 6),
                "velocidad": round(velocidad, 2),
                "fecha_hora": fecha.isoformat(),
                "estacion_id": origen["id"]
            }

            escritor.writerow(registro)

            if (i + 1) % 50000 == 0:
                print(f"Registros generados: {i + 1}")

    print("Generacion finalizada.")
    print(f"Archivo creado: {ARCHIVO_SALIDA}")


generar_datos()
