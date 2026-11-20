
from neo4j import GraphDatabase
import math

URI = "bolt://localhost:7687"
USUARIO = "neo4j"
CONTRASENA = "urbanmove123"

estaciones = [
    {"id": "STATION_01", "lat": 40.7128, "lon": -74.0060},
    {"id": "STATION_02", "lat": 40.7210, "lon": -73.9990},
    {"id": "STATION_03", "lat": 40.7300, "lon": -73.9950},
    {"id": "STATION_04", "lat": 40.7410, "lon": -73.9890},
    {"id": "STATION_05", "lat": 40.7484, "lon": -73.9857}
]

conexiones = [
    ("STATION_01", "STATION_02"),
    ("STATION_02", "STATION_03"),
    ("STATION_03", "STATION_04"),
    ("STATION_04", "STATION_05"),
    ("STATION_02", "STATION_04")
]


def calcular_distancia(lat1, lon1, lat2, lon2):
    radio = 6371

    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)

    diferencia_lat = lat2 - lat1
    diferencia_lon = math.radians(lon2 - lon1)

    a = (
        math.sin(diferencia_lat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2)
        * math.sin(diferencia_lon / 2) ** 2
    )

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return radio * c


driver = GraphDatabase.driver(
    URI,
    auth=(USUARIO, CONTRASENA)
)


def crear_restriccion(tx):
    tx.run("""
        CREATE CONSTRAINT estacion_id IF NOT EXISTS
        FOR (e:Estacion)
        REQUIRE e.id IS UNIQUE
    """).consume()


def cargar_estaciones(tx):
    for estacion in estaciones:
        tx.run("""
            MERGE (e:Estacion {id: $id})
            SET e.latitud = $lat,
                e.longitud = $lon,
                e.origen_datos = 'SIMULADO'
        """, **estacion).consume()


def cargar_conexiones(tx):
    for origen, destino in conexiones:

        estacion_origen = next(
            e for e in estaciones if e["id"] == origen
        )

        estacion_destino = next(
            e for e in estaciones if e["id"] == destino
        )

        distancia = calcular_distancia(
            estacion_origen["lat"],
            estacion_origen["lon"],
            estacion_destino["lat"],
            estacion_destino["lon"]
        )

        tx.run("""
            MATCH (a:Estacion {id: $origen})
            MATCH (b:Estacion {id: $destino})
            MERGE (a)-[r:CONECTADA]->(b)
            SET r.distancia_km = $distancia,
                r.tipo = 'SIMULADA'
        """,
        origen=origen,
        destino=destino,
        distancia=distancia).consume()


try:
    with driver.session() as session:

        # Cada operación utiliza una transacción independiente
        session.execute_write(crear_restriccion)
        session.execute_write(cargar_estaciones)
        session.execute_write(cargar_conexiones)

    print("Red de estaciones cargada correctamente.")

except Exception as error:
    print("Error al cargar la red:", error)

finally:
    driver.close()
