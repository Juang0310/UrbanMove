
import csv
import json
import time
from pathlib import Path
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuracion de Kafka
SERVIDOR = "localhost:9092"
TEMA = "movilidad-gps"

# Configuracion del envio
TAMANO_LOTE = 500
PAUSA = 0.5

# Ubicacion del archivo CSV
ARCHIVO = Path(__file__).resolve().parent.parent / "datos_movilidad.csv"


def crear_productor():

    productor = KafkaProducer(
        bootstrap_servers=SERVIDOR,

        value_serializer=lambda dato: json.dumps(
            dato
        ).encode("utf-8"),

        key_serializer=lambda clave: clave.encode("utf-8"),

        acks="all",
        retries=5,
        compression_type="gzip"
    )

    return productor


def enviar_datos():

    if not ARCHIVO.exists():
        print("No se encontro el archivo CSV.")
        return

    print("Conectando con Apache Kafka...")

    productor = None

    try:

        productor = crear_productor()

        print("Conexion establecida.")
        print("Iniciando envio de eventos GPS...")

        contador = 0

        with open(ARCHIVO, "r", encoding="utf-8") as archivo:

            lector = csv.DictReader(archivo)

            for registro in lector:

                # Convertir datos numericos
                registro["id"] = int(registro["id"])
                registro["latitud"] = float(registro["latitud"])
                registro["longitud"] = float(registro["longitud"])
                registro["velocidad"] = float(registro["velocidad"])

                vehiculo = registro["vehiculo_id"]

                # Enviar evento a Kafka
                productor.send(
                    TEMA,
                    key=vehiculo,
                    value=registro
                )

                contador += 1

                # Confirmar envio por lotes
                if contador % TAMANO_LOTE == 0:

                    productor.flush()

                    print(
                        f"Eventos enviados: {contador}"
                    )

                    time.sleep(PAUSA)

        productor.flush()

        print("--------------------------------")
        print("Envio finalizado correctamente.")
        print(f"Total de eventos enviados: {contador}")
        print("--------------------------------")

    except KeyboardInterrupt:

        print("\nEnvio interrumpido por el usuario.")

    except KafkaError as error:

        print(f"Error de Kafka: {error}")

    except Exception as error:

        print(f"Error inesperado: {error}")

    finally:

        if productor is not None:
            productor.close()


enviar_datos()
