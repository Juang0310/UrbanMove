
import os
from flask import Flask, jsonify, render_template
from dotenv import load_dotenv
import snowflake.connector

# Cargar variables del archivo .env
load_dotenv()

app = Flask(__name__)


# ==========================================
# CONEXION CON SNOWFLAKE
# ==========================================

def conectar_snowflake():
    conexion = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        authenticator="OAUTH_AUTHORIZATION_CODE",
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    return conexion


# ==========================================
# INDICADORES GENERALES
# ==========================================

def obtener_indicadores():
    conexion = conectar_snowflake()
    cursor = conexion.cursor()

    consulta = """
        SELECT
            COUNT(*) AS TOTAL_LECTURAS,
            COUNT(DISTINCT VEHICULO_ID) AS TOTAL_VEHICULOS,
            COUNT(DISTINCT RUTA_ID) AS TOTAL_RUTAS,
            COUNT(DISTINCT ESTACION_ID) AS TOTAL_ESTACIONES,
            ROUND(AVG(VELOCIDAD), 2) AS VELOCIDAD_PROMEDIO,
            ROUND(
                COUNT_IF(VELOCIDAD < 10) * 100.0 / COUNT(*),
                2
            ) AS PORCENTAJE_BAJA_VELOCIDAD
        FROM LECTURAS_MOVILIDAD_COMPLETA
    """

    cursor.execute(consulta)
    resultado = cursor.fetchone()

    indicadores = {
        "total_lecturas": resultado[0],
        "total_vehiculos": resultado[1],
        "total_rutas": resultado[2],
        "total_estaciones": resultado[3],
        "velocidad_promedio": float(resultado[4] or 0),
        "porcentaje_baja_velocidad": float(resultado[5] or 0)
    }

    cursor.close()
    conexion.close()

    return indicadores


# ==========================================
# INFORMACION DE RUTAS
# ==========================================

def obtener_rutas():
    conexion = conectar_snowflake()
    cursor = conexion.cursor()

    consulta = """
        SELECT
            RUTA_ID,
            ROUND(AVG(VELOCIDAD), 2) AS VELOCIDAD_PROMEDIO,
            ROUND(
                COUNT_IF(VELOCIDAD < 10) * 100.0 / COUNT(*),
                2
            ) AS PORCENTAJE_BAJA_VELOCIDAD
        FROM LECTURAS_MOVILIDAD_COMPLETA
        GROUP BY RUTA_ID
        ORDER BY PORCENTAJE_BAJA_VELOCIDAD DESC
    """

    cursor.execute(consulta)

    rutas = []

    for fila in cursor.fetchall():
        rutas.append({
            "ruta": fila[0],
            "velocidad_promedio": float(fila[1]),
            "porcentaje_baja_velocidad": float(fila[2])
        })

    cursor.close()
    conexion.close()

    return rutas


# ==========================================
# INFORMACION DE ESTACIONES
# ==========================================

def obtener_estaciones():
    conexion = conectar_snowflake()
    cursor = conexion.cursor()

    consulta = """
        SELECT
            ESTACION_ID,
            COUNT(*) AS TOTAL_LECTURAS,
            ROUND(AVG(VELOCIDAD), 2) AS VELOCIDAD_PROMEDIO
        FROM LECTURAS_MOVILIDAD_COMPLETA
        GROUP BY ESTACION_ID
        ORDER BY TOTAL_LECTURAS DESC
    """

    cursor.execute(consulta)

    estaciones = []

    for fila in cursor.fetchall():
        estaciones.append({
            "estacion": fila[0],
            "total_lecturas": fila[1],
            "velocidad_promedio": float(fila[2])
        })

    cursor.close()
    conexion.close()

    return estaciones


# ==========================================
# MOVILIDAD POR HORA
# ==========================================

def obtener_movilidad_hora():
    conexion = conectar_snowflake()
    cursor = conexion.cursor()

    consulta = """
        SELECT
            HOUR(FECHA_HORA) AS HORA,
            COUNT(*) AS TOTAL_LECTURAS,
            ROUND(AVG(VELOCIDAD), 2) AS VELOCIDAD_PROMEDIO
        FROM LECTURAS_MOVILIDAD_COMPLETA
        GROUP BY HOUR(FECHA_HORA)
        ORDER BY HORA
    """

    cursor.execute(consulta)

    resultados = []

    for fila in cursor.fetchall():
        resultados.append({
            "hora": int(fila[0]),
            "total_lecturas": fila[1],
            "velocidad_promedio": float(fila[2])
        })

    cursor.close()
    conexion.close()

    return resultados


# ==========================================
# VELOCIDAD PROMEDIO POR ESTACION
# ==========================================

def obtener_velocidad_estaciones():
    conexion = conectar_snowflake()
    cursor = conexion.cursor()

    consulta = """
        SELECT
            ESTACION_ID,
            ROUND(AVG(VELOCIDAD), 2) AS VELOCIDAD_PROMEDIO
        FROM LECTURAS_MOVILIDAD_COMPLETA
        GROUP BY ESTACION_ID
        ORDER BY VELOCIDAD_PROMEDIO DESC
    """

    cursor.execute(consulta)

    resultados = []

    for fila in cursor.fetchall():
        resultados.append({
            "estacion": fila[0],
            "velocidad_promedio": float(fila[1])
        })

    cursor.close()
    conexion.close()

    return resultados


# ==========================================
# PAGINA PRINCIPAL
# ==========================================

@app.route("/")
def inicio():
    return render_template("index.html")


# ==========================================
# API DE INDICADORES
# ==========================================

@app.route("/api/indicadores")
def api_indicadores():
    try:
        return jsonify(obtener_indicadores())
    except Exception as error:
        return jsonify({"error": str(error)}), 500


# ==========================================
# API DE RUTAS
# ==========================================

@app.route("/api/rutas")
def api_rutas():
    try:
        return jsonify(obtener_rutas())
    except Exception as error:
        return jsonify({"error": str(error)}), 500


# ==========================================
# API DE ESTACIONES
# ==========================================

@app.route("/api/estaciones")
def api_estaciones():
    try:
        return jsonify(obtener_estaciones())
    except Exception as error:
        return jsonify({"error": str(error)}), 500


# ==========================================
# API DE MOVILIDAD POR HORA
# ==========================================

@app.route("/api/horas")
def api_horas():
    try:
        return jsonify(obtener_movilidad_hora())
    except Exception as error:
        return jsonify({"error": str(error)}), 500


# ==========================================
# API DE VELOCIDAD POR ESTACION
# ==========================================

@app.route("/api/velocidad-estaciones")
def api_velocidad_estaciones():
    try:
        return jsonify(obtener_velocidad_estaciones())
    except Exception as error:
        return jsonify({"error": str(error)}), 500


# ==========================================
# EJECUTAR APLICACION
# ==========================================

if __name__ == "__main__":
    app.run(
        debug=True,
        host="127.0.0.1",
        port=5000
    )
