CREATE KEYSPACE IF NOT EXISTS urbanmove
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE urbanmove;

CREATE TABLE IF NOT EXISTS lecturas_gps (
    vehiculo_id text,
    fecha_dia date,
    fecha_hora timestamp,
    id int,
    estacion_id text,
    latitud double,
    longitud double,
    ruta_id text,
    velocidad double,
    PRIMARY KEY ((vehiculo_id, fecha_dia), fecha_hora, id)
)
WITH CLUSTERING ORDER BY (
    fecha_hora DESC,
    id ASC
);
