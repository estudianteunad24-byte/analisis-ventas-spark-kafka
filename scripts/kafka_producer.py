import time
import json
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

# Generar una transaccion de venta
def generar_venta():
    productos = [
        ("P001", 15.0),
        ("P002", 25.0),
        ("P003", 40.0),
        ("P004", 12.5)
    ]

    producto, precio = random.choice(productos)
    cantidad = random.randint(1, 5)

    return {
        "id": random.randint(100000, 999999),
        "producto": producto,
        "cantidad": cantidad,
        "precio": precio,
        "ingresos": round(cantidad * precio, 2),
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat()
    }

# Conexion con Kafka
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(
        x
    ).encode("utf-8")
)

try:
    while True:
        venta = generar_venta()

        producer.send("ventas", value=venta)
        producer.flush()

        print("Venta enviada:", venta)
        time.sleep(1)

except KeyboardInterrupt:
    print("Productor detenido.")

finally:
    producer.close()
